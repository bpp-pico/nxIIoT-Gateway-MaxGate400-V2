package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"nxiiot-gateway/internal/acquisition"
	"nxiiot-gateway/internal/api"
	"nxiiot-gateway/internal/config"
	"nxiiot-gateway/internal/connection"
	"nxiiot-gateway/internal/datapoint"
	"nxiiot-gateway/internal/device"
	"nxiiot-gateway/internal/diagnostics"
	"nxiiot-gateway/internal/forwarder"
	"nxiiot-gateway/internal/logger"
	"nxiiot-gateway/internal/netconfig"
	"nxiiot-gateway/internal/processor"
	"nxiiot-gateway/internal/queue"
	"nxiiot-gateway/internal/status"
	"nxiiot-gateway/internal/storage"
	timeservice "nxiiot-gateway/internal/time"
)

func main() {
	configPath := flag.String("config", "configs/config.yaml", "path to config file")
	migrationsDir := flag.String("migrations", "migrations", "path to migrations directory")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		panic(err)
	}

	log, logBuf := logger.NewWithRingBuffer(cfg.Log.Level, cfg.Log.Format, 1000)
	log.Info("starting nxIIoT Gateway", "gateway_id", cfg.Gateway.ID)

	db, err := storage.Open(cfg.Database.Path, *migrationsDir, log)
	if err != nil {
		log.Error("failed to open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if os.Getenv("SEED_DEMO_DEVICE") == "true" {
		seedDemoDevice(db, log)
	}

	// docker-compose reaches the fake server via its service DNS name
	// ("server-demo"), whereas a native run reaches it via localhost — the
	// config file default is the native/localhost case.
	if url := os.Getenv("FORWARDER_SERVER_URL"); url != "" {
		cfg.Forwarder.ServerURL = url
	}
	if transport := os.Getenv("FORWARDER_TRANSPORT"); transport != "" {
		cfg.Forwarder.Transport = transport
	}
	if url := os.Getenv("MQTT_BROKER_URL"); url != "" {
		cfg.MQTT.BrokerURL = url
	}
	if ntpServer := os.Getenv("NTP_SERVER"); ntpServer != "" {
		cfg.Time.NTPServer = ntpServer
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Store & Forward queue (Rule 2/3: persisted before it is forwarded):
	// its own SQLite file, separate from gateway.db, holding compressed
	// chunks of readings — see internal/queue.
	var q *queue.Store
	if !cfg.StoreForward.Disabled {
		q, err = queue.Open(cfg.Queue.Path, queue.Options{
			MaxBytes:            cfg.Queue.MaxBytes,
			MinFreePercent:      cfg.Queue.MinFreePercent,
			FlushInterval:       time.Duration(cfg.Queue.FlushIntervalMs) * time.Millisecond,
			MaintenanceInterval: time.Duration(cfg.Queue.MaintenanceIntervalSec) * time.Second,
			DiskUsage:           func() (uint64, uint64, error) { return storage.DiskUsage(cfg.Queue.Path) },
		}, log)
		if err != nil {
			log.Error("failed to open queue", "path", cfg.Queue.Path, "error", err)
			os.Exit(1)
		}
		defer func() {
			if err := q.Close(); err != nil {
				log.Error("failed to close queue cleanly", "error", err)
			}
		}()
		go q.RunFlusher(ctx)
		go q.RunMaintenance(ctx)
	}
	proc := processor.New(q)

	// Time Service (Rule 8/9/10): an unreachable NTP server only degrades
	// TimeQuality, it never blocks acquisition — this goroutine shares
	// nothing with the Modbus poller but the process.
	timeSvc := timeservice.New(timeservice.Config{
		NTPServer:    cfg.Time.NTPServer,
		SyncInterval: time.Duration(cfg.Time.SyncIntervalSec) * time.Second,
		QueryTimeout: time.Duration(cfg.Time.QueryTimeoutMs) * time.Millisecond,
		RTCDevice:    cfg.Time.RTCDevice,
	}, cfg.Time.Timezone, log)
	go timeSvc.Run(ctx)

	// Acquisition must never depend on the API server or any downstream
	// server connectivity (Rule 1). The Manager owns one poller goroutine
	// per enabled device and is reloaded by the API layer whenever a
	// device/data point is created, edited, deleted, or toggled, so the
	// Web UI takes effect without a gateway restart.
	statusStore := status.NewStore()
	latestStore := acquisition.NewLatestStore()
	diagStore := diagnostics.NewStore()
	connRepo := connection.NewRepository(db)
	deviceRepo := device.NewRepository(db)
	datapointRepo := datapoint.NewRepository(db)
	manager := acquisition.NewManager(ctx, log, connRepo, deviceRepo, datapointRepo, func(r acquisition.Reading) {
		statusStore.Update(r.DeviceID, string(r.Quality), r.EventTimestamp)
		latestStore.Update(r.DatapointID, acquisition.LatestValue{
			Value:   r.Value,
			Quality: string(r.Quality),
			Unit:    r.Unit,
			At:      r.EventTimestamp,
		})
		if !cfg.StoreForward.Disabled {
			proc.Process(r)
		}
		if r.Value != nil {
			log.Info("reading", "device", r.DeviceName, "tag", r.Tag, "value", *r.Value, "unit", r.Unit, "quality", r.Quality)
		} else {
			log.Warn("reading", "device", r.DeviceName, "tag", r.Tag, "quality", r.Quality)
		}
	}, func(deviceID int64, durationMs int64, datapointsRead int, blockReads int, at time.Time) {
		statusStore.UpdatePollTiming(deviceID, durationMs, datapointsRead, blockReads)
	}, diagStore)
	if err := manager.Reload(ctx); err != nil {
		log.Error("failed to start acquisition", "error", err)
	}

	// Store & Forward runs independently of acquisition (Rule 1): a down
	// server only grows the queue, it never blocks Modbus polling.
	// StoreForward.Disabled skips this whole block - no queue (see above
	// and the acquisition callback), no adapter, no MQTT/HTTP connection
	// attempt, no forwarder goroutine. fwd stays nil; the API layer already
	// handles a nil forwarder (see internal/api/storeforward.go).
	var fwd *forwarder.Forwarder
	if cfg.StoreForward.Disabled {
		log.Warn("Store & Forward disabled via config - Modbus-read-only mode: readings are polled and shown live but not persisted or sent anywhere")
	} else {
		adapter, closeAdapter, err := buildAdapter(ctx, cfg, log)
		if err != nil {
			log.Error("failed to initialize forwarder adapter", "error", err)
			os.Exit(1)
		}
		defer closeAdapter()

		fwd = forwarder.New(q, adapter, forwarder.Config{
			GatewayID:             cfg.Gateway.ID,
			MaxReadingsPerMessage: cfg.Forwarder.MaxReadingsPerMessage,
			PollInterval:          time.Duration(cfg.Forwarder.PollIntervalMs) * time.Millisecond,
		}, log)
		go fwd.Run(ctx)
	}

	// Host network configuration (§16 Config page "Gateway IP"): only
	// meaningful when this binary runs directly on the host (systemd
	// deployment, §19), not inside this project's own Docker Compose dev
	// setup — see internal/netconfig's package doc. netSvc.Current()
	// degrades to netconfig.ErrUnsupported wherever nmcli isn't present.
	netSvc := netconfig.NewService(netconfig.New(), log)

	handler := api.NewRouter(cfg, *configPath, db, log, statusStore, latestStore, manager, q, fwd, timeSvc, diagStore, logBuf, netSvc)
	srv := &http.Server{
		Addr:    cfg.API.ListenAddr,
		Handler: handler,
	}

	go func() {
		log.Info("api server listening", "addr", cfg.API.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("api server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown error", "error", err)
	}
	manager.Wait()
}
