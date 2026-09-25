package api

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"nxiiot-gateway/internal/acquisition"
	"nxiiot-gateway/internal/config"
	"nxiiot-gateway/internal/connection"
	"nxiiot-gateway/internal/datapoint"
	"nxiiot-gateway/internal/device"
	"nxiiot-gateway/internal/diagnostics"
	"nxiiot-gateway/internal/forwarder"
	"nxiiot-gateway/internal/logger"
	"nxiiot-gateway/internal/netconfig"
	"nxiiot-gateway/internal/queue"
	"nxiiot-gateway/internal/status"
	"nxiiot-gateway/internal/system"
	timeservice "nxiiot-gateway/internal/time"
)

type Server struct {
	cfg           *config.Config
	configPath    string
	db            *sql.DB
	log           *slog.Logger
	connRepo      *connection.Repository
	deviceRepo    *device.Repository
	datapointRepo *datapoint.Repository
	status        *status.Store
	latest        *acquisition.LatestStore
	manager       *acquisition.Manager
	queue         *queue.Store // nil when Store & Forward is disabled
	forwarder     *forwarder.Forwarder
	timeSvc       *timeservice.Service
	diag          *diagnostics.Store
	logBuf        *logger.RingBuffer
	netSvc        *netconfig.Service
}

func NewRouter(cfg *config.Config, configPath string, db *sql.DB, log *slog.Logger, statusStore *status.Store, latestStore *acquisition.LatestStore, manager *acquisition.Manager, q *queue.Store, fwd *forwarder.Forwarder, timeSvc *timeservice.Service, diag *diagnostics.Store, logBuf *logger.RingBuffer, netSvc *netconfig.Service) http.Handler {
	s := &Server{
		cfg:           cfg,
		configPath:    configPath,
		db:            db,
		log:           log,
		connRepo:      connection.NewRepository(db),
		deviceRepo:    device.NewRepository(db),
		datapointRepo: datapoint.NewRepository(db),
		status:        statusStore,
		latest:        latestStore,
		manager:       manager,
		queue:         q,
		forwarder:     fwd,
		timeSvc:       timeSvc,
		diag:          diag,
		logBuf:        logBuf,
		netSvc:        netSvc,
	}

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(requestLogger(log))

	r.Route("/api", func(r chi.Router) {
		r.Get("/system", s.getSystem)
		r.Get("/system/serial-ports", s.getSerialPorts)
		r.Get("/time", s.getTime)

		r.Get("/connections", s.listConnections)
		r.Post("/connections", s.createConnection)
		r.Put("/connections/{id}", s.updateConnection)
		r.Delete("/connections/{id}", s.deleteConnection)

		r.Get("/devices", s.listDevices)
		r.Post("/devices", s.createDevice)
		r.Put("/devices/{id}", s.updateDevice)
		r.Delete("/devices/{id}", s.deleteDevice)

		r.Get("/devices/{id}/datapoints", s.listDataPoints)
		r.Post("/devices/{id}/datapoints", s.createDataPoint)
		r.Put("/datapoints/{id}", s.updateDataPoint)
		r.Delete("/datapoints/{id}", s.deleteDataPoint)

		r.Post("/devices/{id}/test", s.testDeviceConnection)
		r.Post("/datapoints/{id}/test", s.testDataPointRead)

		r.Get("/store-forward/status", s.getStoreForwardStatus)
		r.Get("/store-forward/statistics", s.getStoreForwardStatistics)

		r.Get("/diagnostics", s.getDiagnostics)
		r.Get("/dashboard/summary", s.getDashboardSummary)

		r.Get("/logs", s.getLogs)

		r.Get("/config/export", s.exportConfig)
		r.Post("/config/import", s.importConfig)
		r.Get("/config/settings", s.getSettings)
		r.Put("/config/settings", s.saveSettings)

		r.Get("/system/network", s.getNetworkStatus)
		r.Post("/system/network", s.applyNetwork)
		r.Post("/system/network/dhcp", s.applyNetworkDHCP)
		r.Post("/system/network/confirm", s.confirmNetwork)
	})

	// Static web UI (opt-in): the frontend has no client-side routing (no
	// react-router — see web/src/App.tsx, a single tab-state SPA), so a
	// plain FileServer with no index.html fallback for unmatched paths is
	// sufficient. Mounted after /api so it never shadows an API route.
	if cfg.API.StaticDir != "" {
		r.Handle("/*", http.FileServer(http.Dir(cfg.API.StaticDir)))
	}

	return r
}

func (s *Server) getSystem(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, system.Current(s.cfg.Database.Path))
}

// getSerialPorts lists serial ports available on the gateway host, for the
// RTU device "Interface" dropdown. An empty list is normal when running
// without a physical RS-485 adapter (e.g. in the dev container).
func (s *Server) getSerialPorts(w http.ResponseWriter, r *http.Request) {
	ports, err := system.ListSerialPorts()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string][]string{"ports": ports})
}

// timeDTO matches §16's Time panel fields.
type timeDTO struct {
	SystemTime    time.Time  `json:"system_time"`
	Timezone      string     `json:"timezone"`
	NTPServer     string     `json:"ntp_server,omitempty"`
	NTPStatus     bool       `json:"ntp_status"`
	LastSync      *time.Time `json:"last_sync,omitempty"`
	ClockOffsetMs *float64   `json:"clock_offset_ms,omitempty"`
	RTCStatus     bool       `json:"rtc_status"`
	RTCTime       *time.Time `json:"rtc_time,omitempty"`
	TimeQuality   string     `json:"time_quality"`
}

func (s *Server) getTime(w http.ResponseWriter, r *http.Request) {
	if s.timeSvc == nil {
		writeJSON(w, http.StatusOK, timeDTO{
			SystemTime:  time.Now().UTC(),
			Timezone:    s.cfg.Time.Timezone,
			TimeQuality: string(timeservice.QualityUnsynced),
		})
		return
	}

	st := s.timeSvc.Status()
	dto := timeDTO{
		SystemTime:  st.SystemTime,
		Timezone:    st.Timezone,
		NTPServer:   st.NTPServer,
		NTPStatus:   st.NTPSynced,
		LastSync:    st.LastSync,
		RTCStatus:   st.RTCAvailable,
		RTCTime:     st.RTCTime,
		TimeQuality: string(st.TimeQuality),
	}
	if st.ClockOffset != nil {
		ms := float64(*st.ClockOffset) / float64(time.Millisecond)
		dto.ClockOffsetMs = &ms
	}
	writeJSON(w, http.StatusOK, dto)
}

// reload asks the acquisition Manager to pick up config changes. Errors are
// logged but not surfaced to the caller: the write to the database already
// succeeded, and the next Reload (or gateway restart) will retry.
func (s *Server) reload(r *http.Request) {
	if s.manager == nil {
		return
	}
	if err := s.manager.Reload(r.Context()); err != nil {
		s.log.Error("acquisition reload failed", "error", err)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func requestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log.Debug("request", "method", r.Method, "path", r.URL.Path)
			next.ServeHTTP(w, r)
		})
	}
}
