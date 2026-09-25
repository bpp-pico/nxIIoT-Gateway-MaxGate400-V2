package main

import (
	"log"
	"strings"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// mqttConsumer subscribes to the data topic and hands every message to
// handle, which returns the ack to publish (nil: publish nothing). apply
// switches to a new broker or topic at runtime, from the config page.
type mqttConsumer struct {
	handle func(payload []byte) (ack []byte)

	mu     sync.Mutex
	client mqtt.Client
	broker string
	topic  string
}

func (c *mqttConsumer) apply(broker, topic string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if broker == c.broker && topic == c.topic {
		return
	}
	if c.client != nil {
		c.client.Disconnect(250)
		c.client = nil
	}
	c.broker, c.topic = broker, topic
	if broker == "" {
		log.Printf("mqtt: consumer disabled")
		return
	}

	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID("server-sim").
		SetAutoReconnect(true).
		SetConnectRetry(true)
	opts.SetOnConnectHandler(func(cl mqtt.Client) {
		log.Printf("mqtt: connected to %s, subscribing to %s", broker, topic)
		token := cl.Subscribe(topic, 1, func(client mqtt.Client, msg mqtt.Message) {
			ack := c.handle(msg.Payload())
			if ack == nil {
				return
			}
			// The ack goes to the sender's own topic with "/data" swapped for
			// "/ack", matching the default scheme MQTTAdapter derives in config.Load.
			client.Publish(strings.TrimSuffix(msg.Topic(), "/data")+"/ack", 1, false, ack)
		})
		if token.Wait() && token.Error() != nil {
			log.Printf("mqtt: subscribe failed: %v", token.Error())
		}
	})
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		log.Printf("mqtt: connection lost, will auto-reconnect: %v", err)
	})

	// With ConnectRetry the client keeps trying in the background, so a
	// broker that is not up yet (or a wrong URL) never stops server-sim.
	c.client = mqtt.NewClient(opts)
	c.client.Connect()
}

func (c *mqttConsumer) status() (broker, topic string, connected bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.broker, c.topic, c.client != nil && c.client.IsConnectionOpen()
}
