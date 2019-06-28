package amberlink

import (
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/brocaar/lora-gateway-bridge/internal/backend/amberlink/packets"
	"github.com/brocaar/lora-gateway-bridge/internal/config"
	"github.com/brocaar/loraserver/api/gw"
	"github.com/brocaar/lorawan"
)

// Backend implements the Amberlink MQTT backend.
type Backend struct {
	sync.RWMutex

	conn       mqtt.Client
	closed     bool
	clientOpts *mqtt.ClientOptions

	txAckChan        chan gw.DownlinkTXAck
	gatewayStatsChan chan gw.GatewayStats
	uplinkFrameChan  chan gw.UplinkFrame

	gateways gateways
}

// NewBackend creates a new Backend.
func NewBackend(conf config.Config) (*Backend, error) {
	b := Backend{
		clientOpts: mqtt.NewClientOptions(),

		txAckChan:        make(chan gw.DownlinkTXAck),
		gatewayStatsChan: make(chan gw.GatewayStats),
		uplinkFrameChan:  make(chan gw.UplinkFrame),

		gateways: gateways{
			gateways:       make(map[lorawan.EUI64]gateway),
			connectChan:    make(chan lorawan.EUI64),
			disconnectChan: make(chan lorawan.EUI64),
		},
	}

	b.clientOpts.AddBroker(conf.Backend.Amberlink.Server)
	b.clientOpts.SetUsername(conf.Backend.Amberlink.Username)
	b.clientOpts.SetPassword(conf.Backend.Amberlink.Password)
	b.clientOpts.SetProtocolVersion(4)
	b.clientOpts.SetCleanSession(true)
	b.clientOpts.SetMaxReconnectInterval(10 * time.Second)
	b.clientOpts.SetOnConnectHandler(b.onConnected)

	b.connectLoop()

	return &b, nil
}

// GetDownlinkTXAckChan returns the downlink tx ack channel.
func (b *Backend) GetDownlinkTXAckChan() chan gw.DownlinkTXAck {
	return b.txAckChan
}

// GetGatewayStatsChan returns the gateway stats channel.
func (b *Backend) GetGatewayStatsChan() chan gw.GatewayStats {
	return b.gatewayStatsChan
}

// GetUplinkFrameChan returns the uplink frame channel.
func (b *Backend) GetUplinkFrameChan() chan gw.UplinkFrame {
	return b.uplinkFrameChan
}

// GetConnectChan returns the channel for received gateway connections.
func (b *Backend) GetConnectChan() chan lorawan.EUI64 {
	return b.gateways.connectChan
}

// GetDisconnectChan returns the channel for disconnected gateway connections.
func (b *Backend) GetDisconnectChan() chan lorawan.EUI64 {
	return b.gateways.disconnectChan
}

// Close closes the backend.
func (b *Backend) Close() error {
	return nil
}

// SendDownlinkFrame sends the given downlink frame.
func (b *Backend) SendDownlinkFrame(pl gw.DownlinkFrame) error {
	var gatewayID lorawan.EUI64
	copy(gatewayID[:], pl.TxInfo.GatewayId)

	gw, err := b.gateways.get(gatewayID)
	if err != nil {
		return errors.Wrap(err, "get gateway error")
	}

	pullResp, err := packets.GetPullRespPacket(gw.protocolVersion, uint16(pl.Token), pl)
	if err != nil {
		return errors.Wrap(err, "get PullRespPacket error")
	}

	bytes, err := pullResp.MarshalBinary()
	if err != nil {
		return errors.Wrap(err, "backend/amberlink: marshal PullRespPacket error")
	}

	b.RLock()
	defer b.RUnlock()

	topic := "$gw.sys/down/" + strings.ToUpper(gatewayID.String())
	log.WithField("topic", topic).Info("backend/amberlink: publishing downlink command")

	if token := b.conn.Publish(topic, 0, false, bytes); token.Wait() && token.Error() != nil {
		return errors.Wrap(err, "publish downlink command error")
	}

	return nil
}

// ApplyConfiguration applies the given configuration to the gateway.
func (b *Backend) ApplyConfiguration(pl gw.GatewayConfiguration) error {
	return nil
}

func (b *Backend) connect() error {
	b.Lock()
	defer b.Unlock()

	b.conn = mqtt.NewClient(b.clientOpts)
	if token := b.conn.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (b *Backend) connectLoop() {
	for {
		if err := b.connect(); err != nil {
			log.WithError(err).Error("integration/amberlink: connection error")
			time.Sleep(time.Second * 2)

		} else {
			break
		}
	}
}

func (b *Backend) onConnected(c mqtt.Client) {
	b.RLock()
	defer b.RUnlock()

	log.Info("integration/amberlink: connected to mqtt broker")

	for {
		log.WithField("topic", "$gw.sys/up/#").Info("integration/amberlink: subscribing to uplink events")
		if token := b.conn.Subscribe("$gw.sys/up/#", 0, b.handleUpEvent); token.Wait() && token.Error() != nil {
			log.WithError(token.Error()).Error("integration/amberlink: subscribe uplink events error")
			time.Sleep(time.Second * 2)
			continue
		}

		break
	}
}

func (b *Backend) handleUpEvent(c mqtt.Client, msg mqtt.Message) {
	if err := func() error {
		var p packets.PushDataPacket
		if err := p.UnmarshalBinary(msg.Payload()); err != nil {
			return errors.Wrap(err, "unmarshal payload error")
		}

		// set gateway
		err := b.gateways.set(p.GatewayMAC, gateway{
			lastSeen:        time.Now(),
			protocolVersion: p.ProtocolVersion,
		})
		if err != nil {
			return errors.Wrap(err, "set gateway error")
		}

		// gateway stats
		stats, err := p.GetGatewayStats()
		if err != nil {
			return errors.Wrap(err, "get stats error")
		}
		if stats != nil {
			b.handleStats(p.GatewayMAC, *stats)
		}

		// uplink frames
		uplinkFrames, err := p.GetUplinkFrames(false)
		if err != nil {
			return errors.Wrap(err, "get uplink frames error")
		}
		b.handleUplinkFrames(uplinkFrames)

		return nil
	}(); err != nil {
		log.WithError(err).WithFields(log.Fields{
			"topic": msg.Topic(),
			"body":  string(msg.Payload()),
		}).Error("integration/amberlink: handle uplink event error")
	}

}

func (b *Backend) handleStats(gatewayID lorawan.EUI64, stats gw.GatewayStats) {
	b.gatewayStatsChan <- stats
}

func (b *Backend) handleUplinkFrames(uplinkFrames []gw.UplinkFrame) error {
	for i := range uplinkFrames {
		b.uplinkFrameChan <- uplinkFrames[i]
	}

	return nil
}
