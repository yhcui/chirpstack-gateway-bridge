package beacon

import (
	"encoding/binary"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/brocaar/chirpstack-api/go/common"
	"github.com/brocaar/chirpstack-api/go/gw"
	"github.com/brocaar/chirpstack-gateway-bridge/internal/config"
	"github.com/brocaar/lorawan"
	"github.com/brocaar/lorawan/gps"
)

type Beacon struct {
	TimeSinceGPSEpoch time.Duration
	Payload           []byte
	TXInfo            gw.DownlinkTXInfo
}

var (
	beaconChannel chan Beacon

	gatewayIDs      []lorawan.EUI64
	frequency       uint32
	power           int32
	bandwidth       uint32
	spreadingFactor uint32
	codeRate        string
)

func Setup(conf config.Config) error {
	beaconChannel = make(chan Beacon)

	for _, gwStr := range conf.Backend.Amberlink.BeaconGatewayIDs {
		var gatewayID lorawan.EUI64
		if err := gatewayID.UnmarshalText([]byte(gwStr)); err != nil {
			return errors.Wrap(err, "unmarshal gateway id error")
		}

		gatewayIDs = append(gatewayIDs, gatewayID)

		log.WithFields(log.Fields{
			"gateway_id": gatewayID,
		}).Info("beacon: setup beacon for gateway")
	}

	frequency = conf.Backend.Amberlink.BeaconFrequency
	power = conf.Backend.Amberlink.BeaconPower
	bandwidth = conf.Backend.Amberlink.BeaconBandwidth
	spreadingFactor = conf.Backend.Amberlink.BeaconSpreadingFactor
	codeRate = conf.Backend.Amberlink.BeaconCodeRate

	go beaconLoop()

	return nil
}

func GetBeaconChannel() chan Beacon {
	return beaconChannel
}

func beaconLoop() {
	nextBeacon := getNextBeaconStartForTime(time.Now())

	for {
		// sleep until next beacon time
		nextBeaconTime := time.Time(gps.NewTimeFromTimeSinceGPSEpoch(nextBeacon))

		log.WithFields(log.Fields{
			"next_beacon_time": nextBeaconTime,
		}).Info("beacon: sleeping until next beacon")

		time.Sleep(nextBeaconTime.Sub(time.Now()))

		for _, gwID := range gatewayIDs {
			beaconChannel <- Beacon{
				TimeSinceGPSEpoch: nextBeacon,
				Payload:           getBeacon(nextBeacon),
				TXInfo: gw.DownlinkTXInfo{
					GatewayId:  gwID[:],
					Frequency:  frequency,
					Power:      power,
					Modulation: common.Modulation_LORA,
					ModulationInfo: &gw.DownlinkTXInfo_LoraModulationInfo{
						LoraModulationInfo: &gw.LoRaModulationInfo{
							Bandwidth:             bandwidth,
							SpreadingFactor:       spreadingFactor,
							CodeRate:              codeRate,
							PolarizationInversion: true,
						},
					},
					Timing: gw.DownlinkTiming_IMMEDIATELY,
				},
			}

			log.WithFields(log.Fields{
				"time_since_gps_epoch": nextBeacon,
				"gateway_id":           gwID,
			}).Info("beacon: beacon scheduled")
		}

		nextBeacon = nextBeacon + (128 * time.Second)
	}
}

func getBeacon(timeSinceEpoch time.Duration) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint32(b[2:6], uint32((timeSinceEpoch/time.Second)%(1<<32)))
	binary.LittleEndian.PutUint16(b[6:8], crc16(b[0:6]))
	return b
}

// see: https://github.com/Lora-net/packet_forwarder/blob/master/lora_pkt_fwd/src/lora_pkt_fwd.c#L858
func crc16(b []byte) uint16 {
	poly := uint16(0x1021)
	var x uint16

	for i := range b {
		x ^= uint16(b[i]) << 8
		for j := 0; j < 8; j++ {
			if x&0x8000 != 0 {
				x = (x << 1) ^ poly
			} else {
				x = x << 1
			}
		}
	}

	return x
}

func getBeaconStartForTime(ts time.Time) time.Duration {
	gpsTime := gps.Time(ts).TimeSinceGPSEpoch()
	return gpsTime - (gpsTime % (128 * time.Second))
}

func getNextBeaconStartForTime(ts time.Time) time.Duration {
	bst := getBeaconStartForTime(ts)
	return bst + (128 * time.Second)
}
