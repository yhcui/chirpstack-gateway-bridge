package stats

import (
	"encoding/hex"
	"sync"

	"github.com/brocaar/chirpstack-api/go/v3/gw"
	"github.com/golang/protobuf/proto"
)

type Collector struct {
	sync.Mutex

	rxCount uint32
	txCount uint32

	rxPerFreqCount map[uint32]uint32
	txPerFreqCount map[uint32]uint32

	rxPerModulationCount map[string]uint32
	txPerModulationCount map[string]uint32

	txStatusCount map[string]uint32
}

func NewCollector() *Collector {
	c := Collector{}
	c.reset()
	return &c
}

func (c *Collector) CountUplink(uf *gw.UplinkFrame) {
	c.Lock()
	defer c.Unlock()

	b, err := proto.Marshal(uf.GetTxInfo())
	if err != nil {
		return
	}
	txInfoStr := hex.EncodeToString(b)

	c.rxCount = c.rxCount + 1
	c.rxPerFreqCount[uf.GetTxInfo().Frequency] = c.rxPerFreqCount[uf.GetTxInfo().Frequency] + 1
	c.rxPerModulationCount[txInfoStr] = c.rxPerModulationCount[txInfoStr] + 1
}

func (c *Collector) CountDownlink(dl *gw.DownlinkFrame, ack *gw.DownlinkTXAck) {
	c.Lock()
	defer c.Unlock()

	for i, item := range ack.Items {
		if item.Status == gw.TxAckStatus_IGNORED {
			continue
		}

		status := item.Status.String()
		c.txStatusCount[status] = c.txStatusCount[status] + 1

		if item.Status == gw.TxAckStatus_OK && i < len(dl.Items) {
			b, err := proto.Marshal(dl.Items[i].GetTxInfo())
			if err != nil {
				return
			}
			txInfoStr := hex.EncodeToString(b)

			c.txCount = c.txCount + 1
			c.txPerFreqCount[dl.Items[i].GetTxInfo().Frequency] = c.txPerFreqCount[dl.Items[i].GetTxInfo().Frequency] + 1
			c.txPerModulationCount[txInfoStr] = c.txPerModulationCount[txInfoStr] + 1
		}
	}

}

func (c *Collector) ExportStats() gw.GatewayStats {
	c.Lock()
	defer c.Unlock()

	stats := gw.GatewayStats{
		RxPacketsReceived:      c.rxCount,
		RxPacketsReceivedOk:    c.rxCount,
		TxPacketsReceived:      c.txCount,
		TxPacketsEmitted:       c.txCount,
		RxPacketsPerFrequency:  make(map[uint32]uint32),
		TxPacketsPerFrequency:  make(map[uint32]uint32),
		RxPacketsPerModulation: make([]*gw.PerModulationCount, 0),
		TxPacketsPerModulation: make([]*gw.PerModulationCount, 0),
		TxPacketsPerStatus:     make(map[string]uint32),
	}

	for f, c := range c.rxPerFreqCount {
		stats.RxPacketsPerFrequency[f] = c
	}

	for f, c := range c.txPerFreqCount {
		stats.TxPacketsPerFrequency[f] = c
	}

	for bStr, c := range c.rxPerModulationCount {
		b, _ := hex.DecodeString(bStr)
		var txInfo gw.UplinkTXInfo
		_ = proto.Unmarshal(b, &txInfo)

		if modInfo := txInfo.GetLoraModulationInfo(); modInfo != nil {
			stats.RxPacketsPerModulation = append(stats.RxPacketsPerModulation, &gw.PerModulationCount{
				Count: c,
				Modulation: &gw.PerModulationCount_LoraModulationInfo{
					LoraModulationInfo: modInfo,
				},
			})
		}

		if modInfo := txInfo.GetFskModulationInfo(); modInfo != nil {
			stats.RxPacketsPerModulation = append(stats.RxPacketsPerModulation, &gw.PerModulationCount{
				Count: c,
				Modulation: &gw.PerModulationCount_FskModulationInfo{
					FskModulationInfo: modInfo,
				},
			})
		}

		if modInfo := txInfo.GetLrFhssModulationInfo(); modInfo != nil {
			stats.RxPacketsPerModulation = append(stats.RxPacketsPerModulation, &gw.PerModulationCount{
				Count: c,
				Modulation: &gw.PerModulationCount_LrFhssModulationInfo{
					LrFhssModulationInfo: modInfo,
				},
			})
		}
	}

	for bStr, c := range c.txPerModulationCount {
		b, _ := hex.DecodeString(bStr)
		var txInfo gw.DownlinkTXInfo
		_ = proto.Unmarshal(b, &txInfo)

		if modInfo := txInfo.GetLoraModulationInfo(); modInfo != nil {
			stats.TxPacketsPerModulation = append(stats.TxPacketsPerModulation, &gw.PerModulationCount{
				Count: c,
				Modulation: &gw.PerModulationCount_LoraModulationInfo{
					LoraModulationInfo: modInfo,
				},
			})
		}

		if modInfo := txInfo.GetFskModulationInfo(); modInfo != nil {
			stats.TxPacketsPerModulation = append(stats.TxPacketsPerModulation, &gw.PerModulationCount{
				Count: c,
				Modulation: &gw.PerModulationCount_FskModulationInfo{
					FskModulationInfo: modInfo,
				},
			})
		}
	}

	for s, c := range c.txStatusCount {
		stats.TxPacketsPerStatus[s] = c
	}

	c.reset()
	return stats
}

func (c *Collector) reset() {
	c.rxCount = 0
	c.rxCount = 0
	c.txCount = 0
	c.rxPerFreqCount = make(map[uint32]uint32)
	c.txPerFreqCount = make(map[uint32]uint32)
	c.rxPerModulationCount = make(map[string]uint32)
	c.txPerModulationCount = make(map[string]uint32)
	c.txStatusCount = make(map[string]uint32)
}
