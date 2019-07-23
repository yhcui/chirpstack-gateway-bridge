package beacon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBeacon(t *testing.T) {
	assert := require.New(t)

	tests := []struct {
		TimeSinceEpoch time.Duration
		Expected       []byte
	}{
		{
			TimeSinceEpoch: 0xcc020000 * time.Second,
			Expected:       []byte{0x00, 0x00, 0x00, 0x00, 0x02, 0xcc, 0xa2, 0x7e},
		},
	}

	for _, tst := range tests {
		assert.Equal(tst.Expected, getBeacon(tst.TimeSinceEpoch))
	}
}
