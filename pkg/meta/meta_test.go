package meta

import (
	"math/rand"
	"testing"
	"time"
)

func TestTimestampEncodeDecode(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 10; i++ {
		ts := rand.Uint64()
		tp := DecodeTs(ts)
		ts1 := EncodeTs(tp)
		if ts != ts1 {
			t.Fatalf("%d != %d", ts, ts1)
		}
	}
}
