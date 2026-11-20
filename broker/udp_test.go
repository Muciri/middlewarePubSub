package broker

import (
	"testing"
	"time"
)

func TestForwardedTTLSubtractsHalfRTT(t *testing.T) {
	if got := forwardedTTL(500*time.Millisecond, 80*time.Millisecond); got != 460*time.Millisecond {
		t.Fatalf("TTL após RTT/2=%v", got)
	}
	if got := forwardedTTL(20*time.Millisecond, 80*time.Millisecond); got != 0 {
		t.Fatalf("TTL deveria expirar, obtido %v", got)
	}
}
