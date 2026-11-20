package diagnostics

import (
	"testing"
	"time"
)

func TestCommandLimiterUsesIdentityBuckets(t *testing.T) {
	l := NewCommandLimiter(20, 50)
	for i := 0; i < 50; i++ {
		if !l.AllowCommand("ana") {
			t.Fatalf("rajada recusada em %d", i)
		}
	}
	if l.AllowCommand("ana") {
		t.Fatal("excedeu rajada")
	}
	if !l.AllowCommand("bia") {
		t.Fatal("cota não isolada por identidade")
	}
	l.mu.Lock()
	b := l.buckets["ana"]
	b.at = time.Now().Add(-time.Second)
	l.buckets["ana"] = b
	l.mu.Unlock()
	if !l.AllowCommand("ana") {
		t.Fatal("token não recarregou")
	}
}

func TestStatsSnapshotCopiesValues(t *testing.T) {
	s := New()
	s.Inc("expired")
	s.SetGauge("queue_bytes", 42)
	a := s.Stats()
	a.Counters["expired"] = 99
	a.Gauges["queue_bytes"] = 0
	b := s.Stats()
	if b.Counters["expired"] != 1 || b.Gauges["queue_bytes"] != 42 {
		t.Fatalf("snapshot mutou fonte: %+v", b)
	}
}
