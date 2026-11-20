// Package diagnostics mantém contadores e gauges do plano de dados.
package diagnostics

import (
	"sync"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/contract"
)

type Source struct {
	mu       sync.Mutex
	counters map[string]uint64
	gauges   map[string]int64
}

func New() *Source                { return &Source{counters: map[string]uint64{}, gauges: map[string]int64{}} }
func (s *Source) Inc(name string) { s.mu.Lock(); s.counters[name]++; s.mu.Unlock() }
func (s *Source) AddGauge(name string, delta int64) {
	s.mu.Lock()
	s.gauges[name] += delta
	s.mu.Unlock()
}
func (s *Source) SetGauge(name string, value int64) {
	s.mu.Lock()
	s.gauges[name] = value
	s.mu.Unlock()
}
func (s *Source) Stats() contract.StatsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := map[string]uint64{}
	g := map[string]int64{}
	for k, v := range s.counters {
		c[k] = v
	}
	for k, v := range s.gauges {
		g[k] = v
	}
	return contract.StatsSnapshot{Counters: c, Gauges: g}
}

// CommandLimiter implementa token bucket por identidade.
type CommandLimiter struct {
	mu          sync.Mutex
	buckets     map[string]bucket
	rate, burst float64
	now         func() time.Time
}
type bucket struct {
	tokens float64
	at     time.Time
}

// TokenLimiter limita unidades por identidade (eventos ou bytes) com token bucket.
type TokenLimiter struct {
	mu          sync.Mutex
	buckets     map[string]bucket
	rate, burst float64
	now         func() time.Time
}

func NewTokenLimiter(rate, burst int) *TokenLimiter {
	return &TokenLimiter{buckets: map[string]bucket{}, rate: float64(rate), burst: float64(burst), now: time.Now}
}
func (l *TokenLimiter) Allow(identity string, amount int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	b, ok := l.buckets[identity]
	if !ok {
		b = bucket{tokens: l.burst, at: now}
	} else {
		b.tokens += now.Sub(b.at).Seconds() * l.rate
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.at = now
	}
	if float64(amount) > b.tokens {
		l.buckets[identity] = b
		return false
	}
	b.tokens -= float64(amount)
	l.buckets[identity] = b
	return true
}

func NewCommandLimiter(rate, burst int) *CommandLimiter {
	return &CommandLimiter{buckets: map[string]bucket{}, rate: float64(rate), burst: float64(burst), now: time.Now}
}
func (l *CommandLimiter) AllowCommand(identity string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	b, ok := l.buckets[identity]
	if !ok {
		b = bucket{tokens: l.burst, at: now}
	} else {
		b.tokens += now.Sub(b.at).Seconds() * l.rate
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.at = now
	}
	if b.tokens < 1 {
		l.buckets[identity] = b
		return false
	}
	b.tokens--
	l.buckets[identity] = b
	return true
}
