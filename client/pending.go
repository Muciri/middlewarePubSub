package client

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/franciscovmn/middlewarePubSub/topic"
)

// PendingPublish acompanha uma publicação até o resultado de admissão.
type PendingPublish struct {
	key    pendingKey
	done   chan struct{}
	status Status
	timer  *time.Timer
}

func (p *PendingPublish) Sequence() uint64 { return p.key.seq }

// Done é fechado quando o resultado é conhecido ou o prazo se esgota.
func (p *PendingPublish) Done() <-chan struct{} { return p.done }

// Wait devolve o resultado de admissão. Esgotado o prazo da biblioteca,
// devolve StatusUnknown sem erro. Só há erro se ctx terminar antes.
func (p *PendingPublish) Wait(ctx context.Context) (Status, error) {
	select {
	case <-p.done:
		return p.status, nil
	default:
	}
	select {
	case <-p.done:
		return p.status, nil
	case <-ctx.Done():
		return StatusUnknown, ctx.Err()
	}
}

type pendingKey struct {
	topic topic.Ref
	seq   uint64
}

var errDuplicatePending = errors.New("client: sequência já pendente")

type pendingTable struct {
	mu      sync.Mutex
	max     int
	timeout time.Duration
	items   map[pendingKey]*PendingPublish
}

func newPendingTable(max int, timeout time.Duration) *pendingTable {
	return &pendingTable{max: max, timeout: timeout, items: make(map[pendingKey]*PendingPublish)}
}

func (t *pendingTable) add(ref topic.Ref, seq uint64) (*PendingPublish, error) {
	key := pendingKey{topic: ref, seq: seq}

	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.items) >= t.max {
		return nil, ErrTooManyPending
	}
	if _, ok := t.items[key]; ok {
		return nil, errDuplicatePending
	}
	p := &PendingPublish{key: key, done: make(chan struct{})}
	t.items[key] = p
	p.timer = time.AfterFunc(t.timeout, func() { t.resolve(ref, seq, StatusUnknown) })
	return p, nil
}

// resolve conclui a publicação. Resultados que chegam depois do prazo, ou
// para sequências desconhecidas, são ignorados.
func (t *pendingTable) resolve(ref topic.Ref, seq uint64, status Status) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := pendingKey{topic: ref, seq: seq}
	p, ok := t.items[key]
	if !ok {
		return false
	}
	delete(t.items, key)
	t.finishLocked(p, status)
	return true
}

// resolveAll conclui todas as publicações pendentes, como na troca de broker.
func (t *pendingTable) resolveAll(status Status) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for key, p := range t.items {
		delete(t.items, key)
		t.finishLocked(p, status)
	}
}

func (t *pendingTable) finishLocked(p *PendingPublish, status Status) {
	p.timer.Stop()
	p.status = status
	close(p.done)
}

func (t *pendingTable) len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.items)
}
