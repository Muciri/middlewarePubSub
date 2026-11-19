package client

import (
	"context"
	"sync"
	"time"

	"github.com/franciscovmn/middlewarePubSub/topic"
)

type Event struct {
	Topic    string
	Ref      topic.Ref
	Session  uint64 // sessão do publicador
	Sequence uint64
	Payload  []byte
	// Deadline é a validade do evento convertida para o relógio local.
	Deadline time.Time
}

// Subscription entrega os eventos de um tópico. A fila segue a política do
// tópico: no efêmero, 256 eventos descartando o mais antigo; no de estado,
// um evento, com a nova versão substituindo a pendente.
type Subscription struct {
	client *Client
	info   topic.Info
	queue  *eventQueue
}

func (s *Subscription) Topic() topic.Info { return s.info }

// Events é fechado após Unsubscribe ou Close do cliente.
func (s *Subscription) Events() <-chan Event { return s.queue.ch }

// Dropped conta os eventos descartados por fila cheia.
func (s *Subscription) Dropped() uint64 { return s.queue.droppedCount() }

// Unsubscribe remove a inscrição e descarta os eventos ainda não lidos.
func (s *Subscription) Unsubscribe(ctx context.Context) error {
	return ErrNotImplemented
}

// eventQueue aplica a política de fila sobre um canal com buffer. Com fila
// cheia, o evento mais antigo sai para dar lugar ao novo; com capacidade 1,
// isso equivale a substituir o pendente.
type eventQueue struct {
	ch chan Event

	mu      sync.Mutex
	closed  bool
	dropped uint64
}

func newEventQueue(p topic.Policy) *eventQueue {
	capacity := p.QueueCapacity
	if capacity < 1 {
		capacity = 1
	}
	return &eventQueue{ch: make(chan Event, capacity)}
}

func (q *eventQueue) push(e Event) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	for {
		select {
		case q.ch <- e:
			return
		default:
		}
		select {
		case <-q.ch:
			q.dropped++
		default:
		}
	}
}

// close descarta os eventos não lidos e fecha o canal.
func (q *eventQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	for {
		select {
		case <-q.ch:
		default:
			close(q.ch)
			return
		}
	}
}

func (q *eventQueue) droppedCount() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dropped
}
