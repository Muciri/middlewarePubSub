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

	mu    sync.Mutex
	ended bool
	err   error
}

func newSubscription(c *Client, info topic.Info) *Subscription {
	return &Subscription{client: c, info: info, queue: newEventQueue(info.Policy)}
}

func (s *Subscription) Topic() topic.Info { return s.info }

// Events é fechado quando a inscrição termina; Err informa o motivo.
func (s *Subscription) Events() <-chan Event { return s.queue.ch }

// Err é nil enquanto a inscrição está ativa ou se ela terminou por
// Unsubscribe. Nos demais casos indica o motivo: ErrTopicDeleted,
// ErrConnectionLost ou ErrClosed.
func (s *Subscription) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Dropped conta os eventos descartados por fila cheia.
func (s *Subscription) Dropped() uint64 { return s.queue.droppedCount() }

// Unsubscribe remove a inscrição e descarta os eventos ainda não lidos.
// Chamar após o fim da inscrição não tem efeito.
func (s *Subscription) Unsubscribe(ctx context.Context) error {
	s.mu.Lock()
	ended := s.ended
	s.mu.Unlock()
	if ended {
		return nil
	}
	return s.client.unsubscribe(ctx, s)
}

func (s *Subscription) finish(err error) {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	s.err = err
	s.mu.Unlock()
	s.queue.close()
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
