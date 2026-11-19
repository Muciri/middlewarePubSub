package client

import (
	"sync"
	"testing"

	"github.com/franciscovmn/middlewarePubSub/topic"
)

func drain(q *eventQueue) []uint64 {
	var seqs []uint64
	for {
		select {
		case e := <-q.ch:
			seqs = append(seqs, e.Sequence)
		default:
			return seqs
		}
	}
}

func TestEphemeralQueueDropsOldest(t *testing.T) {
	q := newEventQueue(topic.DefaultPolicy(topic.KindEphemeral))
	const total = topic.EphemeralQueueCapacity + 10
	for seq := uint64(1); seq <= total; seq++ {
		q.push(Event{Sequence: seq})
	}

	got := drain(q)
	if len(got) != topic.EphemeralQueueCapacity {
		t.Fatalf("fila com %d eventos, esperado %d", len(got), topic.EphemeralQueueCapacity)
	}
	if got[0] != 11 || got[len(got)-1] != total {
		t.Fatalf("restaram %d..%d, esperado 11..%d", got[0], got[len(got)-1], total)
	}
	if q.droppedCount() != 10 {
		t.Fatalf("descartados = %d, esperado 10", q.droppedCount())
	}
}

func TestStateQueueReplacesPending(t *testing.T) {
	q := newEventQueue(topic.DefaultPolicy(topic.KindState))
	for seq := uint64(1); seq <= 3; seq++ {
		q.push(Event{Sequence: seq})
	}
	got := drain(q)
	if len(got) != 1 || got[0] != 3 {
		t.Fatalf("fila de estado = %v, esperado [3]", got)
	}
	if q.droppedCount() != 2 {
		t.Fatalf("descartados = %d, esperado 2", q.droppedCount())
	}
}

func TestQueueCloseDiscardsAndCloses(t *testing.T) {
	q := newEventQueue(topic.DefaultPolicy(topic.KindEphemeral))
	q.push(Event{Sequence: 1})
	q.push(Event{Sequence: 2})
	q.close()

	if _, ok := <-q.ch; ok {
		t.Fatal("eventos pendentes deveriam ser descartados no fechamento")
	}
	q.push(Event{Sequence: 3})
	q.close()
}

func TestQueueConcurrentPushAndRead(t *testing.T) {
	q := newEventQueue(topic.Policy{QueueCapacity: 4})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for seq := uint64(1); seq <= 10000; seq++ {
			q.push(Event{Sequence: seq})
		}
		q.close()
	}()

	var last uint64
	for e := range q.ch {
		if e.Sequence <= last {
			t.Fatalf("ordem violada: %d depois de %d", e.Sequence, last)
		}
		last = e.Sequence
	}
	wg.Wait()
}
