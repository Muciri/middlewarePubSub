package eventwire

import (
	"testing"
	"time"
)

func TestFairSchedulerPublisherLimitAndRoundRobin(t *testing.T) {
	s := NewFairScheduler()
	e := QueuedEvent{Payload: []byte{1}, Deadline: time.Now().Add(time.Second)}
	for i := 0; i < PublisherQueueLimit; i++ {
		if !s.Enqueue("a", e) {
			t.Fatalf("enqueue %d recusado", i)
		}
	}
	if s.Enqueue("a", e) {
		t.Fatal("excedeu limite por publicador")
	}
	if !s.Enqueue("b", e) {
		t.Fatal("segundo publicador recusado")
	}
	first, _, ok := s.Next(DispatchTurnBytes)
	if !ok || first != "a" {
		t.Fatalf("1o turno = %q", first)
	}
	second, _, ok := s.Next(DispatchTurnBytes)
	if !ok || second != "b" {
		t.Fatalf("2o turno = %q", second)
	}
}

func TestSchedulerExpiresQueuedEventAndEnforcesByteBudget(t *testing.T) {
	s := NewFairScheduler()
	e := QueuedEvent{Payload: make([]byte, MaxQueuedBytes-HeaderSize), Deadline: time.Now()}
	if !s.Enqueue("a", e) {
		t.Fatal("evento no limite de memória recusado")
	}
	if s.Enqueue("b", QueuedEvent{Payload: []byte{1}}) {
		t.Fatal("limite de bytes excedido")
	}
	_, got, ok := s.Next(DispatchTurnBytes)
	if !ok {
		t.Fatal("evento ausente")
	}
	if Remaining(got, time.Now()) != 0 {
		t.Fatal("evento expirado ainda válido")
	}
	if s.Bytes() != 0 || s.Len() != 0 {
		t.Fatalf("fila não esvaziou: %d bytes, %d eventos", s.Bytes(), s.Len())
	}
}
