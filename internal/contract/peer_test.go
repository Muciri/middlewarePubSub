package contract

import (
	"testing"
	"time"

	"github.com/franciscovmn/middlewarePubSub/topic"
)

func TestInterestUnknownIsNotFresh(t *testing.T) {
	in := NewMemoryInterest().RemoteInterest(topic.Ref{ID: 1, Generation: 1})
	if in.HasSubscribers || in.Fresh(time.Now(), InterestMaxAge) {
		t.Fatalf("visão inexistente não pode ser recente: %+v", in)
	}
}

func TestInterestFreshness(t *testing.T) {
	m := NewMemoryInterest()
	t0 := time.Now()
	ref := topic.Ref{ID: 1, Generation: 1}
	m.Replace([]topic.Ref{ref}, t0)

	in := m.RemoteInterest(ref)
	if !in.HasSubscribers {
		t.Fatal("tópico do snapshot deveria ter assinantes")
	}
	if !in.Fresh(t0.Add(999*time.Millisecond), InterestMaxAge) {
		t.Fatal("visão com 999 ms deveria ser recente")
	}
	if in.Fresh(t0.Add(time.Second), InterestMaxAge) {
		t.Fatal("visão com 1 s não é recente")
	}

	other := m.RemoteInterest(topic.Ref{ID: 2, Generation: 1})
	if other.HasSubscribers || !other.Fresh(t0, InterestMaxAge) {
		t.Fatalf("ausência confirmada esperada para outro tópico: %+v", other)
	}
}

func TestInterestSetTouchInvalidate(t *testing.T) {
	m := NewMemoryInterest()
	ref := topic.Ref{ID: 1, Generation: 1}
	t0 := time.Now()

	m.Set(ref, true, t0)
	m.Set(ref, false, t0.Add(time.Millisecond))
	if m.RemoteInterest(ref).HasSubscribers {
		t.Fatal("Set(false) não removeu o interesse")
	}

	t1 := t0.Add(2 * time.Second)
	m.Touch(t1)
	if !m.RemoteInterest(ref).Fresh(t1, InterestMaxAge) {
		t.Fatal("Touch deveria renovar a visão")
	}

	m.Set(ref, true, t1)
	m.Invalidate()
	in := m.RemoteInterest(ref)
	if in.HasSubscribers || !in.UpdatedAt.IsZero() {
		t.Fatalf("Invalidate deveria zerar a visão: %+v", in)
	}
}

func TestMemoryLink(t *testing.T) {
	var l MemoryLink
	if l.Link().Alive {
		t.Fatal("enlace começa indisponível")
	}
	st := LinkStatus{Alive: true, Since: time.Now(), RTT: 3 * time.Millisecond}
	l.Set(st)
	if l.Link() != st {
		t.Fatalf("Link() = %+v", l.Link())
	}
}
