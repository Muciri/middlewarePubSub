package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/franciscovmn/middlewarePubSub/topic"
)

var ref1 = topic.Ref{ID: 1, Generation: 1}

func TestPendingResolvedByResult(t *testing.T) {
	tbl := newPendingTable(4, time.Minute)
	p, err := tbl.add(ref1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !tbl.resolve(ref1, 10, StatusAccepted) {
		t.Fatal("resolve não encontrou a publicação")
	}
	st, err := p.Wait(context.Background())
	if err != nil || st != StatusAccepted {
		t.Fatalf("Wait = %v, %v", st, err)
	}
	if tbl.len() != 0 {
		t.Fatal("publicação resolvida continua pendente")
	}
}

func TestPendingTimeoutYieldsUnknownWithoutError(t *testing.T) {
	tbl := newPendingTable(4, 20*time.Millisecond)
	p, _ := tbl.add(ref1, 1)

	st, err := p.Wait(context.Background())
	if err != nil || st != StatusUnknown {
		t.Fatalf("Wait = %v, %v; esperado UNKNOWN sem erro", st, err)
	}
	if tbl.resolve(ref1, 1, StatusAccepted) {
		t.Fatal("resultado após o prazo deveria ser ignorado")
	}
	if st, _ := p.Wait(context.Background()); st != StatusUnknown {
		t.Fatalf("resultado tardio alterou o status para %v", st)
	}
}

func TestPendingWaitContextCanceled(t *testing.T) {
	tbl := newPendingTable(4, time.Minute)
	p, _ := tbl.add(ref1, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	st, err := p.Wait(ctx)
	if !errors.Is(err, context.Canceled) || st != StatusUnknown {
		t.Fatalf("Wait = %v, %v", st, err)
	}
	if tbl.len() != 1 {
		t.Fatal("cancelar a espera não libera a vaga da publicação em trânsito")
	}
}

func TestPendingWaitPrefersKnownResultOverCanceledContext(t *testing.T) {
	tbl := newPendingTable(4, time.Minute)
	p, _ := tbl.add(ref1, 1)
	tbl.resolve(ref1, 1, StatusNoSubscribers)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if st, err := p.Wait(ctx); err != nil || st != StatusNoSubscribers {
		t.Fatalf("Wait = %v, %v", st, err)
	}
}

func TestPendingLimitFailsImmediately(t *testing.T) {
	tbl := newPendingTable(DefaultMaxPending, time.Minute)
	for seq := uint64(1); seq <= DefaultMaxPending; seq++ {
		if _, err := tbl.add(ref1, seq); err != nil {
			t.Fatalf("add(%d): %v", seq, err)
		}
	}
	if _, err := tbl.add(ref1, DefaultMaxPending+1); !errors.Is(err, ErrTooManyPending) {
		t.Fatalf("esperado ErrTooManyPending, obtido %v", err)
	}
	tbl.resolve(ref1, 1, StatusAccepted)
	if _, err := tbl.add(ref1, DefaultMaxPending+1); err != nil {
		t.Fatalf("vaga liberada deveria aceitar nova publicação: %v", err)
	}
	tbl.resolveAll(StatusUnknown)
}

func TestPendingKeyIncludesTopic(t *testing.T) {
	tbl := newPendingTable(4, time.Minute)
	other := topic.Ref{ID: 1, Generation: 2}
	pa, _ := tbl.add(ref1, 5)
	pb, err := tbl.add(other, 5)
	if err != nil {
		t.Fatalf("mesma sequência em outra geração deveria ser aceita: %v", err)
	}
	if _, err := tbl.add(ref1, 5); !errors.Is(err, errDuplicatePending) {
		t.Fatalf("esperado errDuplicatePending, obtido %v", err)
	}

	tbl.resolve(other, 5, StatusTopicNotFound)
	if st, _ := pb.Wait(context.Background()); st != StatusTopicNotFound {
		t.Fatalf("status = %v", st)
	}
	select {
	case <-pa.Done():
		t.Fatal("resultado de outro tópico concluiu a publicação errada")
	default:
	}
	tbl.resolveAll(StatusUnknown)
}

func TestPendingResolveAll(t *testing.T) {
	tbl := newPendingTable(4, time.Minute)
	a, _ := tbl.add(ref1, 1)
	b, _ := tbl.add(ref1, 2)
	tbl.resolveAll(StatusUnknown)

	for _, p := range []*PendingPublish{a, b} {
		if st, err := p.Wait(context.Background()); err != nil || st != StatusUnknown {
			t.Fatalf("Wait = %v, %v", st, err)
		}
	}
	if tbl.len() != 0 {
		t.Fatal("tabela deveria ficar vazia")
	}
}
