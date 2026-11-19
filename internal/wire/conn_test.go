package wire

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/pb"
)

func heartbeat(n uint64) *pb.Envelope {
	return &pb.Envelope{Body: &pb.Envelope_Heartbeat{Heartbeat: &pb.Heartbeat{Nonce: n}}}
}

func TestConnConcurrentSendKeepsFramesIntact(t *testing.T) {
	a, b := net.Pipe()
	sender := NewConn(a, Options{})
	receiver := NewConn(b, Options{})
	defer sender.Close()
	defer receiver.Close()

	const writers, perWriter = 8, 50
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWriter {
				if err := sender.Send(heartbeat(uint64(w*perWriter + i))); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}

	seen := make(map[uint64]bool)
	for range writers * perWriter {
		env := &pb.Envelope{}
		if err := receiver.Receive(env); err != nil {
			t.Fatal(err)
		}
		n := env.GetHeartbeat().GetNonce()
		if seen[n] {
			t.Fatalf("nonce %d repetido", n)
		}
		seen[n] = true
	}
	wg.Wait()
}

func TestConnWriteTimeoutBreaksConnection(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	c := NewConn(a, Options{WriteTimeout: 50 * time.Millisecond})

	start := time.Now()
	err := c.Send(heartbeat(1))
	if !errors.Is(err, ErrConnBroken) {
		t.Fatalf("esperado ErrConnBroken, obtido %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Send demorou %v", elapsed)
	}
	if err := c.Send(heartbeat(2)); !errors.Is(err, ErrConnBroken) {
		t.Fatalf("Send após quebra: esperado ErrConnBroken, obtido %v", err)
	}
	if _, err := b.Read(make([]byte, 1)); err == nil {
		t.Fatal("o outro lado deveria ver a conexão encerrada")
	}
}

func TestConnOversizeSendDoesNotBreakConnection(t *testing.T) {
	a, b := net.Pipe()
	sender := NewConn(a, Options{MaxFrame: 16})
	receiver := NewConn(b, Options{MaxFrame: 16})
	defer sender.Close()
	defer receiver.Close()

	big := &pb.Envelope{Body: &pb.Envelope_LookupTopic{LookupTopic: &pb.LookupTopic{Name: "um-nome-de-topico-longo-demais"}}}
	if err := sender.Send(big); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("esperado ErrFrameTooLarge, obtido %v", err)
	}

	go sender.Send(heartbeat(7))
	env := &pb.Envelope{}
	if err := receiver.Receive(env); err != nil {
		t.Fatal(err)
	}
	if env.GetHeartbeat().GetNonce() != 7 {
		t.Fatalf("mensagem inesperada: %v", env)
	}
}

func TestConnReadTimeout(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	c := NewConn(b, Options{ReadTimeout: 30 * time.Millisecond})
	defer c.Close()

	err := c.Receive(&pb.Envelope{})
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("esperado erro de prazo, obtido %v", err)
	}
}
