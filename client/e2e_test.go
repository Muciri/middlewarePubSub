package client

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/franciscovmn/middlewarePubSub/broker"
	"github.com/franciscovmn/middlewarePubSub/internal/contract"
	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/internal/wire"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

var creds = map[string]broker.Credential{
	"admin": {Token: "ta", Admin: true},
	"ana":   {Token: "t1"},
}

func startBroker(t *testing.T, cfg broker.Config) (*broker.Broker, string) {
	t.Helper()
	if cfg.ID == "" {
		cfg.ID = "A"
	}
	if cfg.Credentials == nil {
		cfg.Credentials = creds
	}
	if cfg.EventPort == 0 {
		cfg.EventPort = 7101
	}
	b, err := broker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go b.Serve(ln)
	t.Cleanup(func() { b.Close() })
	return b, ln.Addr().String()
}

func dial(t *testing.T, addr, identity string, mutate func(*Config)) (*Client, *MemoryDataPlane) {
	t.Helper()
	dp := NewMemoryDataPlane(40100)
	cfg := Config{
		Brokers:   []string{addr},
		Identity:  identity,
		Token:     creds[identity].Token,
		DataPlane: dp,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := Dial(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c, dp
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return c
}

func eventually(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condição não atingida: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func recvClosed(t *testing.T, s *Subscription) {
	t.Helper()
	select {
	case _, ok := <-s.Events():
		if ok {
			t.Fatal("canal de eventos deveria estar fechado")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canal de eventos não foi fechado")
	}
}

func TestDialAnnouncesUDPAndLearnsBrokerAddress(t *testing.T) {
	b, addr := startBroker(t, broker.Config{ID: "B"})
	c, dp := dial(t, addr, "ana", nil)

	if c.BrokerID() != "B" {
		t.Fatalf("BrokerID = %q", c.BrokerID())
	}
	if dp.Broker() != netip.MustParseAddrPort("127.0.0.1:7101") {
		t.Fatalf("destino UDP = %v", dp.Broker())
	}
	s, ok := b.Contracts().Sessions.Get(contract.SessionID(c.session))
	if !ok || s.Identity != "ana" || s.UDPAddr.Port() != 40100 {
		t.Fatalf("sessão no broker = %+v, %v", s, ok)
	}
}

func TestDialFailures(t *testing.T) {
	_, addr := startBroker(t, broker.Config{})

	_, err := Dial(ctx(t), Config{Brokers: []string{addr}, Identity: "ana", Token: "errado"})
	if !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("token errado: %v", err)
	}

	dead, _ := net.Listen("tcp", "127.0.0.1:0")
	deadAddr := dead.Addr().String()
	dead.Close()
	_, err = Dial(ctx(t), Config{Brokers: []string{deadAddr}, Identity: "ana", Token: "t1"})
	if !errors.Is(err, ErrNoBroker) {
		t.Fatalf("sem broker: %v", err)
	}
}

func TestDialSkipsUnreachableBroker(t *testing.T) {
	_, addr := startBroker(t, broker.Config{ID: "B"})
	dead, _ := net.Listen("tcp", "127.0.0.1:0")
	deadAddr := dead.Addr().String()
	dead.Close()

	c, _ := dial(t, deadAddr, "ana", func(cfg *Config) { cfg.Brokers = []string{deadAddr, addr} })
	if c.BrokerID() != "B" {
		t.Fatalf("conectado a %q", c.BrokerID())
	}
}

func TestTopicLifecycle(t *testing.T) {
	_, addr := startBroker(t, broker.Config{})
	c, _ := dial(t, addr, "ana", nil)

	info, err := c.CreateTopic(ctx(t), topic.Spec{Name: "sala-1/placar", Kind: topic.KindState})
	if err != nil {
		t.Fatal(err)
	}
	if info.Generation != 1 || info.Policy != topic.DefaultPolicy(topic.KindState) {
		t.Fatalf("criado = %+v", info)
	}
	if _, err := c.CreateTopic(ctx(t), topic.Spec{Name: "sala-1/placar", Kind: topic.KindState}); !errors.Is(err, ErrTopicExists) {
		t.Fatalf("duplicado: %v", err)
	}
	if _, err := c.CreateTopic(ctx(t), topic.Spec{Name: "Sala", Kind: topic.KindState}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nome inválido: %v", err)
	}
	if got, err := c.LookupTopic(ctx(t), "sala-1/placar"); err != nil || got != info {
		t.Fatalf("Lookup = %+v, %v", got, err)
	}
	if err := c.DeleteTopic(ctx(t), "sala-1/placar"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.LookupTopic(ctx(t), "sala-1/placar"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("Lookup após exclusão: %v", err)
	}
	again, err := c.CreateTopic(ctx(t), topic.Spec{Name: "sala-1/placar", Kind: topic.KindState})
	if err != nil || again.ID != info.ID || again.Generation != 2 {
		t.Fatalf("recriado = %+v, %v", again, err)
	}
}

func TestRestrictedTopicAdmin(t *testing.T) {
	_, addr := startBroker(t, broker.Config{RestrictTopicAdmin: true})
	c, _ := dial(t, addr, "ana", nil)
	if _, err := c.CreateTopic(ctx(t), topic.Spec{Name: "sala", Kind: topic.KindEphemeral}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("esperado ErrForbidden, obtido %v", err)
	}
}

func TestSubscribeDeliversEvents(t *testing.T) {
	b, addr := startBroker(t, broker.Config{})
	c, dp := dial(t, addr, "ana", nil)
	info, _ := c.CreateTopic(ctx(t), topic.Spec{Name: "sala", Kind: topic.KindEphemeral})

	sub, err := c.Subscribe(ctx(t), "sala")
	if err != nil {
		t.Fatal(err)
	}
	if !b.Contracts().Subscriptions.HasLocal(info.Ref) {
		t.Fatal("broker não registrou a inscrição")
	}
	if _, err := c.Subscribe(ctx(t), "sala"); !errors.Is(err, ErrAlreadySubscribed) {
		t.Fatalf("segunda inscrição: %v", err)
	}

	dp.Inject(Event{Ref: topic.Ref{ID: info.ID, Generation: 99}, Sequence: 1})
	dp.Inject(Event{Ref: info.Ref, Sequence: 7, Payload: []byte("lance")})
	e := <-sub.Events()
	if e.Topic != "sala" || e.Sequence != 7 || string(e.Payload) != "lance" {
		t.Fatalf("evento = %+v", e)
	}

	if err := sub.Unsubscribe(ctx(t)); err != nil {
		t.Fatal(err)
	}
	recvClosed(t, sub)
	if sub.Err() != nil {
		t.Fatalf("Err após Unsubscribe = %v", sub.Err())
	}
	if b.Contracts().Subscriptions.HasLocal(info.Ref) {
		t.Fatal("broker manteve a inscrição")
	}
	if _, err := c.Subscribe(ctx(t), "sala"); err != nil {
		t.Fatalf("nova inscrição após Unsubscribe: %v", err)
	}
}

func TestSubscribeUnknownTopic(t *testing.T) {
	_, addr := startBroker(t, broker.Config{})
	c, _ := dial(t, addr, "ana", nil)
	if _, err := c.Subscribe(ctx(t), "nada"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("esperado ErrTopicNotFound, obtido %v", err)
	}
	c.CreateTopic(ctx(t), topic.Spec{Name: "nada", Kind: topic.KindEphemeral})
	if _, err := c.Subscribe(ctx(t), "nada"); err != nil {
		t.Fatalf("nome deveria ser liberado após falha: %v", err)
	}
}

func TestDeletedTopicEndsSubscription(t *testing.T) {
	_, addr := startBroker(t, broker.Config{})
	admin, _ := dial(t, addr, "admin", nil)
	c, _ := dial(t, addr, "ana", nil)
	admin.CreateTopic(ctx(t), topic.Spec{Name: "sala", Kind: topic.KindEphemeral})
	sub, err := c.Subscribe(ctx(t), "sala")
	if err != nil {
		t.Fatal(err)
	}

	if err := admin.DeleteTopic(ctx(t), "sala"); err != nil {
		t.Fatal(err)
	}
	recvClosed(t, sub)
	if !errors.Is(sub.Err(), ErrTopicDeleted) {
		t.Fatalf("Err = %v", sub.Err())
	}
}

func TestPublishSendsDatagramAndReceivesResult(t *testing.T) {
	b, addr := startBroker(t, broker.Config{})
	c, dp := dial(t, addr, "ana", nil)
	info, _ := c.CreateTopic(ctx(t), topic.Spec{Name: "sala", Kind: topic.KindEphemeral})

	p1, err := c.Publish("sala", []byte("a"), 0)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := c.Publish("sala", []byte("b"), 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	sent := dp.Sent()
	if len(sent) != 2 {
		t.Fatalf("%d datagramas enviados", len(sent))
	}
	want := Datagram{Topic: info.Ref, Session: c.session, Sequence: 1, TTL: topic.DefaultEphemeralTTL, Payload: []byte("a")}
	if sent[0].Topic != want.Topic || sent[0].Session != want.Session || sent[0].Sequence != 1 ||
		sent[0].TTL != want.TTL || string(sent[0].Payload) != "a" {
		t.Fatalf("datagrama = %+v", sent[0])
	}
	if sent[1].Sequence != 2 || sent[1].TTL != 100*time.Millisecond {
		t.Fatalf("segundo datagrama = %+v", sent[1])
	}

	results := b.Contracts().Results
	results.Report(contract.AdmissionResult{Session: contract.SessionID(c.session), Topic: info.Ref, Sequence: 2, Status: contract.AdmissionNoSubscribers})
	results.Report(contract.AdmissionResult{Session: contract.SessionID(c.session), Topic: info.Ref, Sequence: 1, Status: contract.AdmissionAccepted})

	if st, err := p1.Wait(ctx(t)); err != nil || st != StatusAccepted {
		t.Fatalf("p1 = %v, %v", st, err)
	}
	if st, err := p2.Wait(ctx(t)); err != nil || st != StatusNoSubscribers {
		t.Fatalf("p2 = %v, %v", st, err)
	}
}

func TestPublishWithoutResultIsUnknown(t *testing.T) {
	_, addr := startBroker(t, broker.Config{})
	c, _ := dial(t, addr, "ana", func(cfg *Config) { cfg.PublishTimeout = 50 * time.Millisecond })
	c.CreateTopic(ctx(t), topic.Spec{Name: "sala", Kind: topic.KindEphemeral})

	p, err := c.Publish("sala", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if st, err := p.Wait(ctx(t)); err != nil || st != StatusUnknown {
		t.Fatalf("Wait = %v, %v", st, err)
	}
}

func TestPublishPendingLimitKeepsSequenceContiguous(t *testing.T) {
	b, addr := startBroker(t, broker.Config{})
	c, dp := dial(t, addr, "ana", func(cfg *Config) { cfg.MaxPending = 2 })
	info, _ := c.CreateTopic(ctx(t), topic.Spec{Name: "sala", Kind: topic.KindEphemeral})

	c.Publish("sala", nil, 0)
	c.Publish("sala", nil, 0)
	if _, err := c.Publish("sala", nil, 0); !errors.Is(err, ErrTooManyPending) {
		t.Fatalf("esperado ErrTooManyPending, obtido %v", err)
	}

	b.Contracts().Results.Report(contract.AdmissionResult{Session: contract.SessionID(c.session), Topic: info.Ref, Sequence: 1, Status: contract.AdmissionAccepted})
	eventually(t, func() bool { return c.pending.len() == 1 }, "resultado processado")

	if _, err := c.Publish("sala", nil, 0); err != nil {
		t.Fatal(err)
	}
	sent := dp.Sent()
	if seq := sent[len(sent)-1].Sequence; seq != 3 {
		t.Fatalf("sequência após recusa = %d, esperado 3", seq)
	}
}

func TestPublishUsesLookupAndRefreshesAfterTopicNotFound(t *testing.T) {
	b, addr := startBroker(t, broker.Config{})
	admin, _ := dial(t, addr, "admin", nil)
	c, dp := dial(t, addr, "ana", nil)

	if _, err := c.Publish("sala", nil, 0); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("tópico inexistente: %v", err)
	}
	old, _ := admin.CreateTopic(ctx(t), topic.Spec{Name: "sala", Kind: topic.KindEphemeral})
	p, err := c.Publish("sala", nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	admin.DeleteTopic(ctx(t), "sala")
	fresh, _ := admin.CreateTopic(ctx(t), topic.Spec{Name: "sala", Kind: topic.KindEphemeral})
	b.Contracts().Results.Report(contract.AdmissionResult{Session: contract.SessionID(c.session), Topic: old.Ref, Sequence: 1, Status: contract.AdmissionTopicNotFound})
	if st, _ := p.Wait(ctx(t)); st != StatusTopicNotFound {
		t.Fatalf("status = %v", st)
	}

	if _, err := c.Publish("sala", nil, 0); err != nil {
		t.Fatal(err)
	}
	sent := dp.Sent()
	last := sent[len(sent)-1]
	if last.Topic != fresh.Ref || last.Sequence != 1 {
		t.Fatalf("publicação após recriação = %+v, esperado ref %+v e sequência 1", last, fresh.Ref)
	}
}

func TestStats(t *testing.T) {
	_, addr := startBroker(t, broker.Config{ID: "B"})
	admin, _ := dial(t, addr, "admin", nil)
	user, _ := dial(t, addr, "ana", nil)

	s, err := admin.Stats(ctx(t))
	if err != nil || s.BrokerID != "B" || s.Counters == nil {
		t.Fatalf("Stats = %+v, %v", s, err)
	}
	if _, err := user.Stats(ctx(t)); !errors.Is(err, ErrForbidden) {
		t.Fatalf("Stats por não admin: %v", err)
	}
}

func TestBrokerFailureEndsClientAfterReconnectTimeout(t *testing.T) {
	b, addr := startBroker(t, broker.Config{})
	c, _ := dial(t, addr, "ana", func(cfg *Config) {
		cfg.PublishTimeout = time.Minute
		cfg.ReconnectTimeout = 200 * time.Millisecond
		cfg.ReconnectBackoffMin = 10 * time.Millisecond
		cfg.ReconnectBackoffMax = 50 * time.Millisecond
	})
	c.CreateTopic(ctx(t), topic.Spec{Name: "sala", Kind: topic.KindEphemeral})
	sub, _ := c.Subscribe(ctx(t), "sala")
	p, _ := c.Publish("sala", nil, 0)

	b.Close()

	recvClosed(t, sub)
	if !errors.Is(sub.Err(), ErrConnectionLost) {
		t.Fatalf("Err = %v", sub.Err())
	}
	if st, err := p.Wait(ctx(t)); err != nil || st != StatusUnknown {
		t.Fatalf("pendente = %v, %v", st, err)
	}
	if _, err := c.LookupTopic(ctx(t), "sala"); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("comando após a queda: %v", err)
	}
}

// silentBroker responde ao Hello e depois não envia mais nada.
func silentBroker(t *testing.T) (addr string, heartbeats <-chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	seen := make(chan struct{}, 100)
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			return
		}
		defer nc.Close()
		wc := wire.NewConn(nc, wire.Options{})
		env := &pb.Envelope{}
		if wc.Receive(env) != nil {
			return
		}
		wc.Send(&pb.Envelope{RequestId: env.RequestId, Body: &pb.Envelope_HelloReply{HelloReply: &pb.HelloReply{
			Status: pb.Status_STATUS_OK, BrokerId: "mudo", HeartbeatIntervalMs: 20, UdpPort: 7001,
		}}})
		for wc.Receive(env) == nil {
			if env.GetHeartbeat() != nil {
				select {
				case seen <- struct{}{}:
				default:
				}
			}
		}
	}()
	return ln.Addr().String(), seen
}

func TestHeartbeatSilenceIsDetected(t *testing.T) {
	addr, heartbeats := silentBroker(t)
	c, _ := dial(t, addr, "ana", func(cfg *Config) { cfg.HeartbeatTimeout = 150 * time.Millisecond })

	select {
	case <-heartbeats:
	case <-time.After(time.Second):
		t.Fatal("cliente não enviou heartbeat")
	}
	eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.reconnecting
	}, "detecção do silêncio do broker")
	if _, err := c.Publish("sala", nil, 0); !errors.Is(err, ErrReconnecting) {
		t.Fatalf("Publish durante a reconexão: %v", err)
	}
}

func TestCloseEndsEverything(t *testing.T) {
	_, addr := startBroker(t, broker.Config{})
	c, _ := dial(t, addr, "ana", func(cfg *Config) { cfg.PublishTimeout = time.Minute })
	c.CreateTopic(ctx(t), topic.Spec{Name: "sala", Kind: topic.KindEphemeral})
	sub, _ := c.Subscribe(ctx(t), "sala")
	p, _ := c.Publish("sala", nil, 0)

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	recvClosed(t, sub)
	if !errors.Is(sub.Err(), ErrClosed) {
		t.Fatalf("Err = %v", sub.Err())
	}
	if st, _ := p.Wait(ctx(t)); st != StatusUnknown {
		t.Fatalf("pendente = %v", st)
	}
	if _, err := c.Publish("sala", nil, 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish após Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("segundo Close: %v", err)
	}
}
