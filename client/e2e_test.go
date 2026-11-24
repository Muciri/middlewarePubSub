package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
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
		probe, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero})
		if err != nil {
			t.Fatal(err)
		}
		cfg.EventPort = uint16(probe.LocalAddr().(*net.UDPAddr).Port)
		probe.Close()
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
	port := clientUDPPort(t)
	b, addr := startBroker(t, broker.Config{ID: "B", EventPort: port})
	c, dp := dial(t, addr, "ana", nil)

	if c.BrokerID() != "B" {
		t.Fatalf("BrokerID = %q", c.BrokerID())
	}
	if dp.Broker() != netip.MustParseAddrPort(net.JoinHostPort("127.0.0.1", strconv.Itoa(int(port)))) {
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

func TestUDPTransportPublishesAndDeliversBetweenClients(t *testing.T) {
	probe, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	port := uint16(probe.LocalAddr().(*net.UDPAddr).Port)
	probe.Close()
	b, addr := startBroker(t, broker.Config{ID: "udp-e2e", EventPort: port})
	_ = b

	dialReal := func(identity string) *Client {
		dp, err := ListenUDPDataPlane("127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		cfg := Config{Brokers: []string{addr}, Identity: identity, Token: creds[identity].Token, DataPlane: dp, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
		c, err := Dial(context.Background(), cfg)
		if err != nil {
			dp.Close()
			t.Fatalf("Dial %s: %v", identity, err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}

	admin := dialReal("admin")
	consumer := dialReal("ana")
	publisher := dialReal("ana")
	info, err := admin.CreateTopic(ctx(t), topic.Spec{Name: "udp/live", Kind: topic.KindEphemeral})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := consumer.Subscribe(ctx(t), info.Name)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := publisher.Publish(info.Name, []byte("datagrama real"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if status, err := pending.Wait(ctx(t)); err != nil || status != StatusAccepted {
		t.Fatalf("admissão TCP = %v, %v", status, err)
	}
	select {
	case event := <-sub.Events():
		if event.Ref != info.Ref || event.Session != publisher.session || event.Sequence != 1 || string(event.Payload) != "datagrama real" {
			t.Fatalf("evento UDP = %+v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("evento não chegou pelo UDP")
	}
}

func TestPublishOversizedPayloadIsInvalidSize(t *testing.T) {
	_, addr := startBroker(t, broker.Config{})
	c, dp := dial(t, addr, "ana", nil)
	if _, err := c.CreateTopic(ctx(t), topic.Spec{Name: "sala", Kind: topic.KindEphemeral}); err != nil {
		t.Fatal(err)
	}
	p, err := c.Publish("sala", make([]byte, 1025), 0)
	if err != nil {
		t.Fatal(err)
	}
	if st, err := p.Wait(ctx(t)); err != nil || st != StatusInvalidSize || p.Sequence() != 0 {
		t.Fatalf("conteúdo grande = %v, %v, sequência %d", st, err, p.Sequence())
	}
	if n := len(dp.Sent()); n != 0 {
		t.Fatalf("%d datagramas enviados para conteúdo acima do limite", n)
	}
	p, err = c.Publish("sala", make([]byte, 1024), 0)
	if err != nil || p.Sequence() != 1 {
		t.Fatalf("conteúdo no limite: %v, sequência %d", err, p.Sequence())
	}
	if sent := dp.Sent(); len(sent) != 1 || sent[0].Sequence != 1 {
		t.Fatalf("datagramas enviados: %+v", sent)
	}
}

// TestDialOpensUDPByDefault confere que, sem DataPlane, a biblioteca abre o
// UDP e entrega eventos pela rede.
func TestDialOpensUDPByDefault(t *testing.T) {
	_, addr := startBroker(t, broker.Config{})
	dialDefault := func(identity string) *Client {
		c, err := Dial(context.Background(), Config{Brokers: []string{addr}, Identity: identity, Token: creds[identity].Token, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		if err != nil {
			t.Fatalf("Dial %s: %v", identity, err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	consumer, publisher := dialDefault("admin"), dialDefault("ana")
	if consumer.dp.Port() == 0 {
		t.Fatal("plano de dados padrão sem porta UDP")
	}
	info, err := consumer.CreateTopic(ctx(t), topic.Spec{Name: "padrao", Kind: topic.KindEphemeral})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := consumer.Subscribe(ctx(t), info.Name)
	if err != nil {
		t.Fatal(err)
	}
	p, err := publisher.Publish(info.Name, []byte("x"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if st, err := p.Wait(ctx(t)); err != nil || st != StatusAccepted {
		t.Fatalf("admissão = %v, %v", st, err)
	}
	select {
	case ev := <-sub.Events():
		if string(ev.Payload) != "x" {
			t.Fatalf("evento %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("evento não chegou pelo UDP padrão")
	}
}

func TestDialClosesDefaultUDPOnFailure(t *testing.T) {
	abertos := func() int {
		fds, err := os.ReadDir("/dev/fd")
		if err != nil {
			t.Skip("sem /dev/fd para contar descritores")
		}
		return len(fds)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	antes := abertos()
	for range 50 {
		if _, err := Dial(ctx(t), Config{Brokers: []string{addr}, Identity: "ana", Token: "t1"}); err == nil {
			t.Fatal("Dial sem broker deveria falhar")
		}
	}
	if depois := abertos(); depois-antes > 5 {
		t.Fatalf("descritores abertos foram de %d para %d após Dial sem broker", antes, depois)
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

// syncBuffer guarda a saída do log com acesso concorrente.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestSilenceLimitRaisedToThreeBrokerHeartbeats(t *testing.T) {
	_, addr := startBroker(t, broker.Config{HeartbeatInterval: 200 * time.Millisecond})
	var out syncBuffer
	c, _ := dial(t, addr, "ana", func(cfg *Config) {
		cfg.HeartbeatTimeout = 300 * time.Millisecond
		cfg.Logger = slog.New(slog.NewTextHandler(&out, nil))
	})

	if got := c.conn.silence; got != 600*time.Millisecond {
		t.Fatalf("limite efetivo = %v", got)
	}
	if log := out.String(); !strings.Contains(log, "level=WARN") || !strings.Contains(log, "limite_efetivo=600ms") {
		t.Fatalf("aviso ausente: %q", log)
	}
	time.Sleep(time.Second)
	if err := c.state(); err != nil || c.BrokerID() != "A" {
		t.Fatalf("cliente deveria seguir conectado: %v", err)
	}
	c.mu.Lock()
	reconnecting := c.reconnecting
	c.mu.Unlock()
	if reconnecting {
		t.Fatal("falha detectada sem falha real")
	}
}

func TestSilenceLimitKeptWhenEnough(t *testing.T) {
	_, addr := startBroker(t, broker.Config{HeartbeatInterval: 100 * time.Millisecond})
	var out syncBuffer
	c, _ := dial(t, addr, "ana", func(cfg *Config) {
		cfg.HeartbeatTimeout = 300 * time.Millisecond
		cfg.Logger = slog.New(slog.NewTextHandler(&out, nil))
	})
	if got := c.conn.silence; got != 300*time.Millisecond {
		t.Fatalf("limite efetivo = %v", got)
	}
	if out.String() != "" {
		t.Fatalf("aviso inesperado: %q", out.String())
	}
}
