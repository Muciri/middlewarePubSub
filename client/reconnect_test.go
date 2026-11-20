package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"github.com/franciscovmn/middlewarePubSub/broker"
	"github.com/franciscovmn/middlewarePubSub/internal/contract"
	"github.com/franciscovmn/middlewarePubSub/internal/testproxy"
	"github.com/franciscovmn/middlewarePubSub/peer"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

func fastPeer(peerID string, admin bool, dial string) *peer.Config {
	return &peer.Config{
		PeerID:            peerID,
		Admin:             admin,
		DialAddr:          dial,
		HeartbeatInterval: 10 * time.Millisecond,
		DeadAfter:         300 * time.Millisecond,
		AckTimeout:        200 * time.Millisecond,
		BackoffMin:        10 * time.Millisecond,
		BackoffMax:        50 * time.Millisecond,
	}
}

// cluster tem os dois brokers ligados pelo canal entre brokers, com um
// proxy em cada caminho do cliente.
type cluster struct {
	a, b         *broker.Broker
	addrA, addrB string
	portA, portB uint16
	toA, toB     *testproxy.Proxy
	peerProxy    *testproxy.Proxy
}

func newProxy(t *testing.T, target string) *testproxy.Proxy {
	t.Helper()
	p, err := testproxy.New(target)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func startCluster(t *testing.T) *cluster {
	t.Helper()
	k := &cluster{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	k.portA = clientUDPPort(t)
	k.portB = clientUDPPort(t)
	k.b, k.addrB = startBroker(t, broker.Config{ID: "B", EventPort: k.portB, HeartbeatInterval: 50 * time.Millisecond, Peer: fastPeer("A", false, "")})
	go k.b.ServePeer(ln)
	k.peerProxy = newProxy(t, ln.Addr().String())
	k.a, k.addrA = startBroker(t, broker.Config{ID: "A", EventPort: k.portA, HeartbeatInterval: 50 * time.Millisecond, Peer: fastPeer("B", true, k.peerProxy.Addr())})
	eventually(t, func() bool {
		return k.a.Contracts().Link.Link().Alive && k.b.Contracts().Link.Link().Alive
	}, "enlace entre brokers")
	k.toA = newProxy(t, k.addrA)
	k.toB = newProxy(t, k.addrB)
	return k
}

func clientUDPPort(t *testing.T) uint16 {
	t.Helper()
	c, e := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero})
	if e != nil {
		t.Fatal(e)
	}
	p := uint16(c.LocalAddr().(*net.UDPAddr).Port)
	c.Close()
	return p
}

type reconnectingClient struct {
	*Client
	dp         *MemoryDataPlane
	reconnects chan Reconnect
}

func (k *cluster) dial(t *testing.T, mutate func(*Config)) *reconnectingClient {
	t.Helper()
	rc := &reconnectingClient{reconnects: make(chan Reconnect, 4)}
	var dp *MemoryDataPlane
	rc.Client, dp = dial(t, k.toA.Addr(), "ana", func(cfg *Config) {
		cfg.Brokers = []string{k.toA.Addr(), k.toB.Addr()}
		cfg.HeartbeatTimeout = 300 * time.Millisecond
		cfg.PublishTimeout = time.Minute
		cfg.ReconnectBackoffMin = 10 * time.Millisecond
		cfg.ReconnectBackoffMax = 50 * time.Millisecond
		cfg.OnReconnect = func(r Reconnect) { rc.reconnects <- r }
		if mutate != nil {
			mutate(cfg)
		}
	})
	rc.dp = dp
	return rc
}

func TestUDPEventForwardsOnceToPeerSubscriber(t *testing.T) {
	k := startCluster(t)
	newReal := func(addr string, identity string) *Client {
		dp, err := ListenUDPDataPlane("127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		c, err := Dial(context.Background(), Config{Brokers: []string{addr}, Identity: identity, Token: creds[identity].Token, DataPlane: dp})
		if err != nil {
			dp.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	admin := newReal(k.addrA, "admin")
	publisher := newReal(k.addrA, "ana")
	consumer := newReal(k.addrB, "ana")
	info, err := admin.CreateTopic(context.Background(), topic.Spec{Name: "peer/udp", Kind: topic.KindEphemeral})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := consumer.Subscribe(context.Background(), info.Name)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := publisher.Publish(info.Name, []byte("um salto"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if st, err := pending.Wait(context.Background()); err != nil || st != StatusAccepted {
		t.Fatalf("admissão=%v, %v", st, err)
	}
	select {
	case e := <-sub.Events():
		if string(e.Payload) != "um salto" {
			t.Fatalf("evento=%+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("evento não chegou ao assinante remoto")
	}
	select {
	case e := <-sub.Events():
		t.Fatalf("evento foi reencaminhado/duplicado: %+v", e)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestMeasureRTTHalfTTLWithControlledDelay(t *testing.T) {
	k := startCluster(t)
	k.peerProxy.SetDelay(30 * time.Millisecond)
	eventually(t, func() bool { return k.a.Contracts().Link.Link().RTT >= 50*time.Millisecond }, "RTT medido com atraso de 30 ms em cada sentido")
	dialReal := func(addr, id, token string) *Client {
		dp, e := ListenUDPDataPlane("127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		c, e := Dial(context.Background(), Config{Brokers: []string{addr}, Identity: id, Token: token, DataPlane: dp})
		if e != nil {
			dp.Close()
			t.Fatal(e)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	admin := dialReal(k.addrA, "admin", "ta")
	pub := dialReal(k.addrA, "ana", "t1")
	con := dialReal(k.addrB, "ana", "t1")
	info, e := admin.CreateTopic(context.Background(), topic.Spec{Name: "rtt/half", Kind: topic.KindEphemeral, MaxTTL: 5 * time.Second})
	if e != nil {
		t.Fatal(e)
	}
	sub, e := con.Subscribe(context.Background(), info.Name)
	if e != nil {
		t.Fatal(e)
	}
	_, e = pub.Publish(info.Name, []byte("rtt"), 5*time.Second)
	if e != nil {
		t.Fatal(e)
	}
	select {
	case ev := <-sub.Events():
		remain := time.Until(ev.Deadline)
		measured := k.a.Contracts().Link.Link().RTT / 2
		if remain > 5*time.Second-measured+15*time.Millisecond || remain < 5*time.Second-measured-100*time.Millisecond {
			t.Fatalf("TTL restante=%v, RTT/2=%v", remain, measured)
		}
		fmt.Printf("MEASURE rtt_ms=%.3f ttl_discount_ms=%.3f ttl_remaining_ms=%.3f\n", float64(2*measured.Microseconds())/1000, float64(measured.Microseconds())/1000, float64(remain.Microseconds())/1000)
	case <-time.After(2 * time.Second):
		t.Fatal("evento não chegou com atraso controlado")
	}
}

func TestMeasureEventGapAcrossBrokerFailover(t *testing.T) {
	k := startCluster(t)
	dialUDP := func() *Client {
		dp, e := ListenUDPDataPlane("127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		c, e := Dial(context.Background(), Config{Brokers: []string{k.toA.Addr(), k.toB.Addr()}, Identity: "ana", Token: "t1", DataPlane: dp, HeartbeatTimeout: 300 * time.Millisecond, PublishTimeout: 500 * time.Millisecond, ReconnectBackoffMin: 10 * time.Millisecond, ReconnectBackoffMax: 50 * time.Millisecond})
		if e != nil {
			dp.Close()
			t.Fatal(e)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	adminDP, _ := ListenUDPDataPlane("127.0.0.1:0")
	admin, e := Dial(context.Background(), Config{Brokers: []string{k.toA.Addr()}, Identity: "admin", Token: "ta", DataPlane: adminDP})
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close()
	info, e := admin.CreateTopic(context.Background(), topic.Spec{Name: "failover/events", Kind: topic.KindEphemeral, MaxTTL: 5 * time.Second})
	if e != nil {
		t.Fatal(e)
	}
	subscriber := dialUDP()
	publisher := dialUDP()
	sub, e := subscriber.Subscribe(context.Background(), info.Name)
	if e != nil {
		t.Fatal(e)
	}
	type stamp struct {
		seq uint64
		at  time.Time
	}
	events := make(chan stamp, 512)
	go func() {
		for ev := range sub.Events() {
			events <- stamp{ev.Sequence, time.Now()}
		}
	}()
	var lastBefore time.Time
	var firstAfter time.Time
	var failedAt = time.Now()
	finish := time.Now().Add(1800 * time.Millisecond)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	cut := time.After(600 * time.Millisecond)
	cutDone := false
	for time.Now().Before(finish) {
		select {
		case ev := <-events:
			if !cutDone {
				lastBefore = ev.at
			} else if firstAfter.IsZero() {
				firstAfter = ev.at
			}
		case <-cut:
			failedAt = time.Now()
			k.a.Close()
			cutDone = true
		case <-ticker.C:
			_, _ = publisher.Publish(info.Name, []byte("tick"), 0)
		}
	}
	gap := time.Duration(0)
	if !lastBefore.IsZero() && !firstAfter.IsZero() {
		gap = firstAfter.Sub(lastBefore)
	}
	fmt.Printf("MEASURE failover_event_gap_ms=%.3f last_before_to_failure_ms=%.3f failure_to_first_after_ms=%.3f\n", float64(gap.Microseconds())/1000, float64(failedAt.Sub(lastBefore).Microseconds())/1000, float64(firstAfter.Sub(failedAt).Microseconds())/1000)
	if lastBefore.IsZero() || firstAfter.IsZero() {
		t.Fatalf("falha não delimitou evento antes/depois: antes=%v depois=%v", lastBefore, firstAfter)
	}
}

func (rc *reconnectingClient) waitReconnect(t *testing.T) Reconnect {
	t.Helper()
	select {
	case r := <-rc.reconnects:
		return r
	case <-time.After(3 * time.Second):
		t.Fatal("reconexão não concluída")
	}
	return Reconnect{}
}

func expectOpen(t *testing.T, s *Subscription) {
	t.Helper()
	select {
	case e, ok := <-s.Events():
		if !ok {
			t.Fatalf("canal de eventos fechado: %v", s.Err())
		}
		t.Fatalf("evento inesperado: %+v", e)
	default:
	}
	if s.Err() != nil {
		t.Fatalf("Err = %v", s.Err())
	}
}

func hasSubscriber(b *broker.Broker, ref topic.Ref, session uint64) bool {
	for _, sub := range b.Contracts().Subscriptions.ForTopic(ref) {
		if sub.Session == contract.SessionID(session) {
			return true
		}
	}
	return false
}

func TestFailoverRestoresSubscriptionsAndKeepsSequence(t *testing.T) {
	k := startCluster(t)
	c := k.dial(t, nil)
	info, err := c.CreateTopic(ctx(t), topic.Spec{Name: "sala-1/lances", Kind: topic.KindEphemeral})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := c.Subscribe(ctx(t), "sala-1/lances")
	if err != nil {
		t.Fatal(err)
	}
	c.Publish("sala-1/lances", []byte("1"), 0)
	pending, _ := c.Publish("sala-1/lances", []byte("2"), 0)

	k.a.Close()
	r := c.waitReconnect(t)

	if r.From != "A" || r.To != "B" || r.Restored != 1 || r.Ended != 0 || r.Gap() <= 0 {
		t.Fatalf("reconexão = %+v", r)
	}
	if c.BrokerID() != "B" {
		t.Fatalf("BrokerID = %q", c.BrokerID())
	}
	expectOpen(t, sub)
	if !hasSubscriber(k.b, info.Ref, c.session) {
		t.Fatal("inscrição não restaurada no B")
	}
	select {
	case <-pending.Done():
	default:
		t.Fatal("publicação pendente deveria ter terminado na detecção da falha")
	}
	if st, _ := pending.Wait(ctx(t)); st != StatusUnknown {
		t.Fatalf("pendente = %v", st)
	}

	if _, err := c.Publish("sala-1/lances", []byte("3"), 0); err != nil {
		t.Fatal(err)
	}
	sent := c.dp.Sent()
	if last := sent[len(sent)-1]; last.Sequence != 3 || last.Session != c.session {
		t.Fatalf("última publicação = %+v", last)
	}
	if c.dp.Broker() != netip.MustParseAddrPort(net.JoinHostPort("127.0.0.1", strconv.Itoa(int(k.portB)))) {
		t.Fatalf("destino UDP = %v", c.dp.Broker())
	}

	c.dp.Inject(Event{Ref: info.Ref, Sequence: 9, Payload: []byte("depois")})
	select {
	case e := <-sub.Events():
		if string(e.Payload) != "depois" {
			t.Fatalf("evento = %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("evento não chegou pelo mesmo canal após a troca")
	}
}

func TestSilentBrokerTriggersFailover(t *testing.T) {
	k := startCluster(t)
	c := k.dial(t, nil)
	c.CreateTopic(ctx(t), topic.Spec{Name: "sala", Kind: topic.KindState})
	sub, _ := c.Subscribe(ctx(t), "sala")

	start := time.Now()
	k.toA.Blackhole(true)
	r := c.waitReconnect(t)
	if r.To != "B" || r.Restored != 1 {
		t.Fatalf("reconexão = %+v", r)
	}
	if detect := r.DetectedAt.Sub(start); detect < 300*time.Millisecond {
		t.Fatalf("falha detectada em %v, antes do limite de silêncio", detect)
	}
	expectOpen(t, sub)
}

func TestPublishAndCommandsFailFastWhileReconnecting(t *testing.T) {
	k := startCluster(t)
	c := k.dial(t, nil)
	c.CreateTopic(ctx(t), topic.Spec{Name: "sala", Kind: topic.KindEphemeral})
	sub, _ := c.Subscribe(ctx(t), "sala")
	c.Publish("sala", nil, 0)

	k.toB.Cut()
	k.toA.Cut()
	eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.reconnecting
	}, "início da reconexão")

	start := time.Now()
	if _, err := c.Publish("sala", nil, 0); !errors.Is(err, ErrReconnecting) {
		t.Fatalf("Publish = %v", err)
	}
	if _, err := c.LookupTopic(ctx(t), "sala"); !errors.Is(err, ErrReconnecting) {
		t.Fatalf("LookupTopic = %v", err)
	}
	if _, err := c.Subscribe(ctx(t), "outra"); !errors.Is(err, ErrReconnecting) {
		t.Fatalf("Subscribe = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("falha deveria ser imediata, levou %v", elapsed)
	}
	expectOpen(t, sub)

	k.toB.Restore()
	r := c.waitReconnect(t)
	if r.To != "B" {
		t.Fatalf("reconexão = %+v", r)
	}
	if _, err := c.Publish("sala", nil, 0); err != nil {
		t.Fatalf("Publish após reconectar: %v", err)
	}
	if last := c.dp.Sent(); last[len(last)-1].Sequence != 2 {
		t.Fatalf("sequência após a reconexão = %d", last[len(last)-1].Sequence)
	}
}

func TestNoAutomaticFailback(t *testing.T) {
	k := startCluster(t)
	c := k.dial(t, nil)
	k.toA.Cut()
	c.waitReconnect(t)
	k.toA.Restore()

	time.Sleep(time.Second)
	if c.BrokerID() != "B" {
		t.Fatalf("cliente voltou ao %q sem falha", c.BrokerID())
	}

	k.toB.Cut()
	if r := c.waitReconnect(t); r.From != "B" || r.To != "A" {
		t.Fatalf("segunda troca = %+v", r)
	}
}

func TestRestoreEndsDeletedAndRecreatedTopics(t *testing.T) {
	k := startCluster(t)
	c := k.dial(t, nil)
	for _, name := range []string{"apagado", "recriado", "mantido"} {
		c.CreateTopic(ctx(t), topic.Spec{Name: name, Kind: topic.KindEphemeral})
	}
	deleted, _ := c.Subscribe(ctx(t), "apagado")
	recreated, _ := c.Subscribe(ctx(t), "recriado")
	kept, _ := c.Subscribe(ctx(t), "mantido")

	k.toA.Cut()
	k.toB.Cut()
	admin, _ := dial(t, k.addrB, "admin", nil)
	if err := admin.DeleteTopic(ctx(t), "apagado"); err != nil {
		t.Fatal(err)
	}
	admin.DeleteTopic(ctx(t), "recriado")
	if _, err := admin.CreateTopic(ctx(t), topic.Spec{Name: "recriado", Kind: topic.KindEphemeral}); err != nil {
		t.Fatal(err)
	}
	k.toB.Restore()

	r := c.waitReconnect(t)
	if r.Restored != 1 || r.Ended != 2 {
		t.Fatalf("reconexão = %+v", r)
	}
	recvClosed(t, deleted)
	if !errors.Is(deleted.Err(), ErrTopicDeleted) {
		t.Fatalf("apagado: Err = %v", deleted.Err())
	}
	recvClosed(t, recreated)
	if !errors.Is(recreated.Err(), ErrTopicRecreated) {
		t.Fatalf("recriado: Err = %v", recreated.Err())
	}
	expectOpen(t, kept)

	again, err := c.Subscribe(ctx(t), "recriado")
	if err != nil || again.Topic().Generation != 2 {
		t.Fatalf("nova inscrição = %+v, %v", again, err)
	}
}

func TestUnsubscribeWhileReconnectingIsLocal(t *testing.T) {
	k := startCluster(t)
	c := k.dial(t, nil)
	info, _ := c.CreateTopic(ctx(t), topic.Spec{Name: "sala", Kind: topic.KindEphemeral})
	sub, _ := c.Subscribe(ctx(t), "sala")

	k.toA.Cut()
	k.toB.Cut()
	eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.reconnecting
	}, "início da reconexão")
	if err := sub.Unsubscribe(ctx(t)); err != nil {
		t.Fatal(err)
	}
	recvClosed(t, sub)
	if sub.Err() != nil {
		t.Fatalf("Err = %v", sub.Err())
	}

	k.toB.Restore()
	if r := c.waitReconnect(t); r.Restored != 0 {
		t.Fatalf("reconexão = %+v", r)
	}
	if hasSubscriber(k.b, info.Ref, c.session) {
		t.Fatal("inscrição cancelada foi restaurada")
	}
}

func TestRestoreSkipsBrokerWithoutCatalog(t *testing.T) {
	k := startCluster(t)
	// Broker não administrativo cujo par nunca conecta: catálogo não
	// sincronizado.
	_, addrU := startBroker(t, broker.Config{ID: "U", HeartbeatInterval: 50 * time.Millisecond, Peer: fastPeer("A", false, "")})
	c := k.dial(t, func(cfg *Config) {
		cfg.Brokers = []string{k.toA.Addr(), addrU, k.toB.Addr()}
	})
	c.CreateTopic(ctx(t), topic.Spec{Name: "sala", Kind: topic.KindEphemeral})
	sub, _ := c.Subscribe(ctx(t), "sala")

	k.toA.Cut()
	r := c.waitReconnect(t)
	if r.To != "B" || r.Attempts != 2 || r.Restored != 1 {
		t.Fatalf("reconexão = %+v", r)
	}
	expectOpen(t, sub)
}

func TestReconnectGivesUpAfterTimeout(t *testing.T) {
	k := startCluster(t)
	c := k.dial(t, func(cfg *Config) { cfg.ReconnectTimeout = 200 * time.Millisecond })
	c.CreateTopic(ctx(t), topic.Spec{Name: "sala", Kind: topic.KindEphemeral})
	sub, _ := c.Subscribe(ctx(t), "sala")

	k.toA.Cut()
	k.toB.Cut()
	recvClosed(t, sub)
	if !errors.Is(sub.Err(), ErrConnectionLost) {
		t.Fatalf("Err = %v", sub.Err())
	}
	if _, err := c.Publish("sala", nil, 0); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("Publish = %v", err)
	}
	select {
	case r := <-c.reconnects:
		t.Fatalf("reconexão inesperada: %+v", r)
	default:
	}
}

func TestCloseStopsReconnection(t *testing.T) {
	k := startCluster(t)
	c := k.dial(t, nil)
	sub := func() *Subscription {
		c.CreateTopic(ctx(t), topic.Spec{Name: "sala", Kind: topic.KindEphemeral})
		s, _ := c.Subscribe(ctx(t), "sala")
		return s
	}()
	k.toA.Cut()
	k.toB.Cut()
	eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.reconnecting
	}, "início da reconexão")

	done := make(chan struct{})
	go func() {
		c.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close não interrompeu a reconexão")
	}
	if !errors.Is(sub.Err(), ErrClosed) {
		t.Fatalf("Err = %v", sub.Err())
	}
}
