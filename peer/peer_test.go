package peer

import (
	"errors"
	"net"
	"net/netip"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/internal/testproxy"
	"github.com/franciscovmn/middlewarePubSub/internal/wire"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

type fakeStore struct {
	catalog *topic.Catalog

	mu        sync.Mutex
	local     []topic.Ref
	applied   []Update
	snapshots int
	link      *Link
}

func newFakeStore() *fakeStore { return &fakeStore{catalog: topic.NewCatalog()} }

func (f *fakeStore) CatalogSnapshot() topic.Snapshot { return f.catalog.Snapshot() }
func (f *fakeStore) CatalogVersion() uint64          { return f.catalog.Version() }

func (f *fakeStore) ApplyUpdate(u Update) error {
	var err error
	switch u.Kind {
	case OpCreate:
		err = f.catalog.Put(u.Topic, u.Version)
	case OpDelete:
		_, err = f.catalog.Remove(u.Topic.Ref, u.Version)
	}
	if err == nil {
		f.mu.Lock()
		f.applied = append(f.applied, u)
		f.mu.Unlock()
	}
	return err
}

func (f *fakeStore) ApplySnapshot(s topic.Snapshot) error {
	_, err := f.catalog.Restore(s)
	f.mu.Lock()
	f.snapshots++
	f.mu.Unlock()
	return err
}

func (f *fakeStore) WithLocalInterest(fn func([]topic.Ref)) {
	f.mu.Lock()
	refs := append([]topic.Ref(nil), f.local...)
	f.mu.Unlock()
	fn(refs)
}

// Execute faz no broker administrativo o mesmo que o broker real: planeja,
// replica e só então aplica.
func (f *fakeStore) Execute(req Request) Result {
	info, err := f.catalog.PlanCreate(req.Spec)
	if err != nil {
		return Result{Status: pb.Status_STATUS_TOPIC_EXISTS, Detail: err.Error()}
	}
	u := Update{OperationID: req.OperationID, Version: f.catalog.Version() + 1, Kind: OpCreate, Topic: info}
	if err := f.link.Replicate(u); err != nil {
		return Result{Status: pb.Status_STATUS_UNAVAILABLE}
	}
	f.catalog.Put(info, u.Version)
	return Result{Topic: info, Status: pb.Status_STATUS_OK}
}

func (f *fakeStore) appliedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.applied)
}

func (f *fakeStore) snapshotCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snapshots
}

func fastConfig(local, peer string, admin bool) Config {
	return Config{
		LocalID: local, PeerID: peer, Admin: admin,
		HeartbeatInterval: 10 * time.Millisecond,
		DeadAfter:         300 * time.Millisecond,
		AckTimeout:        200 * time.Millisecond,
		BackoffMin:        10 * time.Millisecond,
		BackoffMax:        50 * time.Millisecond,
	}
}

type pair struct {
	admin, replica           *Link
	adminStore, replicaStore *fakeStore
	proxy                    *testproxy.Proxy
	replicaAddr              string
	adminOverride            netip.AddrPort
}

// startPair sobe o broker não administrativo escutando e o administrativo
// discando através do proxy.
func startPair(t *testing.T, prepare func(p *pair)) *pair {
	t.Helper()
	p := &pair{adminStore: newFakeStore(), replicaStore: newFakeStore()}
	if prepare != nil {
		prepare(p)
	}

	var err error
	rcfg := fastConfig("B", "A", false)
	rcfg.EventPort = 17101
	p.replica, err = New(rcfg, p.replicaStore)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p.replicaAddr = ln.Addr().String()
	go p.replica.Serve(ln)

	p.proxy, err = testproxy.New(p.replicaAddr)
	if err != nil {
		t.Fatal(err)
	}
	cfg := fastConfig("A", "B", true)
	cfg.DialAddr = p.proxy.Addr()
	cfg.EventPort = 17201
	cfg.PeerUDPOverride = p.adminOverride
	p.admin, err = New(cfg, p.adminStore)
	if err != nil {
		t.Fatal(err)
	}
	p.adminStore.link = p.admin
	p.admin.Start()

	t.Cleanup(func() {
		p.admin.Close()
		p.replica.Close()
		p.proxy.Close()
	})
	waitAlive(t, p.admin, p.replica)
	return p
}

func eventually(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condição não atingida: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitAlive(t *testing.T, links ...*Link) {
	t.Helper()
	eventually(t, func() bool {
		for _, l := range links {
			if !l.Link().Alive {
				return false
			}
		}
		return true
	}, "enlace vivo")
}

func waitDown(t *testing.T, links ...*Link) {
	t.Helper()
	eventually(t, func() bool {
		for _, l := range links {
			if l.Link().Alive {
				return false
			}
		}
		return true
	}, "enlace fora")
}

func planCreate(t *testing.T, s *fakeStore, name string) Update {
	t.Helper()
	info, err := s.catalog.PlanCreate(topic.Spec{Name: name, Kind: topic.KindEphemeral})
	if err != nil {
		t.Fatal(err)
	}
	return Update{OperationID: NewOperationID(), Version: s.catalog.Version() + 1, Kind: OpCreate, Topic: info}
}

func TestConfigValidation(t *testing.T) {
	cases := map[string]Config{
		"sem ids":         {},
		"mesmo id":        {LocalID: "A", PeerID: "A"},
		"admin sem par":   {LocalID: "A", PeerID: "B", Admin: true},
		"heartbeat longo": {LocalID: "A", PeerID: "B", HeartbeatInterval: 6 * time.Second},
		"recuo invertido": {LocalID: "A", PeerID: "B", BackoffMin: time.Second, BackoffMax: time.Millisecond},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := cfg.withDefaults(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("esperado ErrInvalidConfig, obtido %v", err)
			}
		})
	}
	cfg, err := Config{LocalID: "B", PeerID: "A"}.withDefaults()
	if err != nil || cfg.HeartbeatInterval != 250*time.Millisecond || cfg.DeadAfter != 5*time.Second {
		t.Fatalf("padrões = %+v, %v", cfg, err)
	}
}

func TestHandshakeSendsCatalogSnapshotWithTombstones(t *testing.T) {
	p := startPair(t, func(p *pair) {
		c := p.adminStore.catalog
		c.Create(topic.Spec{Name: "sala-1/lances", Kind: topic.KindEphemeral})
		c.Create(topic.Spec{Name: "sala-1/placar", Kind: topic.KindState})
		c.Delete("sala-1/lances")
		p.replicaStore.catalog.Create(topic.Spec{Name: "intruso", Kind: topic.KindState})
	})
	if !reflect.DeepEqual(p.replicaStore.catalog.Snapshot(), p.adminStore.catalog.Snapshot()) {
		t.Fatalf("catálogo do par = %+v", p.replicaStore.catalog.Snapshot())
	}
}

func TestReplicateAppliesOnReplica(t *testing.T) {
	p := startPair(t, nil)
	u := planCreate(t, p.adminStore, "sala")
	if err := p.admin.Replicate(u); err != nil {
		t.Fatal(err)
	}
	if got, ok := p.replicaStore.catalog.Lookup("sala"); !ok || got != u.Topic {
		t.Fatalf("par = %+v, %v", got, ok)
	}
	if _, ok := p.adminStore.catalog.Lookup("sala"); ok {
		t.Fatal("Replicate não deve aplicar no broker administrativo")
	}
}

func TestReplicateIsIdempotent(t *testing.T) {
	p := startPair(t, nil)
	u := planCreate(t, p.adminStore, "sala")
	if err := p.admin.Replicate(u); err != nil {
		t.Fatal(err)
	}
	if err := p.admin.Replicate(u); err != nil {
		t.Fatalf("operação repetida: %v", err)
	}
	again := u
	again.OperationID = NewOperationID()
	if err := p.admin.Replicate(again); err != nil {
		t.Fatalf("versão já aplicada: %v", err)
	}
	if n := p.replicaStore.appliedCount(); n != 1 {
		t.Fatalf("aplicada %d vezes", n)
	}
	if !p.admin.Link().Alive {
		t.Fatal("enlace não deveria cair")
	}
}

func TestVersionGapForcesResync(t *testing.T) {
	p := startPair(t, nil)
	u := planCreate(t, p.adminStore, "sala")
	u.Version += 5
	if err := p.admin.Replicate(u); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("esperado ErrUnavailable, obtido %v", err)
	}
	eventually(t, func() bool { return p.replicaStore.snapshotCount() >= 2 }, "novo snapshot após a recusa")
	if _, ok := p.replicaStore.catalog.Lookup("sala"); ok {
		t.Fatal("versão fora de sequência foi aplicada")
	}
}

func TestReplicateUnavailableWhenLinkDown(t *testing.T) {
	p := startPair(t, nil)
	p.proxy.Cut()
	waitDown(t, p.admin, p.replica)
	if err := p.admin.Replicate(planCreate(t, p.adminStore, "sala")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("esperado ErrUnavailable, obtido %v", err)
	}
	if _, err := p.replica.Forward(Request{OperationID: NewOperationID(), Kind: OpCreate, Spec: topic.Spec{Name: "x", Kind: topic.KindState}}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Forward: esperado ErrUnavailable, obtido %v", err)
	}
	if err := p.admin.Announce(topic.Ref{ID: 1, Generation: 1}, true); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Announce: esperado ErrUnavailable, obtido %v", err)
	}
}

func TestAckTimeoutDropsLinkAndSnapshotUndoes(t *testing.T) {
	p := startPair(t, nil)
	p.proxy.SetDelay(150 * time.Millisecond)
	if err := p.admin.Replicate(planCreate(t, p.adminStore, "sala")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("esperado ErrUnavailable, obtido %v", err)
	}
	p.proxy.SetDelay(0)
	eventually(t, func() bool {
		_, ok := p.replicaStore.catalog.Lookup("sala")
		return !ok && p.admin.Link().Alive && p.replicaStore.snapshotCount() >= 2
	}, "snapshot desfazendo a alteração sem ACK")
}

func TestForwardExecutesOnAdmin(t *testing.T) {
	p := startPair(t, nil)
	res, err := p.replica.Forward(Request{OperationID: "op-1", Kind: OpCreate, Spec: topic.Spec{Name: "sala", Kind: topic.KindEphemeral}})
	if err != nil || res.Status != pb.Status_STATUS_OK {
		t.Fatalf("Forward = %+v, %v", res, err)
	}
	if got, ok := p.replicaStore.catalog.Lookup("sala"); !ok || got != res.Topic {
		t.Fatal("alteração deveria estar aplicada no par antes da resposta")
	}
	if _, ok := p.adminStore.catalog.Lookup("sala"); !ok {
		t.Fatal("broker administrativo não aplicou")
	}
	if u := p.replicaStore.applied[0]; u.OperationID != "op-1" {
		t.Fatalf("operation_id = %q, esperado o do pedido repassado", u.OperationID)
	}

	res, err = p.replica.Forward(Request{OperationID: "op-2", Kind: OpCreate, Spec: topic.Spec{Name: "sala", Kind: topic.KindEphemeral}})
	if err != nil || res.Status != pb.Status_STATUS_TOPIC_EXISTS {
		t.Fatalf("Forward repetido = %+v, %v", res, err)
	}
}

func TestInterestExchange(t *testing.T) {
	r1 := topic.Ref{ID: 1, Generation: 1}
	r2 := topic.Ref{ID: 2, Generation: 1}
	p := startPair(t, func(p *pair) { p.replicaStore.local = []topic.Ref{r1} })

	eventually(t, func() bool { return p.admin.Interest().RemoteInterest(r1).HasSubscribers }, "snapshot de interesse")
	if err := p.admin.Announce(r2, true); err != nil {
		t.Fatal(err)
	}
	if !p.replica.Interest().RemoteInterest(r2).HasSubscribers {
		t.Fatal("par não registrou o interesse anunciado")
	}
	if err := p.admin.Announce(r2, false); err != nil {
		t.Fatal(err)
	}
	if p.replica.Interest().RemoteInterest(r2).HasSubscribers {
		t.Fatal("par não registrou a retirada do interesse")
	}

	time.Sleep(150 * time.Millisecond)
	if in := p.admin.Interest().RemoteInterest(r1); !in.Fresh(time.Now(), 50*time.Millisecond) {
		t.Fatalf("heartbeat não renovou a visão: %+v", in)
	}
}

func TestLinkLossInvalidatesInterestAndReconnectRestoresIt(t *testing.T) {
	r1 := topic.Ref{ID: 1, Generation: 1}
	p := startPair(t, func(p *pair) { p.replicaStore.local = []topic.Ref{r1} })
	eventually(t, func() bool { return p.admin.Interest().RemoteInterest(r1).HasSubscribers }, "interesse inicial")

	p.proxy.Cut()
	waitDown(t, p.admin, p.replica)
	if in := p.admin.Interest().RemoteInterest(r1); in.HasSubscribers || !in.UpdatedAt.IsZero() {
		t.Fatalf("visão deveria ser descartada: %+v", in)
	}

	p.proxy.Restore()
	waitAlive(t, p.admin, p.replica)
	eventually(t, func() bool { return p.admin.Interest().RemoteInterest(r1).HasSubscribers }, "interesse reenviado")
}

func TestSilentPartitionDetected(t *testing.T) {
	p := startPair(t, nil)
	p.proxy.Blackhole(true)
	waitDown(t, p.admin, p.replica)
	p.proxy.Restore()
	waitAlive(t, p.admin, p.replica)
}

func TestRTTMeasured(t *testing.T) {
	p := startPair(t, nil)
	p.proxy.SetDelay(20 * time.Millisecond)
	eventually(t, func() bool {
		return p.admin.Link().RTT >= 40*time.Millisecond && p.replica.Link().RTT >= 40*time.Millisecond
	}, "RTT com 20 ms de atraso em cada sentido")
}

func rawHello(t *testing.T, addr string, hello *pb.PeerHello) *pb.PeerHelloReply {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	wc := wire.NewConn(nc, wire.Options{MaxFrame: wire.MaxPeerFrame, ReadTimeout: time.Second})
	defer wc.Close()
	if err := wc.Send(&pb.PeerEnvelope{Body: &pb.PeerEnvelope_Hello{Hello: hello}}); err != nil {
		t.Fatal(err)
	}
	env := &pb.PeerEnvelope{}
	if err := wc.Receive(env); err != nil {
		t.Fatal(err)
	}
	return env.GetHelloReply()
}

func TestDuplicateAndUnknownPeerRejected(t *testing.T) {
	p := startPair(t, nil)
	if st := rawHello(t, p.replicaAddr, &pb.PeerHello{BrokerId: "A"}).GetStatus(); st != pb.Status_STATUS_SESSION_CONFLICT {
		t.Fatalf("conexão duplicada: %v", st)
	}
	if st := rawHello(t, p.replicaAddr, &pb.PeerHello{BrokerId: "X"}).GetStatus(); st != pb.Status_STATUS_UNAUTHENTICATED {
		t.Fatalf("par desconhecido: %v", st)
	}
	if !p.admin.Link().Alive {
		t.Fatal("conexão recusada derrubou o enlace existente")
	}
}

func TestTokenMismatchRejected(t *testing.T) {
	store := newFakeStore()
	cfg := fastConfig("B", "A", false)
	cfg.Token = "segredo"
	replica, err := New(cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go replica.Serve(ln)
	defer replica.Close()

	if st := rawHello(t, ln.Addr().String(), &pb.PeerHello{BrokerId: "A", Token: "errado"}).GetStatus(); st != pb.Status_STATUS_UNAUTHENTICATED {
		t.Fatalf("token errado: %v", st)
	}
}

func TestAdminRetriesUntilPeerAppears(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	cfg := fastConfig("A", "B", true)
	cfg.DialAddr = addr
	admin, err := New(cfg, newFakeStore())
	if err != nil {
		t.Fatal(err)
	}
	admin.Start()
	defer admin.Close()
	time.Sleep(100 * time.Millisecond)

	replica, err := New(fastConfig("B", "A", false), newFakeStore())
	if err != nil {
		t.Fatal(err)
	}
	ln, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	go replica.Serve(ln)
	defer replica.Close()
	waitAlive(t, admin, replica)
}

func TestEmptyAdminAdoptsPeerCatalog(t *testing.T) {
	p := startPair(t, func(p *pair) {
		c := p.replicaStore.catalog
		c.Create(topic.Spec{Name: "sala-1/lances", Kind: topic.KindEphemeral})
		c.Create(topic.Spec{Name: "sala-1/placar", Kind: topic.KindState})
		c.Delete("sala-1/lances")
	})
	want := p.replicaStore.catalog.Snapshot()
	if got := p.adminStore.catalog.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("catálogo adotado = %+v, esperado %+v", got, want)
	}
	if !p.admin.Synced() || !p.replica.Synced() {
		t.Fatal("os dois lados deveriam estar sincronizados")
	}
	recreated, err := p.adminStore.catalog.PlanCreate(topic.Spec{Name: "sala-1/lances", Kind: topic.KindEphemeral})
	if err != nil || recreated.ID != 1 || recreated.Generation != 2 {
		t.Fatalf("recriação após adoção = %+v, %v", recreated, err)
	}
}

func TestAdoptionHappensOnlyOnce(t *testing.T) {
	p := startPair(t, nil)
	u := planCreate(t, p.adminStore, "sala")
	if err := p.admin.Replicate(u); err != nil {
		t.Fatal(err)
	}
	p.adminStore.catalog.Put(u.Topic, u.Version)
	p.replicaStore.catalog.Restore(topic.Snapshot{})

	p.proxy.Cut()
	waitDown(t, p.admin, p.replica)
	p.proxy.Restore()
	waitAlive(t, p.admin, p.replica)
	if _, ok := p.replicaStore.catalog.Lookup("sala"); !ok {
		t.Fatal("na reconexão o administrativo deveria enviar o próprio catálogo")
	}
}

func TestNotSyncedBeforeFirstSnapshot(t *testing.T) {
	l, err := New(fastConfig("B", "A", false), newFakeStore())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if l.Synced() {
		t.Fatal("broker recém criado não está sincronizado")
	}
}

func TestPeerUDPLearnedFromHandshake(t *testing.T) {
	p := startPair(t, nil)
	if got, want := p.admin.Link().PeerUDP, netip.MustParseAddrPort("127.0.0.1:17101"); got != want {
		t.Fatalf("UDP do par no administrativo = %v, esperado %v", got, want)
	}
	if got, want := p.replica.Link().PeerUDP, netip.MustParseAddrPort("127.0.0.1:17201"); got != want {
		t.Fatalf("UDP do par no outro = %v, esperado %v", got, want)
	}
}

func TestPeerUDPOverride(t *testing.T) {
	override := netip.MustParseAddrPort("10.0.0.9:9001")
	p := startPair(t, func(p *pair) { p.adminOverride = override })
	if got := p.admin.Link().PeerUDP; got != override {
		t.Fatalf("UDP do par = %v, esperado a sobrescrita %v", got, override)
	}
	p.proxy.Cut()
	waitDown(t, p.admin)
	if got := p.admin.Link().PeerUDP; got != override {
		t.Fatalf("endereço deveria permanecer após a queda: %v", got)
	}
}
