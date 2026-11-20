package broker

import (
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/contract"
	"github.com/franciscovmn/middlewarePubSub/internal/pb"
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

type brokerPair struct {
	a, b         *Broker
	addrA, addrB string
	peerAddrB    string
	proxy        *testproxy.Proxy
}

// startReplica sobe o broker B com o canal de clientes e o canal do par.
func startReplica(t *testing.T, peerLn net.Listener) (*Broker, string) {
	t.Helper()
	b, addr := startBroker(t, Config{ID: "B", EventPort: 7102, Peer: fastPeer("A", false, "")})
	go b.ServePeer(peerLn)
	return b, addr
}

func startPair(t *testing.T) *brokerPair {
	t.Helper()
	p := &brokerPair{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p.peerAddrB = ln.Addr().String()
	p.b, p.addrB = startReplica(t, ln)

	p.proxy, err = testproxy.New(p.peerAddrB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.proxy.Close)
	p.a, p.addrA = startBroker(t, Config{ID: "A", Peer: fastPeer("B", true, p.proxy.Addr())})
	waitLinked(t, p.a, p.b)
	return p
}

func waitLinked(t *testing.T, brokers ...*Broker) {
	t.Helper()
	eventually(t, func() bool {
		for _, b := range brokers {
			if !b.Contracts().Link.Link().Alive {
				return false
			}
		}
		return true
	}, "enlace entre brokers")
}

func waitUnlinked(t *testing.T, brokers ...*Broker) {
	t.Helper()
	eventually(t, func() bool {
		for _, b := range brokers {
			if b.Contracts().Link.Link().Alive {
				return false
			}
		}
		return true
	}, "enlace fora")
}

func status(env *pb.Envelope) pb.Status {
	switch body := env.Body.(type) {
	case *pb.Envelope_CreateTopicReply:
		return body.CreateTopicReply.GetStatus()
	case *pb.Envelope_DeleteTopicReply:
		return body.DeleteTopicReply.GetStatus()
	case *pb.Envelope_LookupTopicReply:
		return body.LookupTopicReply.GetStatus()
	case *pb.Envelope_SubscribeReply:
		return body.SubscribeReply.GetStatus()
	case *pb.Envelope_UnsubscribeReply:
		return body.UnsubscribeReply.GetStatus()
	}
	return pb.Status_STATUS_UNSPECIFIED
}

func expect(t *testing.T, env *pb.Envelope, want pb.Status) {
	t.Helper()
	if got := status(env); got != want {
		t.Fatalf("status = %v, esperado %v (%v)", got, want, env)
	}
}

func TestCreateOnAdminIsVisibleOnPeer(t *testing.T) {
	p := startPair(t)
	ca := login(t, p.addrA, "ana", 1)
	cb := login(t, p.addrB, "bia", 2)

	created := ca.call(createTopic("sala-1/lances", ephemeral)).GetCreateTopicReply().GetTopic()
	found := cb.call(lookupTopic("sala-1/lances")).GetLookupTopicReply()
	if found.GetStatus() != pb.Status_STATUS_OK || found.GetTopic().GetId() != created.GetId() ||
		found.GetTopic().GetGeneration() != created.GetGeneration() {
		t.Fatalf("no B: %v; criado no A: %v", found, created)
	}
}

func TestCreateOnPeerIsForwardedToAdmin(t *testing.T) {
	p := startPair(t)
	ca := login(t, p.addrA, "ana", 1)
	cb := login(t, p.addrB, "bia", 2)

	reply := cb.call(createTopic("sala-1/placar", pb.TopicKind_TOPIC_KIND_STATE)).GetCreateTopicReply()
	if reply.GetStatus() != pb.Status_STATUS_OK || reply.GetTopic().GetId() != 1 {
		t.Fatalf("CreateTopic no B = %v", reply)
	}
	expect(t, cb.call(lookupTopic("sala-1/placar")), pb.Status_STATUS_OK)
	expect(t, ca.call(lookupTopic("sala-1/placar")), pb.Status_STATUS_OK)
	expect(t, cb.call(createTopic("sala-1/placar", ephemeral)), pb.Status_STATUS_TOPIC_EXISTS)
	expect(t, cb.call(createTopic("Sala", ephemeral)), pb.Status_STATUS_INVALID_ARGUMENT)

	expect(t, cb.call(deleteTopic("sala-1/placar")), pb.Status_STATUS_OK)
	expect(t, ca.call(lookupTopic("sala-1/placar")), pb.Status_STATUS_TOPIC_NOT_FOUND)
	expect(t, cb.call(lookupTopic("sala-1/placar")), pb.Status_STATUS_TOPIC_NOT_FOUND)
}

func TestRestrictedAdminCheckedBeforeForwarding(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	b, addrB := startBroker(t, Config{ID: "B", EventPort: 7102, RestrictTopicAdmin: true, Peer: fastPeer("A", false, "")})
	go b.ServePeer(ln)
	a, _ := startBroker(t, Config{ID: "A", Peer: fastPeer("B", true, ln.Addr().String())})
	waitLinked(t, a, b)

	expect(t, login(t, addrB, "ana", 1).call(createTopic("sala", ephemeral)), pb.Status_STATUS_FORBIDDEN)
	expect(t, login(t, addrB, "admin", 2).call(createTopic("sala", ephemeral)), pb.Status_STATUS_OK)
}

func TestCatalogChangesUnavailableWithoutPeer(t *testing.T) {
	p := startPair(t)
	ca := login(t, p.addrA, "ana", 1)
	cb := login(t, p.addrB, "bia", 2)
	ca.call(createTopic("existente", ephemeral))

	p.proxy.Cut()
	waitUnlinked(t, p.a, p.b)

	expect(t, ca.call(createTopic("sala", ephemeral)), pb.Status_STATUS_UNAVAILABLE)
	expect(t, ca.call(lookupTopic("sala")), pb.Status_STATUS_TOPIC_NOT_FOUND)
	expect(t, ca.call(deleteTopic("existente")), pb.Status_STATUS_UNAVAILABLE)
	expect(t, cb.call(createTopic("sala", ephemeral)), pb.Status_STATUS_UNAVAILABLE)

	expect(t, ca.call(lookupTopic("existente")), pb.Status_STATUS_OK)
	expect(t, cb.call(lookupTopic("existente")), pb.Status_STATUS_OK)
}

func TestCreateWithPeerProcessDown(t *testing.T) {
	p := startPair(t)
	ca := login(t, p.addrA, "ana", 1)
	p.b.Close()
	waitUnlinked(t, p.a)
	expect(t, ca.call(createTopic("sala", ephemeral)), pb.Status_STATUS_UNAVAILABLE)
	expect(t, ca.call(lookupTopic("sala")), pb.Status_STATUS_TOPIC_NOT_FOUND)
}

func TestDeleteEndsSubscriptionsOnBothBrokers(t *testing.T) {
	p := startPair(t)
	admin := login(t, p.addrA, "admin", 1)
	subA := login(t, p.addrA, "ana", 2)
	subB := login(t, p.addrB, "bia", 3)
	admin.call(createTopic("sala", ephemeral))
	expect(t, subA.call(subscribe("sala", 0)), pb.Status_STATUS_OK)
	expect(t, subB.call(subscribe("sala", 0)), pb.Status_STATUS_OK)

	expect(t, admin.call(deleteTopic("sala")), pb.Status_STATUS_OK)

	for name, c := range map[string]*rawClient{"A": subA, "B": subB} {
		ended := c.nextPush().GetSubscriptionEnded()
		if ended.GetTopic().GetName() != "sala" || ended.GetReason() != pb.Status_STATUS_TOPIC_NOT_FOUND {
			t.Fatalf("aviso no %s = %v", name, ended)
		}
	}
	ref := topic.Ref{ID: 1, Generation: 1}
	if p.a.Contracts().Subscriptions.HasLocal(ref) || p.b.Contracts().Subscriptions.HasLocal(ref) {
		t.Fatal("inscrições do tópico apagado continuam registradas")
	}
}

func TestSubscribeWaitsForPeerAck(t *testing.T) {
	p := startPair(t)
	ca := login(t, p.addrA, "ana", 1)
	cb := login(t, p.addrB, "bia", 2)
	ref := topic.Ref{ID: 1, Generation: 1}
	ca.call(createTopic("sala", ephemeral))

	expect(t, cb.call(subscribe("sala", 0)), pb.Status_STATUS_OK)
	in := p.a.Contracts().Interest.RemoteInterest(ref)
	if !in.HasSubscribers || !in.Fresh(time.Now(), contract.InterestMaxAge) {
		t.Fatalf("A deveria saber do assinante do B ao confirmar a inscrição: %+v", in)
	}

	expect(t, cb.call(unsubscribe("sala")), pb.Status_STATUS_OK)
	if p.a.Contracts().Interest.RemoteInterest(ref).HasSubscribers {
		t.Fatal("A deveria saber da retirada ao confirmar o Unsubscribe")
	}
}

func TestSubscribeUnavailableWhileLinkDownKeepsActiveOnes(t *testing.T) {
	p := startPair(t)
	ca := login(t, p.addrA, "ana", 1)
	cb := login(t, p.addrB, "bia", 2)
	other := login(t, p.addrB, "admin", 3)
	ca.call(createTopic("sala", ephemeral))
	ca.call(createTopic("outra", ephemeral))
	expect(t, cb.call(subscribe("sala", 0)), pb.Status_STATUS_OK)

	p.proxy.Cut()
	waitUnlinked(t, p.a, p.b)

	expect(t, other.call(subscribe("outra", 0)), pb.Status_STATUS_UNAVAILABLE)
	if p.b.Contracts().Subscriptions.HasLocal(topic.Ref{ID: 2, Generation: 1}) {
		t.Fatal("inscrição recusada ficou registrada")
	}
	if !p.b.Contracts().Subscriptions.HasLocal(topic.Ref{ID: 1, Generation: 1}) {
		t.Fatal("inscrição ativa deveria continuar local")
	}
	if in := p.a.Contracts().Interest.RemoteInterest(topic.Ref{ID: 1, Generation: 1}); in.Fresh(time.Now(), contract.InterestMaxAge) {
		t.Fatalf("visão do par deveria estar descartada: %+v", in)
	}
}

func TestInterestResentAfterReconnect(t *testing.T) {
	p := startPair(t)
	ca := login(t, p.addrA, "ana", 1)
	cb := login(t, p.addrB, "bia", 2)
	ref := topic.Ref{ID: 1, Generation: 1}
	ca.call(createTopic("sala", ephemeral))
	expect(t, cb.call(subscribe("sala", 0)), pb.Status_STATUS_OK)

	p.proxy.Cut()
	waitUnlinked(t, p.a, p.b)
	p.proxy.Restore()
	waitLinked(t, p.a, p.b)

	eventually(t, func() bool { return p.a.Contracts().Interest.RemoteInterest(ref).HasSubscribers }, "interesse reenviado")
	time.Sleep(100 * time.Millisecond)
	if in := p.a.Contracts().Interest.RemoteInterest(ref); !in.Fresh(time.Now(), 50*time.Millisecond) {
		t.Fatalf("heartbeat não mantém a visão recente: %+v", in)
	}
}

func TestDisconnectWithdrawsInterest(t *testing.T) {
	p := startPair(t)
	ca := login(t, p.addrA, "ana", 1)
	cb := login(t, p.addrB, "bia", 2)
	ref := topic.Ref{ID: 1, Generation: 1}
	ca.call(createTopic("sala", ephemeral))
	expect(t, cb.call(subscribe("sala", 0)), pb.Status_STATUS_OK)

	cb.wc.Close()
	eventually(t, func() bool { return !p.a.Contracts().Interest.RemoteInterest(ref).HasSubscribers }, "retirada do interesse")
}

func TestReconnectSnapshotRemovesDivergentTopic(t *testing.T) {
	p := startPair(t)
	cb := login(t, p.addrB, "bia", 2)

	divergent := topic.Info{Ref: topic.Ref{ID: 50, Generation: 1}, Name: "fantasma", Kind: topic.KindEphemeral, Policy: topic.DefaultPolicy(topic.KindEphemeral)}
	if err := p.b.catalog.Put(divergent, p.b.catalog.Version()); err != nil {
		t.Fatal(err)
	}
	expect(t, cb.call(subscribe("fantasma", 0)), pb.Status_STATUS_OK)

	p.proxy.Cut()
	waitUnlinked(t, p.a, p.b)
	p.proxy.Restore()
	waitLinked(t, p.a, p.b)

	ended := cb.nextPush().GetSubscriptionEnded()
	if ended.GetTopic().GetName() != "fantasma" {
		t.Fatalf("aviso = %v", ended)
	}
	expect(t, cb.call(lookupTopic("fantasma")), pb.Status_STATUS_TOPIC_NOT_FOUND)
}

func TestRestartedPeerReceivesSnapshotWithTombstones(t *testing.T) {
	p := startPair(t)
	ca := login(t, p.addrA, "ana", 1)
	ca.call(createTopic("sala-1/lances", ephemeral))
	ca.call(createTopic("sala-1/placar", pb.TopicKind_TOPIC_KIND_STATE))
	ca.call(deleteTopic("sala-1/lances"))

	p.b.Close()
	waitUnlinked(t, p.a)
	ln, err := net.Listen("tcp", p.peerAddrB)
	if err != nil {
		t.Fatal(err)
	}
	fresh, addr := startReplica(t, ln)
	waitLinked(t, p.a, fresh)

	if !reflect.DeepEqual(fresh.catalog.Snapshot(), p.a.catalog.Snapshot()) {
		t.Fatalf("catálogo do B reiniciado = %+v", fresh.catalog.Snapshot())
	}
	recreated := login(t, addr, "bia", 2).call(createTopic("sala-1/lances", ephemeral)).GetCreateTopicReply().GetTopic()
	if recreated.GetId() != 1 || recreated.GetGeneration() != 2 {
		t.Fatalf("recriado pelo B reiniciado = %v", recreated)
	}
}

func TestRTTMeasuredOnBothSides(t *testing.T) {
	p := startPair(t)
	p.proxy.SetDelay(15 * time.Millisecond)
	eventually(t, func() bool {
		return p.a.Contracts().Link.Link().RTT >= 30*time.Millisecond &&
			p.b.Contracts().Link.Link().RTT >= 30*time.Millisecond
	}, "RTT com 15 ms de atraso em cada sentido")
}

func TestSilentPartitionIsDetectedAndHealed(t *testing.T) {
	p := startPair(t)
	ca := login(t, p.addrA, "ana", 1)
	p.proxy.Blackhole(true)
	waitUnlinked(t, p.a, p.b)
	expect(t, ca.call(createTopic("sala", ephemeral)), pb.Status_STATUS_UNAVAILABLE)

	p.proxy.Restore()
	waitLinked(t, p.a, p.b)
	expect(t, ca.call(createTopic("sala", ephemeral)), pb.Status_STATUS_OK)
}

func restore(name string, gen uint32) *pb.Envelope {
	return &pb.Envelope{Body: &pb.Envelope_Subscribe{Subscribe: &pb.Subscribe{Name: name, Generation: gen, Restore: true}}}
}

func TestRestartedAdminAdoptsPeerCatalog(t *testing.T) {
	p := startPair(t)
	ca := login(t, p.addrA, "ana", 1)
	cb := login(t, p.addrB, "bia", 2)
	ca.call(createTopic("sala-1/lances", ephemeral))
	ca.call(createTopic("sala-1/placar", pb.TopicKind_TOPIC_KIND_STATE))
	ca.call(createTopic("sala-1/reacoes", ephemeral))
	ca.call(deleteTopic("sala-1/reacoes"))
	expect(t, cb.call(subscribe("sala-1/placar", 0)), pb.Status_STATUS_OK)
	before := p.b.catalog.Snapshot()

	p.a.Close()
	waitUnlinked(t, p.b)
	a2, addrA2 := startBroker(t, Config{ID: "A", Peer: fastPeer("B", true, p.proxy.Addr())})
	waitLinked(t, a2, p.b)

	if got := a2.catalog.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("catálogo do A reiniciado = %+v, esperado %+v", got, before)
	}
	if got := p.b.catalog.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("catálogo do B mudou: %+v", got)
	}
	placar := topic.Ref{ID: 2, Generation: 1}
	if !p.b.Contracts().Subscriptions.HasLocal(placar) {
		t.Fatal("inscrição do B não sobreviveu ao reinício do A")
	}
	expect(t, cb.call(lookupTopic("sala-1/placar")), pb.Status_STATUS_OK)
	if len(cb.pushes) != 0 {
		t.Fatalf("assinante do B recebeu avisos: %v", cb.pushes)
	}
	eventually(t, func() bool { return a2.Contracts().Interest.RemoteInterest(placar).HasSubscribers }, "interesse do B no A reiniciado")

	recreated := login(t, addrA2, "ana", 3).call(createTopic("sala-1/reacoes", ephemeral)).GetCreateTopicReply().GetTopic()
	if recreated.GetId() != 3 || recreated.GetGeneration() != 2 {
		t.Fatalf("recriado no A reiniciado = %v", recreated)
	}
	fresh := login(t, addrA2, "ana", 4).call(createTopic("sala-2/lances", ephemeral)).GetCreateTopicReply().GetTopic()
	if fresh.GetId() != 4 || fresh.GetGeneration() != 1 {
		t.Fatalf("tópico novo no A reiniciado = %v", fresh)
	}
	expect(t, cb.call(lookupTopic("sala-2/lances")), pb.Status_STATUS_OK)
}

func TestUnsyncedBrokerAnswersUnavailable(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	a, addrA := startBroker(t, Config{ID: "A", Peer: fastPeer("B", true, "127.0.0.1:1")})
	b, addrB := startReplica(t, ln)
	for _, addr := range []string{addrA, addrB} {
		c := login(t, addr, "ana", 1)
		expect(t, c.call(lookupTopic("sala")), pb.Status_STATUS_UNAVAILABLE)
		expect(t, c.call(restore("sala", 1)), pb.Status_STATUS_UNAVAILABLE)
		expect(t, c.call(createTopic("sala", ephemeral)), pb.Status_STATUS_UNAVAILABLE)
	}
	if a.catalogReady() || b.catalogReady() {
		t.Fatal("brokers sem sincronização não deveriam responder pelo catálogo")
	}
}

func TestRestoreAcceptedWhileLinkDown(t *testing.T) {
	p := startPair(t)
	ca := login(t, p.addrA, "ana", 1)
	ca.call(createTopic("sala", ephemeral))
	ca.call(createTopic("outra", ephemeral))
	ca.call(deleteTopic("outra"))
	ca.call(createTopic("outra", ephemeral))

	p.proxy.Cut()
	waitUnlinked(t, p.a, p.b)
	cb := login(t, p.addrB, "bia", 2)
	expect(t, cb.call(subscribe("sala", 0)), pb.Status_STATUS_UNAVAILABLE)
	expect(t, cb.call(restore("sala", 1)), pb.Status_STATUS_OK)
	expect(t, cb.call(restore("outra", 1)), pb.Status_STATUS_TOPIC_NOT_FOUND)

	ref := topic.Ref{ID: 1, Generation: 1}
	p.proxy.Restore()
	waitLinked(t, p.a, p.b)
	eventually(t, func() bool { return p.a.Contracts().Interest.RemoteInterest(ref).HasSubscribers }, "interesse restaurado chega ao par")
}

func TestContractsExposePeerUDP(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	b, _ := startBroker(t, Config{ID: "B", EventPort: 17301, Peer: fastPeer("A", false, "")})
	go b.ServePeer(ln)
	a, _ := startBroker(t, Config{ID: "A", EventPort: 17401, Peer: fastPeer("B", true, ln.Addr().String())})
	waitLinked(t, a, b)
	if got := a.Contracts().Link.Link().PeerUDP.String(); got != "127.0.0.1:17301" {
		t.Fatalf("UDP do B visto pelo A = %s", got)
	}
	if got := b.Contracts().Link.Link().PeerUDP.String(); got != "127.0.0.1:17401" {
		t.Fatalf("UDP do A visto pelo B = %s", got)
	}
}
