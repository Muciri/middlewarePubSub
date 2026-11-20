package broker

import (
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/contract"
	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/internal/wire"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

var testCreds = map[string]Credential{
	"admin": {Token: "ta", Admin: true},
	"ana":   {Token: "t1"},
	"bia":   {Token: "t2"},
}

var tokens = map[string]string{"admin": "ta", "ana": "t1", "bia": "t2"}

func startBroker(t *testing.T, cfg Config) (*Broker, string) {
	t.Helper()
	if cfg.ID == "" {
		cfg.ID = "A"
	}
	if cfg.Credentials == nil {
		cfg.Credentials = testCreds
	}
	if cfg.EventPort == 0 {
		cfg.EventPort = freeUDPPort(t)
	}
	b, err := New(cfg)
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

func freeUDPPort(t *testing.T) uint16 {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero})
	if err != nil {
		t.Fatal(err)
	}
	p := uint16(c.LocalAddr().(*net.UDPAddr).Port)
	c.Close()
	return p
}

type rawClient struct {
	t      *testing.T
	wc     *wire.Conn
	nextID uint64
	pushes []*pb.Envelope
}

func dialRaw(t *testing.T, addr string) *rawClient {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	r := &rawClient{t: t, wc: wire.NewConn(nc, wire.Options{ReadTimeout: 2 * time.Second})}
	t.Cleanup(func() { r.wc.Close() })
	return r
}

func (r *rawClient) hello(identity, token string, session uint64, udpPort uint32) *pb.HelloReply {
	r.t.Helper()
	reply := r.call(&pb.Envelope{Body: &pb.Envelope_Hello{Hello: &pb.Hello{
		Identity: identity, Token: token, SessionId: session, UdpPort: udpPort,
	}}})
	return reply.GetHelloReply()
}

func login(t *testing.T, addr, identity string, session uint64) *rawClient {
	t.Helper()
	r := dialRaw(t, addr)
	if st := r.hello(identity, tokens[identity], session, 40000).GetStatus(); st != pb.Status_STATUS_OK {
		t.Fatalf("Hello de %s: %v", identity, st)
	}
	return r
}

// call envia o pedido e lê até a resposta de mesmo request_id, guardando as
// mensagens espontâneas que chegarem antes.
func (r *rawClient) call(env *pb.Envelope) *pb.Envelope {
	r.t.Helper()
	r.nextID++
	env.RequestId = r.nextID
	if err := r.wc.Send(env); err != nil {
		r.t.Fatalf("Send: %v", err)
	}
	for {
		got := &pb.Envelope{}
		if err := r.wc.Receive(got); err != nil {
			r.t.Fatalf("Receive: %v", err)
		}
		if got.RequestId == env.RequestId {
			return got
		}
		if got.GetHeartbeat() == nil {
			r.pushes = append(r.pushes, got)
		}
	}
}

// nextPush devolve a próxima mensagem espontânea que não seja heartbeat.
func (r *rawClient) nextPush() *pb.Envelope {
	r.t.Helper()
	if len(r.pushes) > 0 {
		p := r.pushes[0]
		r.pushes = r.pushes[1:]
		return p
	}
	for {
		got := &pb.Envelope{}
		if err := r.wc.Receive(got); err != nil {
			r.t.Fatalf("Receive: %v", err)
		}
		if got.GetHeartbeat() == nil {
			return got
		}
	}
}

// expectClosed lê até o broker encerrar a conexão, ignorando heartbeats.
func (r *rawClient) expectClosed() {
	r.t.Helper()
	for {
		err := r.wc.Receive(&pb.Envelope{})
		if err == nil {
			continue
		}
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			r.t.Fatal("conexão continuou aberta")
		}
		return
	}
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

func createTopic(name string, kind pb.TopicKind) *pb.Envelope {
	return &pb.Envelope{Body: &pb.Envelope_CreateTopic{CreateTopic: &pb.CreateTopic{Name: name, Kind: kind}}}
}

func deleteTopic(name string) *pb.Envelope {
	return &pb.Envelope{Body: &pb.Envelope_DeleteTopic{DeleteTopic: &pb.DeleteTopic{Name: name}}}
}

func lookupTopic(name string) *pb.Envelope {
	return &pb.Envelope{Body: &pb.Envelope_LookupTopic{LookupTopic: &pb.LookupTopic{Name: name}}}
}

func subscribe(name string, gen uint32) *pb.Envelope {
	return &pb.Envelope{Body: &pb.Envelope_Subscribe{Subscribe: &pb.Subscribe{Name: name, Generation: gen}}}
}

func unsubscribe(name string) *pb.Envelope {
	return &pb.Envelope{Body: &pb.Envelope_Unsubscribe{Unsubscribe: &pb.Unsubscribe{Name: name}}}
}

func getStats() *pb.Envelope {
	return &pb.Envelope{Body: &pb.Envelope_GetStats{GetStats: &pb.GetStats{}}}
}

const ephemeral = pb.TopicKind_TOPIC_KIND_EPHEMERAL

func TestHelloSuccess(t *testing.T) {
	b, addr := startBroker(t, Config{ID: "B", EventPort: 7101, HeartbeatInterval: 250 * time.Millisecond})
	r := dialRaw(t, addr)
	reply := r.hello("ana", "t1", 99, 40123)

	if reply.GetStatus() != pb.Status_STATUS_OK || reply.GetBrokerId() != "B" ||
		reply.GetUdpPort() != 7101 || reply.GetHeartbeatIntervalMs() != 250 {
		t.Fatalf("HelloReply = %v", reply)
	}
	s, ok := b.Contracts().Sessions.Get(99)
	want := contract.Session{ID: 99, Identity: "ana", UDPAddr: netip.MustParseAddrPort("127.0.0.1:40123")}
	if !ok || s != want {
		t.Fatalf("sessão = %+v, %v; esperado %+v", s, ok, want)
	}
}

func TestHelloAuthFailureClosesConnection(t *testing.T) {
	_, addr := startBroker(t, Config{})
	cases := map[string][2]string{
		"token errado":           {"ana", "t2"},
		"identidade inexistente": {"zé", "t1"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			r := dialRaw(t, addr)
			if st := r.hello(c[0], c[1], 1, 0).GetStatus(); st != pb.Status_STATUS_UNAUTHENTICATED {
				t.Fatalf("status = %v", st)
			}
			r.expectClosed()
		})
	}
}

func TestHelloInvalidArguments(t *testing.T) {
	_, addr := startBroker(t, Config{})
	for name, c := range map[string]struct {
		session uint64
		port    uint32
	}{"sessão zero": {0, 1}, "porta inválida": {1, 70000}} {
		t.Run(name, func(t *testing.T) {
			r := dialRaw(t, addr)
			if st := r.hello("ana", "t1", c.session, c.port).GetStatus(); st != pb.Status_STATUS_INVALID_ARGUMENT {
				t.Fatalf("status = %v", st)
			}
			r.expectClosed()
		})
	}
}

func TestFirstMessageMustBeHello(t *testing.T) {
	_, addr := startBroker(t, Config{})
	r := dialRaw(t, addr)
	if err := r.wc.Send(lookupTopic("sala")); err != nil {
		t.Fatal(err)
	}
	if err := r.wc.Receive(&pb.Envelope{}); err == nil {
		t.Fatal("broker respondeu a comando antes do Hello")
	}
}

func TestProtocolViolationClosesConnection(t *testing.T) {
	_, addr := startBroker(t, Config{})
	var session uint64
	for name, env := range map[string]*pb.Envelope{
		"segundo Hello":  {Body: &pb.Envelope_Hello{Hello: &pb.Hello{Identity: "ana", Token: "t1", SessionId: 1}}},
		"resposta":       {Body: &pb.Envelope_LookupTopicReply{LookupTopicReply: &pb.LookupTopicReply{}}},
		"envelope vazio": {},
	} {
		session++
		t.Run(name, func(t *testing.T) {
			r := login(t, addr, "ana", session)
			if err := r.wc.Send(env); err != nil {
				t.Fatal(err)
			}
			r.expectClosed()
		})
	}
}

func TestSessionConflictWithOtherIdentity(t *testing.T) {
	b, addr := startBroker(t, Config{})
	first := login(t, addr, "ana", 7)

	r := dialRaw(t, addr)
	if st := r.hello("bia", "t2", 7, 1).GetStatus(); st != pb.Status_STATUS_SESSION_CONFLICT {
		t.Fatalf("status = %v", st)
	}
	r.expectClosed()

	if s, _ := b.Contracts().Sessions.Get(7); s.Identity != "ana" {
		t.Fatalf("vínculo alterado: %+v", s)
	}
	if st := first.call(lookupTopic("x")).GetLookupTopicReply().GetStatus(); st != pb.Status_STATUS_TOPIC_NOT_FOUND {
		t.Fatalf("conexão original deveria continuar ativa, status %v", st)
	}
}

func TestRebindSameIdentityReplacesOldConnection(t *testing.T) {
	b, addr := startBroker(t, Config{})
	admin := login(t, addr, "admin", 1)
	admin.call(createTopic("sala", ephemeral))

	old := login(t, addr, "ana", 7)
	old.call(subscribe("sala", 0))

	fresh := dialRaw(t, addr)
	if st := fresh.hello("ana", "t1", 7, 40999).GetStatus(); st != pb.Status_STATUS_OK {
		t.Fatalf("status = %v", st)
	}
	old.expectClosed()

	if st := fresh.call(subscribe("sala", 0)).GetSubscribeReply().GetStatus(); st != pb.Status_STATUS_OK {
		t.Fatalf("Subscribe = %v", st)
	}
	info, _ := b.catalog.Lookup("sala")
	subs := b.Contracts().Subscriptions.ForTopic(info.Ref)
	if len(subs) != 1 || subs[0].UDPAddr.Port() != 40999 {
		t.Fatalf("inscrições = %+v", subs)
	}
}

func TestTopicCommands(t *testing.T) {
	_, addr := startBroker(t, Config{})
	r := login(t, addr, "ana", 1)

	created := r.call(createTopic("sala-1/lances", ephemeral)).GetCreateTopicReply()
	if created.GetStatus() != pb.Status_STATUS_OK || created.GetTopic().GetId() != 1 ||
		created.GetTopic().GetGeneration() != 1 || created.GetTopic().GetMaxTtlMs() != 500 {
		t.Fatalf("CreateTopic = %v", created)
	}
	if st := r.call(createTopic("sala-1/lances", ephemeral)).GetCreateTopicReply().GetStatus(); st != pb.Status_STATUS_TOPIC_EXISTS {
		t.Fatalf("duplicado: %v", st)
	}
	if st := r.call(createTopic("Sala", ephemeral)).GetCreateTopicReply().GetStatus(); st != pb.Status_STATUS_INVALID_ARGUMENT {
		t.Fatalf("nome inválido: %v", st)
	}
	if st := r.call(createTopic("sala", pb.TopicKind_TOPIC_KIND_UNSPECIFIED)).GetCreateTopicReply().GetStatus(); st != pb.Status_STATUS_INVALID_ARGUMENT {
		t.Fatalf("sem tipo: %v", st)
	}

	found := r.call(lookupTopic("sala-1/lances")).GetLookupTopicReply()
	if found.GetStatus() != pb.Status_STATUS_OK || found.GetTopic().GetName() != "sala-1/lances" {
		t.Fatalf("LookupTopic = %v", found)
	}

	if st := r.call(deleteTopic("sala-1/lances")).GetDeleteTopicReply().GetStatus(); st != pb.Status_STATUS_OK {
		t.Fatalf("DeleteTopic = %v", st)
	}
	if st := r.call(deleteTopic("sala-1/lances")).GetDeleteTopicReply().GetStatus(); st != pb.Status_STATUS_TOPIC_NOT_FOUND {
		t.Fatalf("segunda exclusão = %v", st)
	}
	if st := r.call(lookupTopic("sala-1/lances")).GetLookupTopicReply().GetStatus(); st != pb.Status_STATUS_TOPIC_NOT_FOUND {
		t.Fatalf("Lookup após exclusão = %v", st)
	}

	again := r.call(createTopic("sala-1/lances", ephemeral)).GetCreateTopicReply().GetTopic()
	if again.GetId() != 1 || again.GetGeneration() != 2 {
		t.Fatalf("recriado = %v", again)
	}
}

func TestRestrictTopicAdmin(t *testing.T) {
	_, addr := startBroker(t, Config{RestrictTopicAdmin: true})
	user := login(t, addr, "ana", 1)
	admin := login(t, addr, "admin", 2)

	if st := user.call(createTopic("sala", ephemeral)).GetCreateTopicReply().GetStatus(); st != pb.Status_STATUS_FORBIDDEN {
		t.Fatalf("Create por não admin = %v", st)
	}
	if st := admin.call(createTopic("sala", ephemeral)).GetCreateTopicReply().GetStatus(); st != pb.Status_STATUS_OK {
		t.Fatalf("Create por admin = %v", st)
	}
	if st := user.call(deleteTopic("sala")).GetDeleteTopicReply().GetStatus(); st != pb.Status_STATUS_FORBIDDEN {
		t.Fatalf("Delete por não admin = %v", st)
	}
	if st := user.call(lookupTopic("sala")).GetLookupTopicReply().GetStatus(); st != pb.Status_STATUS_OK {
		t.Fatalf("Lookup por não admin = %v", st)
	}
}

func TestSubscribeAndUnsubscribe(t *testing.T) {
	b, addr := startBroker(t, Config{})
	r := login(t, addr, "ana", 5)
	r.call(createTopic("sala", ephemeral))

	reply := r.call(subscribe("sala", 0)).GetSubscribeReply()
	if reply.GetStatus() != pb.Status_STATUS_OK || reply.GetTopic().GetGeneration() != 1 {
		t.Fatalf("Subscribe = %v", reply)
	}
	ref := topic.Ref{ID: 1, Generation: 1}
	subs := b.Contracts().Subscriptions.ForTopic(ref)
	if len(subs) != 1 || subs[0].Session != 5 || subs[0].Identity != "ana" ||
		subs[0].UDPAddr != netip.MustParseAddrPort("127.0.0.1:40000") {
		t.Fatalf("inscrições = %+v", subs)
	}
	done := subs[0].Done()

	if st := r.call(subscribe("sala", 0)).GetSubscribeReply().GetStatus(); st != pb.Status_STATUS_OK {
		t.Fatalf("Subscribe repetido = %v", st)
	}
	if st := r.call(unsubscribe("sala")).GetUnsubscribeReply().GetStatus(); st != pb.Status_STATUS_OK {
		t.Fatalf("Unsubscribe = %v", st)
	}
	if b.Contracts().Subscriptions.HasLocal(ref) {
		t.Fatal("inscrição continua registrada")
	}
	select {
	case <-done:
	default:
		t.Fatal("Done não foi fechado no Unsubscribe")
	}
	if st := r.call(unsubscribe("sala")).GetUnsubscribeReply().GetStatus(); st != pb.Status_STATUS_OK {
		t.Fatalf("Unsubscribe repetido = %v", st)
	}
	if st := r.call(unsubscribe("nada")).GetUnsubscribeReply().GetStatus(); st != pb.Status_STATUS_TOPIC_NOT_FOUND {
		t.Fatalf("Unsubscribe de tópico inexistente = %v", st)
	}
}

func TestSubscribeRejectsUnknownTopicAndOldGeneration(t *testing.T) {
	_, addr := startBroker(t, Config{})
	r := login(t, addr, "ana", 1)
	if st := r.call(subscribe("nada", 0)).GetSubscribeReply().GetStatus(); st != pb.Status_STATUS_TOPIC_NOT_FOUND {
		t.Fatalf("tópico inexistente = %v", st)
	}
	r.call(createTopic("sala", ephemeral))
	r.call(deleteTopic("sala"))
	r.call(createTopic("sala", ephemeral))

	if st := r.call(subscribe("sala", 1)).GetSubscribeReply().GetStatus(); st != pb.Status_STATUS_TOPIC_NOT_FOUND {
		t.Fatalf("geração antiga = %v", st)
	}
	if st := r.call(subscribe("sala", 2)).GetSubscribeReply().GetStatus(); st != pb.Status_STATUS_OK {
		t.Fatalf("geração atual = %v", st)
	}
}

func TestDeleteTopicNotifiesSubscribers(t *testing.T) {
	b, addr := startBroker(t, Config{})
	admin := login(t, addr, "admin", 1)
	sub := login(t, addr, "ana", 2)
	admin.call(createTopic("sala", ephemeral))
	sub.call(subscribe("sala", 0))

	admin.call(deleteTopic("sala"))

	ended := sub.nextPush().GetSubscriptionEnded()
	if ended == nil || ended.GetTopic().GetName() != "sala" || ended.GetReason() != pb.Status_STATUS_TOPIC_NOT_FOUND {
		t.Fatalf("aviso = %v", ended)
	}
	if b.Contracts().Subscriptions.HasLocal(topic.Ref{ID: 1, Generation: 1}) {
		t.Fatal("inscrição do tópico apagado continua registrada")
	}
}

func TestDisconnectCleansUpSessionAndSubscriptions(t *testing.T) {
	b, addr := startBroker(t, Config{})
	r := login(t, addr, "ana", 3)
	r.call(createTopic("sala", ephemeral))
	r.call(subscribe("sala", 0))
	ref := topic.Ref{ID: 1, Generation: 1}
	done := b.Contracts().Subscriptions.ForTopic(ref)[0].Done()

	r.wc.Close()

	eventually(t, func() bool {
		_, bound := b.Contracts().Sessions.Get(3)
		return !bound && !b.Contracts().Subscriptions.HasLocal(ref)
	}, "limpeza da sessão")
	select {
	case <-done:
	default:
		t.Fatal("Done não foi fechado na queda da conexão")
	}
}

func TestReportDeliversPublishResult(t *testing.T) {
	b, addr := startBroker(t, Config{})
	r := login(t, addr, "ana", 4)

	b.Contracts().Results.Report(contract.AdmissionResult{Session: 999, Sequence: 1, Status: contract.AdmissionAccepted})
	b.Contracts().Results.Report(contract.AdmissionResult{
		Session: 4, Topic: topic.Ref{ID: 2, Generation: 3}, Sequence: 17, Status: contract.AdmissionNoSubscribers,
	})

	res := r.nextPush().GetPublishResult()
	if res.GetTopicId() != 2 || res.GetGeneration() != 3 || res.GetSequence() != 17 ||
		res.GetStatus() != pb.AdmissionStatus_ADMISSION_NO_SUBSCRIBERS {
		t.Fatalf("PublishResult = %v", res)
	}
}

type fixedStats struct{}

func (fixedStats) Stats() contract.StatsSnapshot {
	return contract.StatsSnapshot{Counters: map[string]uint64{"expired": 3}, Gauges: map[string]int64{"queue": 7}}
}

func TestGetStats(t *testing.T) {
	_, addr := startBroker(t, Config{Stats: fixedStats{}})
	admin := login(t, addr, "admin", 1)
	user := login(t, addr, "ana", 2)

	reply := admin.call(getStats()).GetGetStatsReply()
	if reply.GetStatus() != pb.Status_STATUS_OK || reply.GetBrokerId() != "A" ||
		reply.GetCounters()["expired"] != 3 || reply.GetGauges()["queue"] != 7 {
		t.Fatalf("GetStats = %v", reply)
	}
	if st := user.call(getStats()).GetGetStatsReply().GetStatus(); st != pb.Status_STATUS_FORBIDDEN {
		t.Fatalf("GetStats por não admin = %v", st)
	}
}

type denyAll struct{}

func (denyAll) AllowCommand(string) bool { return false }

func TestCommandLimiter(t *testing.T) {
	_, addr := startBroker(t, Config{Limiter: denyAll{}})
	r := login(t, addr, "ana", 1)
	reply := r.call(lookupTopic("sala")).GetLookupTopicReply()
	if reply.GetStatus() != pb.Status_STATUS_LIMIT_EXCEEDED {
		t.Fatalf("status = %v", reply.GetStatus())
	}
}

func TestBrokerSendsHeartbeats(t *testing.T) {
	_, addr := startBroker(t, Config{HeartbeatInterval: 20 * time.Millisecond, HeartbeatTimeout: time.Second})
	r := login(t, addr, "ana", 1)
	env := &pb.Envelope{}
	if err := r.wc.Receive(env); err != nil {
		t.Fatal(err)
	}
	if env.GetHeartbeat() == nil {
		t.Fatalf("esperado heartbeat, obtido %v", env)
	}
}

func TestSilentClientIsDisconnected(t *testing.T) {
	b, addr := startBroker(t, Config{HeartbeatInterval: 20 * time.Millisecond, HeartbeatTimeout: 100 * time.Millisecond})
	login(t, addr, "ana", 1)
	eventually(t, func() bool {
		_, bound := b.Contracts().Sessions.Get(1)
		return !bound
	}, "desconexão do cliente silencioso")
}

func TestClientHeartbeatsKeepConnectionAlive(t *testing.T) {
	b, addr := startBroker(t, Config{HeartbeatInterval: 20 * time.Millisecond, HeartbeatTimeout: 100 * time.Millisecond})
	r := login(t, addr, "ana", 1)
	for range 10 {
		if err := r.wc.Send(&pb.Envelope{Body: &pb.Envelope_Heartbeat{Heartbeat: &pb.Heartbeat{}}}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(30 * time.Millisecond)
	}
	if _, bound := b.Contracts().Sessions.Get(1); !bound {
		t.Fatal("cliente com heartbeat foi desconectado")
	}
}

func TestStandaloneInterestIsFreshAndEmpty(t *testing.T) {
	b, _ := startBroker(t, Config{})
	in := b.Contracts().Interest.RemoteInterest(topic.Ref{ID: 1, Generation: 1})
	if in.HasSubscribers || !in.Fresh(time.Now(), contract.InterestMaxAge) {
		t.Fatalf("visão sem par = %+v", in)
	}
}

func TestCloseStopsServeAndConnections(t *testing.T) {
	b, err := New(Config{ID: "A", Credentials: testCreds})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- b.Serve(ln) }()

	r := login(t, ln.Addr().String(), "ana", 1)
	pending := dialRaw(t, ln.Addr().String())

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-served; err != nil {
		t.Fatalf("Serve = %v", err)
	}
	r.expectClosed()
	if err := pending.wc.Receive(&pb.Envelope{}); err == nil {
		t.Fatal("conexão sem Hello deveria ser encerrada")
	}
	if err := b.Serve(ln); !errors.Is(err, ErrClosed) {
		t.Fatalf("Serve após Close = %v", err)
	}
}
