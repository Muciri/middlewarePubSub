package peer

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/internal/protoconv"
	"github.com/franciscovmn/middlewarePubSub/internal/wire"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

// session é uma conexão com o par, do Hello até a queda.
type session struct {
	l  *Link
	wc *wire.Conn

	closeOnce sync.Once
	done      chan struct{}

	// interestMu mantém as atualizações de interesse na ordem de seq.
	interestMu sync.Mutex
	outSeq     uint64

	mu           sync.Mutex
	ready        bool
	inSeq        uint64
	viewKnown    bool
	catalogAcks  map[string]chan *pb.CatalogAck
	forwards     map[string]chan *pb.ForwardCatalogReply
	interestAcks map[uint64]chan struct{}
	pings        map[uint64]time.Time
	// cause é o motivo do encerramento, registrado na queda do enlace.
	cause error
}

func newSession(l *Link, wc *wire.Conn) *session {
	return &session{
		l:            l,
		wc:           wc,
		done:         make(chan struct{}),
		catalogAcks:  make(map[string]chan *pb.CatalogAck),
		forwards:     make(map[string]chan *pb.ForwardCatalogReply),
		interestAcks: make(map[uint64]chan struct{}),
		pings:        make(map[uint64]time.Time),
	}
}

func (s *session) close() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.wc.Close()
	})
}

// fail encerra a sessão guardando o primeiro motivo, e devolve err.
func (s *session) fail(err error) error {
	s.mu.Lock()
	if s.cause == nil {
		s.cause = err
	}
	s.mu.Unlock()
	s.close()
	return err
}

func (s *session) failure() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cause
}

// abort encerra uma sessão que não chegou a funcionar.
func (s *session) abort(err error) error {
	s.fail(err)
	s.l.detach(s)
	return err
}

func (s *session) isReady() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ready
}

func (s *session) send(env *pb.PeerEnvelope) error {
	return s.wc.Send(env)
}

// run envia o snapshot de interesse, marca o enlace como vivo e atende a
// conexão até ela cair.
func (s *session) run() {
	defer s.l.detach(s)
	defer s.close()

	var err error
	var sent int
	s.l.store.WithLocalInterest(func(refs []topic.Ref) {
		sent = len(refs)
		m := &pb.InterestSnapshot{}
		for _, r := range refs {
			m.Topics = append(m.Topics, refToProto(r))
		}
		s.interestMu.Lock()
		defer s.interestMu.Unlock()
		if err = s.send(&pb.PeerEnvelope{Body: &pb.PeerEnvelope_InterestSnapshot{InterestSnapshot: m}}); err != nil {
			return
		}
		s.mu.Lock()
		s.ready = true
		s.mu.Unlock()
		s.l.markAlive(s)
	})
	if err != nil {
		s.fail(fmt.Errorf("envio do snapshot de interesse: %w", err))
		return
	}
	s.l.log.Info("enlace: interesse local enviado ao par", "par", s.l.cfg.PeerID, "topicos", sent)

	go s.heartbeat()
	for {
		env := &pb.PeerEnvelope{}
		if err := s.wc.Receive(env); err != nil {
			s.fail(readFailure(err, s.l.cfg.DeadAfter))
			return
		}
		if !s.handle(env) {
			s.fail(errors.New("mensagem fora do protocolo"))
			return
		}
		s.mu.Lock()
		known := s.viewKnown
		s.mu.Unlock()
		if known {
			s.l.interest.Touch(time.Now())
		}
	}
}

func (s *session) heartbeat() {
	ticker := time.NewTicker(s.l.cfg.HeartbeatInterval)
	defer ticker.Stop()
	var nonce uint64
	for {
		select {
		case <-s.done:
			return
		case now := <-ticker.C:
			nonce++
			s.mu.Lock()
			s.pings[nonce] = now
			for n, sent := range s.pings {
				if now.Sub(sent) > s.l.cfg.DeadAfter {
					delete(s.pings, n)
				}
			}
			s.mu.Unlock()
			if err := s.send(&pb.PeerEnvelope{Body: &pb.PeerEnvelope_Ping{Ping: &pb.Ping{Nonce: nonce}}}); err != nil {
				s.fail(fmt.Errorf("envio de heartbeat: %w", err))
				return
			}
		}
	}
}

// handle trata uma mensagem recebida; falso encerra a conexão.
func (s *session) handle(env *pb.PeerEnvelope) bool {
	admin := s.l.cfg.Admin
	switch body := env.Body.(type) {
	case *pb.PeerEnvelope_Ping:
		return s.send(&pb.PeerEnvelope{Body: &pb.PeerEnvelope_Pong{Pong: &pb.Pong{Nonce: body.Ping.GetNonce()}}}) == nil
	case *pb.PeerEnvelope_Pong:
		s.mu.Lock()
		sent, ok := s.pings[body.Pong.GetNonce()]
		delete(s.pings, body.Pong.GetNonce())
		s.mu.Unlock()
		if ok {
			s.l.setRTT(time.Since(sent))
		}
		return true
	case *pb.PeerEnvelope_CatalogUpdate:
		return !admin && s.handleUpdate(body.CatalogUpdate)
	case *pb.PeerEnvelope_CatalogAck:
		s.mu.Lock()
		ch := s.catalogAcks[body.CatalogAck.GetOperationId()]
		delete(s.catalogAcks, body.CatalogAck.GetOperationId())
		s.mu.Unlock()
		if ch != nil {
			ch <- body.CatalogAck
		}
		return admin
	case *pb.PeerEnvelope_ForwardCatalog:
		if !admin {
			return false
		}
		go s.handleForward(body.ForwardCatalog)
		return true
	case *pb.PeerEnvelope_ForwardCatalogReply:
		s.mu.Lock()
		ch := s.forwards[body.ForwardCatalogReply.GetOperationId()]
		delete(s.forwards, body.ForwardCatalogReply.GetOperationId())
		s.mu.Unlock()
		if ch != nil {
			ch <- body.ForwardCatalogReply
		}
		return !admin
	case *pb.PeerEnvelope_InterestSnapshot:
		refs := make([]topic.Ref, 0, len(body.InterestSnapshot.GetTopics()))
		for _, r := range body.InterestSnapshot.GetTopics() {
			refs = append(refs, refFromProto(r))
		}
		s.l.interest.Replace(refs, time.Now())
		s.mu.Lock()
		s.inSeq = body.InterestSnapshot.GetSeq()
		s.viewKnown = true
		s.mu.Unlock()
		s.l.log.Info("enlace: interesse do par recebido", "par", s.l.cfg.PeerID, "topicos", len(refs))
		return true
	case *pb.PeerEnvelope_InterestUpdate:
		return s.handleInterestUpdate(body.InterestUpdate)
	case *pb.PeerEnvelope_InterestAck:
		s.mu.Lock()
		ch := s.interestAcks[body.InterestAck.GetSeq()]
		delete(s.interestAcks, body.InterestAck.GetSeq())
		s.mu.Unlock()
		if ch != nil {
			close(ch)
		}
		return true
	}
	return false
}

// handleUpdate aplica uma alteração do catálogo no broker não
// administrativo. Uma operação já vista ou uma versão já aplicada é
// confirmada sem reaplicar; uma versão fora de sequência é recusada, e o
// broker administrativo derruba o enlace para reenviar o snapshot.
func (s *session) handleUpdate(m *pb.CatalogUpdate) bool {
	ack := &pb.CatalogAck{OperationId: m.GetOperationId(), Version: m.GetVersion(), Status: pb.Status_STATUS_OK}
	cur := s.l.store.CatalogVersion()
	switch {
	case s.l.applied.has(m.GetOperationId()) || m.GetVersion() <= cur:
	case m.GetVersion() != cur+1:
		ack.Status = pb.Status_STATUS_INTERNAL
		ack.Detail = fmt.Sprintf("versão %d fora de sequência; local %d", m.GetVersion(), cur)
	default:
		u, err := updateFromProto(m)
		if err == nil {
			err = s.l.store.ApplyUpdate(u)
		}
		if err != nil {
			ack.Status = pb.Status_STATUS_INTERNAL
			ack.Detail = err.Error()
		} else {
			s.l.applied.add(m.GetOperationId())
		}
	}
	return s.send(&pb.PeerEnvelope{Body: &pb.PeerEnvelope_CatalogAck{CatalogAck: ack}}) == nil
}

func (s *session) handleForward(m *pb.ForwardCatalog) {
	reply := &pb.ForwardCatalogReply{OperationId: m.GetOperationId()}
	req, ok := requestFromProto(m)
	if !ok {
		reply.Status = pb.Status_STATUS_INVALID_ARGUMENT
		reply.Detail = "pedido sem operação"
	} else {
		res := s.l.store.Execute(req)
		reply.Status = res.Status
		reply.Detail = res.Detail
		if res.Status == pb.Status_STATUS_OK {
			reply.Topic = protoconv.TopicInfoToProto(res.Topic)
		}
	}
	if err := s.send(&pb.PeerEnvelope{Body: &pb.PeerEnvelope_ForwardCatalogReply{ForwardCatalogReply: reply}}); err != nil {
		s.fail(fmt.Errorf("resposta ao repasse: %w", err))
	}
}

func (s *session) handleInterestUpdate(m *pb.InterestUpdate) bool {
	s.mu.Lock()
	if !s.viewKnown || m.GetSeq() != s.inSeq+1 {
		s.mu.Unlock()
		return false
	}
	s.inSeq = m.GetSeq()
	s.mu.Unlock()
	s.l.interest.Set(refFromProto(m.GetTopic()), m.GetHasSubscribers(), time.Now())
	return s.send(&pb.PeerEnvelope{Body: &pb.PeerEnvelope_InterestAck{InterestAck: &pb.InterestAck{Seq: m.GetSeq()}}}) == nil
}

// opSet guarda os últimos identificadores de operação aplicados.
type opSet struct {
	mu   sync.Mutex
	ids  map[string]struct{}
	ring []string
	next int
}

func newOpSet(size int) *opSet {
	return &opSet{ids: make(map[string]struct{}, size), ring: make([]string, size)}
}

func (o *opSet) has(id string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	_, ok := o.ids[id]
	return ok
}

func (o *opSet) add(id string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.ids[id]; ok {
		return
	}
	if old := o.ring[o.next]; old != "" {
		delete(o.ids, old)
	}
	o.ring[o.next] = id
	o.ids[id] = struct{}{}
	o.next = (o.next + 1) % len(o.ring)
}

func (o *opSet) reset() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ids = make(map[string]struct{}, len(o.ring))
	for i := range o.ring {
		o.ring[i] = ""
	}
	o.next = 0
}

// readFailure descreve a falha de leitura, distinguindo o silêncio do par
// do encerramento explícito da conexão.
func readFailure(err error, silence time.Duration) error {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return fmt.Errorf("par em silêncio por %v", silence)
	}
	return fmt.Errorf("conexão encerrada: %w", err)
}
