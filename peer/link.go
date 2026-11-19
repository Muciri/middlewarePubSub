package peer

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/contract"
	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/internal/wire"
)

var (
	ErrUnavailable = errors.New("peer: par indisponível")
	ErrClosed      = errors.New("peer: enlace encerrado")
	ErrRejected    = errors.New("peer: conexão recusada pelo par")
)

// Link mantém no máximo uma conexão com o par. O broker administrativo
// disca e reconecta com recuo progressivo; o outro aceita e recusa uma
// segunda conexão enquanto a primeira estiver ativa.
type Link struct {
	cfg      Config
	store    Store
	interest *contract.MemoryInterest
	applied  *opSet

	mu        sync.Mutex
	sess      *session
	status    contract.LinkStatus
	closed    bool
	listeners map[net.Listener]struct{}
	stop      chan struct{}
	wg        sync.WaitGroup
}

var _ contract.LinkState = (*Link)(nil)

func New(cfg Config, store Store) (*Link, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Link{
		cfg:       cfg,
		store:     store,
		interest:  contract.NewMemoryInterest(),
		applied:   newOpSet(1024),
		status:    contract.LinkStatus{Since: time.Now()},
		listeners: make(map[net.Listener]struct{}),
		stop:      make(chan struct{}),
	}, nil
}

func (l *Link) Admin() bool { return l.cfg.Admin }

// Interest é a visão do interesse do par.
func (l *Link) Interest() contract.InterestView { return l.interest }

func (l *Link) Link() contract.LinkStatus {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.status
}

// Start inicia a discagem no broker administrativo; no outro não faz nada.
func (l *Link) Start() {
	if !l.cfg.Admin {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.wg.Add(1)
	go l.dialLoop()
}

func (l *Link) Close() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	close(l.stop)
	for ln := range l.listeners {
		ln.Close()
	}
	s := l.sess
	l.mu.Unlock()
	if s != nil {
		s.close()
	}
	l.wg.Wait()
}

func (l *Link) dialLoop() {
	defer l.wg.Done()
	backoff := l.cfg.BackoffMin
	for {
		s, err := l.dial()
		if err == nil {
			backoff = l.cfg.BackoffMin
			s.run()
			continue
		}
		if errors.Is(err, ErrClosed) {
			return
		}
		select {
		case <-l.stop:
			return
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, l.cfg.BackoffMax)
	}
}

func (l *Link) wireOptions() wire.Options {
	return wire.Options{MaxFrame: wire.MaxPeerFrame, WriteTimeout: l.cfg.DeadAfter, ReadTimeout: l.cfg.DeadAfter}
}

// dial abre a conexão, apresenta-se e envia o snapshot do catálogo.
func (l *Link) dial() (*session, error) {
	nc, err := net.DialTimeout("tcp", l.cfg.DialAddr, l.cfg.DeadAfter)
	if err != nil {
		return nil, err
	}
	wc := wire.NewConn(nc, l.wireOptions())
	fail := func(err error) (*session, error) {
		wc.Close()
		return nil, err
	}

	if err := wc.Send(&pb.PeerEnvelope{Body: &pb.PeerEnvelope_Hello{Hello: &pb.PeerHello{
		BrokerId: l.cfg.LocalID, Token: l.cfg.Token,
	}}}); err != nil {
		return fail(err)
	}
	env := &pb.PeerEnvelope{}
	if err := wc.Receive(env); err != nil {
		return fail(err)
	}
	reply := env.GetHelloReply()
	switch {
	case reply == nil:
		return fail(fmt.Errorf("%w: resposta inesperada", ErrRejected))
	case reply.GetStatus() != pb.Status_STATUS_OK:
		return fail(fmt.Errorf("%w: %v %s", ErrRejected, reply.GetStatus(), reply.GetDetail()))
	case reply.GetBrokerId() != l.cfg.PeerID:
		return fail(fmt.Errorf("%w: par se identificou como %q", ErrRejected, reply.GetBrokerId()))
	}

	s := newSession(l, wc)
	if err := l.attach(s); err != nil {
		return fail(err)
	}
	snap := l.store.CatalogSnapshot()
	if err := wc.Send(&pb.PeerEnvelope{Body: &pb.PeerEnvelope_CatalogSnapshot{CatalogSnapshot: snapshotToProto(snap)}}); err != nil {
		return nil, s.abort(err)
	}
	if err := wc.Receive(env); err != nil {
		return nil, s.abort(err)
	}
	if ack := env.GetCatalogSnapshotAck(); ack == nil || ack.GetVersion() != snap.Version {
		return nil, s.abort(fmt.Errorf("%w: snapshot não confirmado", ErrRejected))
	}
	return s, nil
}

// Serve aceita conexões do par até Close.
func (l *Link) Serve(ln net.Listener) error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		ln.Close()
		return ErrClosed
	}
	l.listeners[ln] = struct{}{}
	l.mu.Unlock()

	for {
		nc, err := ln.Accept()
		if err != nil {
			l.mu.Lock()
			closed := l.closed
			delete(l.listeners, ln)
			l.mu.Unlock()
			if closed {
				return nil
			}
			return err
		}
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			nc.Close()
			return nil
		}
		l.wg.Add(1)
		l.mu.Unlock()
		go l.accept(nc)
	}
}

func (l *Link) accept(nc net.Conn) {
	defer l.wg.Done()
	wc := wire.NewConn(nc, l.wireOptions())
	reject := func(st pb.Status, detail string) {
		wc.Send(&pb.PeerEnvelope{Body: &pb.PeerEnvelope_HelloReply{HelloReply: &pb.PeerHelloReply{
			Status: st, Detail: detail, BrokerId: l.cfg.LocalID,
		}}})
		wc.Close()
	}

	env := &pb.PeerEnvelope{}
	if err := wc.Receive(env); err != nil {
		wc.Close()
		return
	}
	hello := env.GetHello()
	if hello == nil {
		wc.Close()
		return
	}
	if l.cfg.Admin || hello.GetBrokerId() != l.cfg.PeerID ||
		subtle.ConstantTimeCompare([]byte(hello.GetToken()), []byte(l.cfg.Token)) != 1 {
		reject(pb.Status_STATUS_UNAUTHENTICATED, "par não reconhecido")
		return
	}
	s := newSession(l, wc)
	if err := l.attach(s); err != nil {
		reject(pb.Status_STATUS_SESSION_CONFLICT, "já existe conexão com o par")
		return
	}
	if err := wc.Send(&pb.PeerEnvelope{Body: &pb.PeerEnvelope_HelloReply{HelloReply: &pb.PeerHelloReply{
		Status: pb.Status_STATUS_OK, BrokerId: l.cfg.LocalID,
	}}}); err != nil {
		s.abort(err)
		return
	}

	if err := wc.Receive(env); err != nil {
		s.abort(err)
		return
	}
	m := env.GetCatalogSnapshot()
	if m == nil {
		s.abort(fmt.Errorf("%w: esperado snapshot do catálogo", ErrRejected))
		return
	}
	snap, err := snapshotFromProto(m)
	if err == nil {
		err = l.store.ApplySnapshot(snap)
	}
	if err != nil {
		s.abort(err)
		return
	}
	l.applied.reset()
	if err := wc.Send(&pb.PeerEnvelope{Body: &pb.PeerEnvelope_CatalogSnapshotAck{CatalogSnapshotAck: &pb.CatalogSnapshotAck{
		Version: snap.Version,
	}}}); err != nil {
		s.abort(err)
		return
	}
	s.run()
}

// attach registra a sessão como a conexão com o par.
func (l *Link) attach(s *session) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch {
	case l.closed:
		return ErrClosed
	case l.sess != nil:
		return fmt.Errorf("%w: conexão duplicada", ErrRejected)
	}
	l.sess = s
	return nil
}

// detach remove a sessão encerrada e descarta a visão do par.
func (l *Link) detach(s *session) {
	l.mu.Lock()
	if l.sess != s {
		l.mu.Unlock()
		return
	}
	l.sess = nil
	l.status.Alive = false
	l.status.Since = time.Now()
	l.mu.Unlock()
	l.interest.Invalidate()
}

func (l *Link) markAlive(s *session) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.sess == s {
		l.status.Alive = true
		l.status.Since = time.Now()
	}
}

func (l *Link) setRTT(rtt time.Duration) {
	l.mu.Lock()
	l.status.RTT = rtt
	l.mu.Unlock()
}

// readySession devolve a sessão que já trocou os snapshots.
func (l *Link) readySession() (*session, error) {
	l.mu.Lock()
	s := l.sess
	l.mu.Unlock()
	if s == nil || !s.isReady() {
		return nil, ErrUnavailable
	}
	return s, nil
}

// NewOperationID gera um identificador aleatório de 128 bits.
func NewOperationID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
