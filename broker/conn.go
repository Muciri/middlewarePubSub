package broker

import (
	"crypto/subtle"
	"math"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/contract"
	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/internal/wire"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

type clientConn struct {
	b       *Broker
	wc      *wire.Conn
	session contract.Session
	release func()

	// outbox guarda as mensagens enviadas por iniciativa do broker.
	outbox chan *pb.Envelope

	closeOnce sync.Once
	closing   chan struct{}
	done      chan struct{}
}

func (b *Broker) handle(nc net.Conn) {
	defer b.untrack(nc)
	wc := wire.NewConn(nc, wire.Options{
		MaxFrame:     wire.MaxClientFrame,
		WriteTimeout: b.cfg.WriteTimeout,
		ReadTimeout:  b.cfg.HeartbeatTimeout,
	})
	c := b.admit(wc, nc.RemoteAddr())
	if c == nil {
		wc.Close()
		return
	}
	c.serve()
}

// admit trata o Hello. Devolve nil quando a conexão deve ser encerrada; o
// motivo, se houver, já foi respondido ao cliente.
func (b *Broker) admit(wc *wire.Conn, remote net.Addr) *clientConn {
	env := &pb.Envelope{}
	if err := wc.Receive(env); err != nil {
		return nil
	}
	hello := env.GetHello()
	if hello == nil {
		return nil
	}
	reject := func(st pb.Status, detail string) *clientConn {
		wc.Send(&pb.Envelope{RequestId: env.RequestId, Body: &pb.Envelope_HelloReply{HelloReply: &pb.HelloReply{
			Status: st, Detail: detail, BrokerId: b.cfg.ID,
		}}})
		return nil
	}

	cred, known := b.cfg.Credentials[hello.Identity]
	if !known || subtle.ConstantTimeCompare([]byte(cred.Token), []byte(hello.Token)) != 1 {
		return reject(pb.Status_STATUS_UNAUTHENTICATED, "identidade ou token inválido")
	}
	if hello.SessionId == 0 || hello.UdpPort > math.MaxUint16 {
		return reject(pb.Status_STATUS_INVALID_ARGUMENT, "sessão zero ou porta UDP inválida")
	}

	s := contract.Session{
		ID:       contract.SessionID(hello.SessionId),
		Identity: hello.Identity,
		Admin:    cred.Admin,
		UDPAddr:  udpAddr(remote, uint16(hello.UdpPort)),
	}
	c, err := b.register(wc, s)
	if err != nil {
		return reject(pb.Status_STATUS_SESSION_CONFLICT, "sessão ligada a outra identidade")
	}
	if c == nil {
		return nil
	}

	err = wc.Send(&pb.Envelope{RequestId: env.RequestId, Body: &pb.Envelope_HelloReply{HelloReply: &pb.HelloReply{
		Status:              pb.Status_STATUS_OK,
		BrokerId:            b.cfg.ID,
		HeartbeatIntervalMs: uint32(b.cfg.HeartbeatInterval / time.Millisecond),
		UdpPort:             uint32(b.cfg.EventPort),
	}}})
	if err != nil {
		c.close()
		c.cleanup()
		return nil
	}
	return c
}

// register liga a sessão à conexão. Se a mesma identidade já usa a sessão
// em outra conexão, a anterior é encerrada e limpa antes do novo vínculo.
// Devolve nil sem erro se o broker estiver encerrando.
func (b *Broker) register(wc *wire.Conn, s contract.Session) (*clientConn, error) {
	b.helloMu.Lock()
	defer b.helloMu.Unlock()

	if cur, ok := b.sessions.Get(s.ID); ok && cur.Identity != s.Identity {
		return nil, contract.ErrSessionConflict
	}
	b.mu.Lock()
	old := b.conns[s.ID]
	b.mu.Unlock()
	if old != nil {
		old.close()
		<-old.done
	}

	release, err := b.sessions.Bind(s)
	if err != nil {
		return nil, err
	}
	c := &clientConn{
		b:       b,
		wc:      wc,
		session: s,
		release: release,
		outbox:  make(chan *pb.Envelope, b.cfg.ResultQueue),
		closing: make(chan struct{}),
		done:    make(chan struct{}),
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		release()
		return nil, nil
	}
	b.conns[s.ID] = c
	return c, nil
}

func udpAddr(remote net.Addr, port uint16) netip.AddrPort {
	if remote == nil {
		return netip.AddrPort{}
	}
	ap, err := netip.ParseAddrPort(remote.String())
	if err != nil {
		return netip.AddrPort{}
	}
	return netip.AddrPortFrom(ap.Addr().Unmap(), port)
}

func (c *clientConn) serve() {
	go c.writeLoop()
	for {
		req := &pb.Envelope{}
		if err := c.wc.Receive(req); err != nil {
			break
		}
		if !c.dispatch(req) {
			break
		}
	}
	c.close()
	c.cleanup()
}

func (c *clientConn) writeLoop() {
	ticker := time.NewTicker(c.b.cfg.HeartbeatInterval)
	defer ticker.Stop()
	var nonce uint64
	for {
		var env *pb.Envelope
		select {
		case <-c.closing:
			return
		case env = <-c.outbox:
		case <-ticker.C:
			nonce++
			env = &pb.Envelope{Body: &pb.Envelope_Heartbeat{Heartbeat: &pb.Heartbeat{Nonce: nonce}}}
		}
		if err := c.wc.Send(env); err != nil {
			c.close()
			return
		}
	}
}

// push enfileira sem bloquear; com a fila cheia, a mensagem é descartada.
func (c *clientConn) push(env *pb.Envelope) bool {
	select {
	case <-c.closing:
		return false
	default:
	}
	select {
	case c.outbox <- env:
		return true
	default:
		return false
	}
}

func (c *clientConn) close() {
	c.closeOnce.Do(func() {
		close(c.closing)
		c.wc.Close()
	})
}

// cleanup desfaz a sessão e as inscrições da conexão. Roda uma única vez,
// na goroutine que atende a conexão.
func (c *clientConn) cleanup() {
	b := c.b
	b.mu.Lock()
	if b.conns[c.session.ID] == c {
		delete(b.conns, c.session.ID)
	}
	b.mu.Unlock()
	removed := b.subs.RemoveSession(c.session.ID)
	c.release()
	if b.link != nil && len(removed) > 0 {
		refs := make([]topic.Ref, len(removed))
		for i, sub := range removed {
			refs[i] = sub.Topic.Ref
		}
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			b.withdrawInterest(refs)
		}()
	}
	close(c.done)
}
