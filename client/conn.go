package client

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/internal/wire"
)

// controlConn é o canal TCP de controle com um broker.
type controlConn struct {
	wc        *wire.Conn
	brokerID  string
	udpAddr   netip.AddrPort
	heartbeat time.Duration

	mu      sync.Mutex
	nextID  uint64
	waiters map[uint64]chan *pb.Envelope
	err     error
	lost    chan struct{}
}

// dialControl conecta e autentica. Erros de autenticação e de argumento
// vêm das sentinelas do pacote; os demais são falhas de rede.
func dialControl(ctx context.Context, addr string, cfg Config, session uint64, udpPort uint16) (*controlConn, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { nc.Close() })
	defer stop()

	wc := wire.NewConn(nc, wire.Options{
		MaxFrame:     wire.MaxClientFrame,
		WriteTimeout: cfg.HeartbeatTimeout,
		ReadTimeout:  cfg.HeartbeatTimeout,
	})
	hello := &pb.Envelope{RequestId: 1, Body: &pb.Envelope_Hello{Hello: &pb.Hello{
		Identity:  cfg.Identity,
		SessionId: session,
		Token:     cfg.Token,
		UdpPort:   uint32(udpPort),
	}}}
	if err := wc.Send(hello); err != nil {
		wc.Close()
		return nil, contextErr(ctx, err)
	}
	env := &pb.Envelope{}
	if err := wc.Receive(env); err != nil {
		wc.Close()
		return nil, contextErr(ctx, err)
	}
	reply := env.GetHelloReply()
	if reply == nil {
		wc.Close()
		return nil, fmt.Errorf("client: resposta inesperada ao Hello de %s", addr)
	}
	if err := statusError(reply.GetStatus(), reply.GetDetail()); err != nil {
		wc.Close()
		return nil, err
	}
	if !stop() {
		wc.Close()
		return nil, ctx.Err()
	}

	remote, err := netip.ParseAddrPort(nc.RemoteAddr().String())
	if err != nil {
		wc.Close()
		return nil, err
	}
	interval := time.Duration(reply.GetHeartbeatIntervalMs()) * time.Millisecond
	if interval <= 0 || interval >= cfg.HeartbeatTimeout {
		interval = cfg.HeartbeatTimeout / 5
	}
	return &controlConn{
		wc:        wc,
		brokerID:  reply.GetBrokerId(),
		udpAddr:   netip.AddrPortFrom(remote.Addr().Unmap(), uint16(reply.GetUdpPort())),
		heartbeat: interval,
		nextID:    1,
		waiters:   make(map[uint64]chan *pb.Envelope),
		lost:      make(chan struct{}),
	}, nil
}

func contextErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// start inicia a leitura e o envio de heartbeats. onPush recebe as
// mensagens enviadas pelo broker por iniciativa própria; onLost é chamado
// uma vez quando o canal se perde, exceto após close.
func (c *controlConn) start(onPush func(*pb.Envelope), onLost func()) {
	go c.readLoop(onPush, onLost)
	go c.heartbeatLoop()
}

func (c *controlConn) readLoop(onPush func(*pb.Envelope), onLost func()) {
	for {
		env := &pb.Envelope{}
		if err := c.wc.Receive(env); err != nil {
			if c.fail(fmt.Errorf("%w: %w", ErrConnectionLost, err)) {
				onLost()
			}
			return
		}
		if env.RequestId == 0 {
			if env.GetHeartbeat() == nil {
				onPush(env)
			}
			continue
		}
		c.mu.Lock()
		ch := c.waiters[env.RequestId]
		delete(c.waiters, env.RequestId)
		c.mu.Unlock()
		if ch != nil {
			ch <- env
		}
	}
}

func (c *controlConn) heartbeatLoop() {
	ticker := time.NewTicker(c.heartbeat)
	defer ticker.Stop()
	var nonce uint64
	for {
		select {
		case <-c.lost:
			return
		case <-ticker.C:
			nonce++
			if err := c.wc.Send(&pb.Envelope{Body: &pb.Envelope_Heartbeat{Heartbeat: &pb.Heartbeat{Nonce: nonce}}}); err != nil {
				c.wc.Close()
				return
			}
		}
	}
}

// call envia um comando e espera a resposta de mesmo request_id.
func (c *controlConn) call(ctx context.Context, env *pb.Envelope) (*pb.Envelope, error) {
	ch := make(chan *pb.Envelope, 1)
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		return nil, err
	}
	c.nextID++
	id := c.nextID
	c.waiters[id] = ch
	c.mu.Unlock()

	env.RequestId = id
	if err := c.wc.Send(env); err != nil {
		c.forget(id)
		c.wc.Close()
		return nil, fmt.Errorf("%w: %w", ErrConnectionLost, err)
	}
	select {
	case reply := <-ch:
		return reply, nil
	case <-c.lost:
		select {
		case reply := <-ch:
			return reply, nil
		default:
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		return nil, c.err
	case <-ctx.Done():
		c.forget(id)
		return nil, ctx.Err()
	}
}

func (c *controlConn) forget(id uint64) {
	c.mu.Lock()
	delete(c.waiters, id)
	c.mu.Unlock()
}

// fail marca o canal como perdido. Devolve verdadeiro só na primeira vez.
func (c *controlConn) fail(err error) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return false
	}
	c.err = err
	close(c.lost)
	return true
}

// cause devolve o motivo da perda do canal, ou nil se ele está ativo.
func (c *controlConn) cause() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *controlConn) close() {
	c.fail(ErrClosed)
	c.wc.Close()
}
