package client

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/eventwire"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

// Datagram é o evento entregue ao plano de dados para envio por UDP.
type Datagram struct {
	Topic    topic.Ref
	Session  uint64
	Sequence uint64
	TTL      time.Duration
	Payload  []byte
}

// UDPDataPlane implementa o transporte UDP real do cliente.
type UDPDataPlane struct {
	conn    *net.UDPConn
	mu      sync.Mutex
	broker  netip.AddrPort
	deliver func(Event)
	closed  bool
	last    map[stream]uint64
}
type stream struct {
	session uint64
	topic   topic.Ref
}

func ListenUDPDataPlane(addr string) (*UDPDataPlane, error) {
	a, e := net.ResolveUDPAddr("udp", addr)
	if e != nil {
		return nil, e
	}
	c, e := net.ListenUDP("udp", a)
	if e != nil {
		return nil, e
	}
	d := &UDPDataPlane{conn: c, last: map[stream]uint64{}}
	go d.readLoop()
	return d, nil
}
func (d *UDPDataPlane) Port() uint16 {
	if a, ok := d.conn.LocalAddr().(*net.UDPAddr); ok {
		return uint16(a.Port)
	}
	return 0
}
func (d *UDPDataPlane) SetBroker(a netip.AddrPort) { d.mu.Lock(); d.broker = a; d.mu.Unlock() }
func (d *UDPDataPlane) SetDeliver(fn func(Event))  { d.mu.Lock(); d.deliver = fn; d.mu.Unlock() }
func (d *UDPDataPlane) Send(x Datagram) error {
	p, e := eventwire.Encode(eventwire.Header{Topic: x.Topic.ID, Generation: x.Topic.Generation, Session: x.Session, Sequence: x.Sequence, TTL: x.TTL}, x.Payload)
	if e != nil {
		return e
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrDataPlaneClosed
	}
	if !d.broker.IsValid() {
		return fmt.Errorf("client: destino UDP não definido")
	}
	_, e = d.conn.WriteToUDPAddrPort(p, d.broker)
	return e
}
func (d *UDPDataPlane) readLoop() {
	buf := make([]byte, eventwire.HeaderSize+eventwire.MaxPayload+1)
	for {
		n, _, e := d.conn.ReadFromUDP(buf)
		if e != nil {
			return
		}
		h, p, e := eventwire.Decode(buf[:n])
		if e != nil {
			continue
		}
		key := stream{h.Session, topic.Ref{ID: h.Topic, Generation: h.Generation}}
		d.mu.Lock()
		if h.Sequence <= d.last[key] {
			d.mu.Unlock()
			continue
		}
		d.last[key] = h.Sequence
		fn := d.deliver
		d.mu.Unlock()
		if fn != nil && h.TTL > 0 {
			fn(Event{Ref: key.topic, Session: h.Session, Sequence: h.Sequence, Payload: append([]byte(nil), p...), Deadline: time.Now().Add(h.TTL)})
		}
	}
}
func (d *UDPDataPlane) Close() error {
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()
	return d.conn.Close()
}

// DataPlane é o lado UDP da biblioteca: monta e envia os datagramas e
// entrega os eventos recebidos. Deduplicação por sequência e descarte de
// eventos expirados acontecem antes da entrega.
type DataPlane interface {
	// Port é a porta UDP local anunciada no Hello; zero se não recebe eventos.
	Port() uint16
	// SetBroker define o destino dos datagramas, trocado na reconexão.
	SetBroker(addr netip.AddrPort)
	Send(d Datagram) error
	// SetDeliver registra a função chamada para cada evento recebido. Os
	// campos Ref, Session, Sequence, Payload e Deadline vêm preenchidos.
	SetDeliver(fn func(Event))
	Close() error
}

var ErrDataPlaneClosed = errors.New("client: plano de dados encerrado")

// MemoryDataPlane guarda os datagramas enviados e permite injetar eventos.
// Serve para testes e para usar a biblioteca sem o transporte UDP.
type MemoryDataPlane struct {
	port uint16

	mu      sync.Mutex
	broker  netip.AddrPort
	sent    []Datagram
	deliver func(Event)
	closed  bool
}

var _ DataPlane = (*MemoryDataPlane)(nil)

func NewMemoryDataPlane(port uint16) *MemoryDataPlane {
	return &MemoryDataPlane{port: port}
}

func (m *MemoryDataPlane) Port() uint16 { return m.port }

func (m *MemoryDataPlane) SetBroker(addr netip.AddrPort) {
	m.mu.Lock()
	m.broker = addr
	m.mu.Unlock()
}

func (m *MemoryDataPlane) Broker() netip.AddrPort {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.broker
}

func (m *MemoryDataPlane) Send(d Datagram) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrDataPlaneClosed
	}
	m.sent = append(m.sent, d)
	return nil
}

func (m *MemoryDataPlane) Sent() []Datagram {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Datagram(nil), m.sent...)
}

func (m *MemoryDataPlane) SetDeliver(fn func(Event)) {
	m.mu.Lock()
	m.deliver = fn
	m.mu.Unlock()
}

// Inject entrega um evento como se tivesse chegado por UDP.
func (m *MemoryDataPlane) Inject(e Event) {
	m.mu.Lock()
	fn, closed := m.deliver, m.closed
	m.mu.Unlock()
	if fn != nil && !closed {
		fn(e)
	}
}

func (m *MemoryDataPlane) Close() error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	return nil
}
