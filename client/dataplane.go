package client

import (
	"errors"
	"net/netip"
	"sync"
	"time"

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
