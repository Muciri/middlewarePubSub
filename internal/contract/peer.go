package contract

import (
	"net/netip"
	"sync"
	"time"

	"github.com/franciscovmn/middlewarePubSub/topic"
)

// InterestMaxAge é a idade máxima da visão do par para que a ausência de
// assinantes remotos seja aceita como confirmada (NO_SUBSCRIBERS).
const InterestMaxAge = time.Second

// Interest é a visão do broker local sobre os assinantes do par num tópico.
// UpdatedAt é o instante, no relógio local, da última confirmação da visão
// completa recebida pelo canal entre brokers; zero indica visão inexistente.
type Interest struct {
	HasSubscribers bool
	UpdatedAt      time.Time
}

// Fresh informa se a visão foi confirmada há menos de maxAge.
func (i Interest) Fresh(now time.Time, maxAge time.Duration) bool {
	return !i.UpdatedAt.IsZero() && now.Sub(i.UpdatedAt) < maxAge
}

// InterestView é consultada na admissão. A visão é descartada quando o
// enlace cai, então a idade dela já reflete o estado do enlace: NO_SUBSCRIBERS
// exige ausência local e visão recente sem assinantes; visão antiga ou
// inexistente leva a UNAVAILABLE. Sem par configurado, a visão é sempre
// recente e vazia.
type InterestView interface {
	RemoteInterest(ref topic.Ref) Interest
}

type MemoryInterest struct {
	mu        sync.RWMutex
	topics    map[topic.Ref]bool
	updatedAt time.Time
}

var _ InterestView = (*MemoryInterest)(nil)

func NewMemoryInterest() *MemoryInterest {
	return &MemoryInterest{topics: make(map[topic.Ref]bool)}
}

// Replace troca a visão inteira, como ao receber o snapshot de interesse.
func (m *MemoryInterest) Replace(refs []topic.Ref, at time.Time) {
	topics := make(map[topic.Ref]bool, len(refs))
	for _, ref := range refs {
		topics[ref] = true
	}
	m.mu.Lock()
	m.topics = topics
	m.updatedAt = at
	m.mu.Unlock()
}

// Set altera um tópico e confirma a visão em at.
func (m *MemoryInterest) Set(ref topic.Ref, has bool, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if has {
		m.topics[ref] = true
	} else {
		delete(m.topics, ref)
	}
	m.updatedAt = at
}

// Touch confirma a visão sem alterá-la, como ao receber um heartbeat do par.
func (m *MemoryInterest) Touch(at time.Time) {
	m.mu.Lock()
	m.updatedAt = at
	m.mu.Unlock()
}

// Invalidate descarta a visão, como ao perder o enlace.
func (m *MemoryInterest) Invalidate() {
	m.mu.Lock()
	m.topics = make(map[topic.Ref]bool)
	m.updatedAt = time.Time{}
	m.mu.Unlock()
}

func (m *MemoryInterest) RemoteInterest(ref topic.Ref) Interest {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return Interest{HasSubscribers: m.topics[ref], UpdatedAt: m.updatedAt}
}

// LinkStatus descreve o enlace TCP com o par. RTT é a última medida pelo
// heartbeat, usada para descontar RTT/2 da validade no encaminhamento.
// PeerUDP é o destino dos eventos encaminhados ao par: o IP da conexão do
// enlace com a porta informada pelo par, ou o endereço configurado como
// sobrescrita. Permanece com o último valor conhecido após a queda.
type LinkStatus struct {
	Alive   bool
	Since   time.Time
	RTT     time.Duration
	PeerUDP netip.AddrPort
}

type LinkState interface {
	Link() LinkStatus
}

type MemoryLink struct {
	mu sync.RWMutex
	st LinkStatus
}

var _ LinkState = (*MemoryLink)(nil)

func (m *MemoryLink) Set(st LinkStatus) {
	m.mu.Lock()
	m.st = st
	m.mu.Unlock()
}

func (m *MemoryLink) Link() LinkStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.st
}
