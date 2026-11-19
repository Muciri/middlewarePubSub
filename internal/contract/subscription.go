package contract

import (
	"net/netip"
	"sort"
	"sync"

	"github.com/franciscovmn/middlewarePubSub/topic"
)

type SubscriptionID uint64

// Subscription é uma inscrição local. Topic traz a geração e a política de
// fila vigentes no momento da inscrição.
type Subscription struct {
	ID       SubscriptionID
	Session  SessionID
	Identity string
	UDPAddr  netip.AddrPort
	Topic    topic.Info

	done chan struct{}
}

// Done é fechado quando a inscrição deixa de existir: Unsubscribe, queda da
// conexão, encerramento por consumidor lento ou exclusão do tópico. A fila
// associada à inscrição pode ser liberada nesse momento.
func (s Subscription) Done() <-chan struct{} { return s.done }

// Subscriptions responde quais sessões locais recebem eventos de uma versão
// de tópico. Os resultados são cópias e não mudam após o retorno.
type Subscriptions interface {
	ForTopic(ref topic.Ref) []Subscription
	HasLocal(ref topic.Ref) bool
}

type SubscriptionTable struct {
	mu      sync.RWMutex
	byTopic map[topic.Ref]map[SessionID]Subscription
	nextID  SubscriptionID
}

var _ Subscriptions = (*SubscriptionTable)(nil)

func NewSubscriptionTable() *SubscriptionTable {
	return &SubscriptionTable{byTopic: make(map[topic.Ref]map[SessionID]Subscription)}
}

// Add registra a inscrição da sessão no tópico. Repetir o pedido devolve a
// inscrição existente com created falso.
func (t *SubscriptionTable) Add(s Session, info topic.Info) (sub Subscription, created bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	subs := t.byTopic[info.Ref]
	if subs == nil {
		subs = make(map[SessionID]Subscription)
		t.byTopic[info.Ref] = subs
	}
	if cur, ok := subs[s.ID]; ok {
		return cur, false
	}
	t.nextID++
	sub = Subscription{ID: t.nextID, Session: s.ID, Identity: s.Identity, UDPAddr: s.UDPAddr, Topic: info, done: make(chan struct{})}
	subs[s.ID] = sub
	return sub, true
}

func (t *SubscriptionTable) Remove(session SessionID, ref topic.Ref) (Subscription, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	subs := t.byTopic[ref]
	sub, ok := subs[session]
	if !ok {
		return Subscription{}, false
	}
	delete(subs, session)
	if len(subs) == 0 {
		delete(t.byTopic, ref)
	}
	close(sub.done)
	return sub, true
}

// RemoveSession remove todas as inscrições da sessão, ao encerrar a conexão.
func (t *SubscriptionTable) RemoveSession(session SessionID) []Subscription {
	t.mu.Lock()
	defer t.mu.Unlock()

	var removed []Subscription
	for ref, subs := range t.byTopic {
		if sub, ok := subs[session]; ok {
			close(sub.done)
			removed = append(removed, sub)
			delete(subs, session)
			if len(subs) == 0 {
				delete(t.byTopic, ref)
			}
		}
	}
	sortByID(removed)
	return removed
}

// RemoveTopic remove as inscrições de todas as gerações do tópico, ao apagá-lo.
func (t *SubscriptionTable) RemoveTopic(id topic.ID) []Subscription {
	t.mu.Lock()
	defer t.mu.Unlock()

	var removed []Subscription
	for ref, subs := range t.byTopic {
		if ref.ID != id {
			continue
		}
		for _, sub := range subs {
			close(sub.done)
			removed = append(removed, sub)
		}
		delete(t.byTopic, ref)
	}
	sortByID(removed)
	return removed
}

func (t *SubscriptionTable) ForTopic(ref topic.Ref) []Subscription {
	t.mu.RLock()
	subs := t.byTopic[ref]
	out := make([]Subscription, 0, len(subs))
	for _, sub := range subs {
		out = append(out, sub)
	}
	t.mu.RUnlock()

	sortByID(out)
	return out
}

func (t *SubscriptionTable) HasLocal(ref topic.Ref) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.byTopic[ref]) > 0
}

func sortByID(subs []Subscription) {
	sort.Slice(subs, func(i, j int) bool { return subs[i].ID < subs[j].ID })
}
