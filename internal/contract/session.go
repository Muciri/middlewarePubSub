package contract

import (
	"errors"
	"net/netip"
	"sync"
)

type SessionID uint64

// Session é o vínculo criado pelo Hello. UDPAddr é o IP de origem da conexão
// TCP com a porta UDP informada no Hello; datagramas de outra origem não
// pertencem à sessão.
type Session struct {
	ID       SessionID
	Identity string
	Admin    bool
	UDPAddr  netip.AddrPort
}

// Sessions associa o identificador de sessão do cabeçalho UDP à identidade
// autenticada.
type Sessions interface {
	Get(id SessionID) (Session, bool)
}

var (
	ErrInvalidSession  = errors.New("contract: sessão zero")
	ErrSessionConflict = errors.New("contract: sessão ligada a outra identidade")
)

type SessionTable struct {
	mu        sync.RWMutex
	bindings  map[SessionID]sessionBinding
	nextLease uint64
}

type sessionBinding struct {
	session Session
	lease   uint64
}

var _ Sessions = (*SessionTable)(nil)

func NewSessionTable() *SessionTable {
	return &SessionTable{bindings: make(map[SessionID]sessionBinding)}
}

// Bind liga a sessão à identidade e ao endereço UDP. Uma sessão já ligada a
// outra identidade é recusada; a mesma identidade substitui o vínculo
// anterior. A função devolvida desfaz o vínculo apenas se ele ainda for o
// criado por esta chamada, então a limpeza de uma conexão antiga não remove
// o vínculo de uma conexão mais nova.
func (t *SessionTable) Bind(s Session) (release func(), err error) {
	if s.ID == 0 {
		return nil, ErrInvalidSession
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if cur, ok := t.bindings[s.ID]; ok && cur.session.Identity != s.Identity {
		return nil, ErrSessionConflict
	}
	t.nextLease++
	lease := t.nextLease
	t.bindings[s.ID] = sessionBinding{session: s, lease: lease}

	var once sync.Once
	return func() {
		once.Do(func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			if cur, ok := t.bindings[s.ID]; ok && cur.lease == lease {
				delete(t.bindings, s.ID)
			}
		})
	}, nil
}

func (t *SessionTable) Get(id SessionID) (Session, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	b, ok := t.bindings[id]
	return b.session, ok
}
