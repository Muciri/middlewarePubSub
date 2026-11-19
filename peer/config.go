// Package peer implementa o canal TCP entre os dois brokers (porta 7002):
// replicação do catálogo, repasse de alterações ao broker administrativo,
// troca de interesse e heartbeat.
package peer

import (
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

const (
	DefaultPort              = 7002
	DefaultHeartbeatInterval = 250 * time.Millisecond
	DefaultDeadAfter         = 5 * time.Second
	DefaultAckTimeout        = 2 * time.Second
	DefaultBackoffMin        = 100 * time.Millisecond
	DefaultBackoffMax        = 5 * time.Second
)

type Config struct {
	LocalID string
	PeerID  string
	// Admin indica o broker administrativo (A): ele disca o par, é a fonte
	// do catálogo e recebe as alterações repassadas. O outro broker escuta.
	Admin bool
	// DialAddr é o endereço do canal entre brokers do par, usado pelo
	// broker administrativo.
	DialAddr string
	// Token é um segredo compartilhado opcional conferido no PeerHello.
	Token string
	// EventPort é a porta UDP local informada ao par no handshake.
	EventPort uint16
	// PeerUDPOverride, se válido, substitui o endereço UDP aprendido do par.
	PeerUDPOverride netip.AddrPort

	HeartbeatInterval time.Duration
	// DeadAfter é o silêncio que caracteriza o par como fora.
	DeadAfter  time.Duration
	AckTimeout time.Duration
	BackoffMin time.Duration
	BackoffMax time.Duration
}

var ErrInvalidConfig = errors.New("peer: configuração inválida")

func (c Config) withDefaults() (Config, error) {
	switch {
	case c.LocalID == "" || c.PeerID == "":
		return c, fmt.Errorf("%w: identificadores vazios", ErrInvalidConfig)
	case c.LocalID == c.PeerID:
		return c, fmt.Errorf("%w: par com o mesmo identificador", ErrInvalidConfig)
	case c.Admin && c.DialAddr == "":
		return c, fmt.Errorf("%w: broker administrativo sem endereço do par", ErrInvalidConfig)
	case c.HeartbeatInterval < 0 || c.DeadAfter < 0 || c.AckTimeout < 0 || c.BackoffMin < 0 || c.BackoffMax < 0:
		return c, fmt.Errorf("%w: valores negativos", ErrInvalidConfig)
	}
	set := func(d *time.Duration, def time.Duration) {
		if *d == 0 {
			*d = def
		}
	}
	set(&c.HeartbeatInterval, DefaultHeartbeatInterval)
	set(&c.DeadAfter, DefaultDeadAfter)
	set(&c.AckTimeout, DefaultAckTimeout)
	set(&c.BackoffMin, DefaultBackoffMin)
	set(&c.BackoffMax, DefaultBackoffMax)
	if c.HeartbeatInterval >= c.DeadAfter {
		return c, fmt.Errorf("%w: heartbeat deve ser menor que o limite de silêncio", ErrInvalidConfig)
	}
	if c.BackoffMin > c.BackoffMax {
		return c, fmt.Errorf("%w: recuo mínimo maior que o máximo", ErrInvalidConfig)
	}
	return c, nil
}

type OpKind uint8

const (
	OpCreate OpKind = 1
	OpDelete OpKind = 2
)

// Update é uma alteração do catálogo já decidida pelo broker
// administrativo. Em OpDelete, Topic é a versão removida.
type Update struct {
	OperationID string
	Version     uint64
	Kind        OpKind
	Topic       topic.Info
}

// Request é um pedido de alteração recebido pelo broker não administrativo.
// Spec vale para OpCreate e Name para OpDelete.
type Request struct {
	OperationID string
	Kind        OpKind
	Spec        topic.Spec
	Name        string
}

// Result é a resposta do broker administrativo a um Request.
type Result struct {
	Topic  topic.Info
	Status pb.Status
	Detail string
}

// Store é o estado local que o enlace lê e altera; o broker o implementa.
type Store interface {
	// CatalogSnapshot é chamado no broker administrativo a cada conexão e,
	// no outro, quando o administrativo sobe sem catálogo.
	CatalogSnapshot() topic.Snapshot
	// CatalogVersion é chamado nos dois; zero no administrativo indica que
	// ele subiu sem catálogo e deve adotar o do par. ApplyUpdate é chamado
	// no broker não administrativo; ApplySnapshot nele e no administrativo,
	// ao adotar o catálogo do par.
	CatalogVersion() uint64
	ApplyUpdate(u Update) error
	ApplySnapshot(s topic.Snapshot) error
	// WithLocalInterest chama fn com as versões de tópico que têm
	// assinantes locais, sem que o interesse local mude durante a chamada.
	WithLocalInterest(fn func(refs []topic.Ref))
	// Execute trata, no broker administrativo, um pedido repassado.
	Execute(req Request) Result
}
