// Package topic define identificador, geração, validade e política de fila
// dos tópicos, além do catálogo em memória.
package topic

import (
	"errors"
	"fmt"
	"math"
	"time"
)

type ID uint32

type Generation uint32

// Ref identifica uma versão específica de um tópico. Eventos e inscrições de
// uma geração anterior não valem para o tópico recriado.
type Ref struct {
	ID         ID
	Generation Generation
}

type Kind uint8

const (
	KindEphemeral Kind = 1 // reações e lances
	KindState     Kind = 2 // placar
)

func (k Kind) Valid() bool { return k == KindEphemeral || k == KindState }

func (k Kind) String() string {
	switch k {
	case KindEphemeral:
		return "ephemeral"
	case KindState:
		return "state"
	}
	return fmt.Sprintf("Kind(%d)", uint8(k))
}

// Overflow define o que acontece quando a fila de uma inscrição está cheia.
type Overflow uint8

const (
	DropOldest     Overflow = 1
	ReplacePending Overflow = 2
)

func (o Overflow) String() string {
	switch o {
	case DropOldest:
		return "drop-oldest"
	case ReplacePending:
		return "replace-pending"
	}
	return fmt.Sprintf("Overflow(%d)", uint8(o))
}

const (
	DefaultEphemeralTTL    = 500 * time.Millisecond
	DefaultStateTTL        = 2 * time.Second
	EphemeralQueueCapacity = 256
	StateQueueCapacity     = 1

	MaxNameLength = 128
	maxTTL        = time.Duration(math.MaxUint32) * time.Millisecond
)

type Policy struct {
	MaxTTL        time.Duration
	QueueCapacity int
	Overflow      Overflow
}

// DefaultPolicy devolve a política padrão do tipo, ou a política zero para
// um tipo inválido.
func DefaultPolicy(k Kind) Policy {
	switch k {
	case KindEphemeral:
		return Policy{MaxTTL: DefaultEphemeralTTL, QueueCapacity: EphemeralQueueCapacity, Overflow: DropOldest}
	case KindState:
		return Policy{MaxTTL: DefaultStateTTL, QueueCapacity: StateQueueCapacity, Overflow: ReplacePending}
	}
	return Policy{}
}

var (
	ErrInvalidName = errors.New("topic: nome inválido")
	ErrInvalidKind = errors.New("topic: tipo inválido")
	ErrInvalidTTL  = errors.New("topic: validade inválida")
)

// ValidateName aceita de 1 a MaxNameLength bytes entre a-z, 0-9, '_', '-' e '/'.
func ValidateName(name string) error {
	if len(name) == 0 || len(name) > MaxNameLength {
		return fmt.Errorf("%w: tamanho %d fora de 1..%d", ErrInvalidName, len(name), MaxNameLength)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		ok := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '/'
		if !ok {
			return fmt.Errorf("%w: caractere %q na posição %d", ErrInvalidName, c, i)
		}
	}
	return nil
}

// Spec descreve o pedido de criação de um tópico. MaxTTL zero usa a
// validade padrão do tipo.
type Spec struct {
	Name   string
	Kind   Kind
	MaxTTL time.Duration
}

func (s Spec) Validate() error {
	if err := ValidateName(s.Name); err != nil {
		return err
	}
	if !s.Kind.Valid() {
		return fmt.Errorf("%w: %v", ErrInvalidKind, s.Kind)
	}
	if s.MaxTTL < 0 || s.MaxTTL > maxTTL || (s.MaxTTL > 0 && s.MaxTTL < time.Millisecond) {
		return fmt.Errorf("%w: %v", ErrInvalidTTL, s.MaxTTL)
	}
	return nil
}

func (s Spec) Policy() Policy {
	p := DefaultPolicy(s.Kind)
	if s.MaxTTL > 0 {
		p.MaxTTL = s.MaxTTL
	}
	return p
}

type Info struct {
	Ref
	Name   string
	Kind   Kind
	Policy Policy
}

// AcceptsTTL informa se a validade de um evento cabe na configuração do
// tópico; fora dela, a admissão responde INVALID_TTL.
func (i Info) AcceptsTTL(ttl time.Duration) bool {
	return ttl > 0 && ttl <= i.Policy.MaxTTL
}
