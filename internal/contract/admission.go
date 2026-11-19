package contract

import (
	"sync"

	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

type AdmissionStatus = pb.AdmissionStatus

const (
	AdmissionAccepted      = pb.AdmissionStatus_ADMISSION_ACCEPTED
	AdmissionNoSubscribers = pb.AdmissionStatus_ADMISSION_NO_SUBSCRIBERS
	AdmissionTopicNotFound = pb.AdmissionStatus_ADMISSION_TOPIC_NOT_FOUND
	AdmissionLimitExceeded = pb.AdmissionStatus_ADMISSION_LIMIT_EXCEEDED
	AdmissionOverloaded    = pb.AdmissionStatus_ADMISSION_OVERLOADED
	AdmissionInvalidSize   = pb.AdmissionStatus_ADMISSION_INVALID_SIZE
	AdmissionInvalidTTL    = pb.AdmissionStatus_ADMISSION_INVALID_TTL
	AdmissionUnavailable   = pb.AdmissionStatus_ADMISSION_UNAVAILABLE
	AdmissionUnknown       = pb.AdmissionStatus_ADMISSION_UNKNOWN
)

// AdmissionResult é a decisão sobre um evento, identificado pela sessão,
// pela versão do tópico e pela sequência do cabeçalho UDP.
type AdmissionResult struct {
	Session  SessionID
	Topic    topic.Ref
	Sequence uint64
	Status   AdmissionStatus
}

// ResultSink entrega o resultado à conexão TCP da sessão. Report não
// bloqueia: sem conexão ativa ou com o canal saturado, o resultado é
// descartado e a biblioteca cliente conclui UNKNOWN pelo prazo.
type ResultSink interface {
	Report(r AdmissionResult)
}

type MemoryResultSink struct {
	mu      sync.Mutex
	results []AdmissionResult
}

var _ ResultSink = (*MemoryResultSink)(nil)

func (m *MemoryResultSink) Report(r AdmissionResult) {
	m.mu.Lock()
	m.results = append(m.results, r)
	m.mu.Unlock()
}

func (m *MemoryResultSink) Results() []AdmissionResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]AdmissionResult(nil), m.results...)
}
