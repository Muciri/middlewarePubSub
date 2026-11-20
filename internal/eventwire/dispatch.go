package eventwire

import (
	"time"

	"github.com/franciscovmn/middlewarePubSub/topic"
)

// QueuedEvent guarda um evento já validado e seu prazo monotônico local.
type QueuedEvent struct {
	Header   Header
	Payload  []byte
	Deadline time.Time
}

// Queue define a base comum para filas de entrada e por inscrição.
type Queue interface {
	Push(QueuedEvent) (dropped int, accepted bool)
	Pop() (QueuedEvent, bool)
	Len() int
}

// QueueFactory cria uma fila aplicando a política declarada pelo tópico.
type QueueFactory interface{ NewQueue(topic.Info) Queue }

// Scheduler alterna filas de publicadores, despachando no máximo um evento
// ou maxBytes por turno (o que ocorrer primeiro).
type Scheduler interface {
	Enqueue(identity string, event QueuedEvent) bool
	Next(maxBytes int) (identity string, event QueuedEvent, ok bool)
}
