package client

import (
	"fmt"

	"github.com/franciscovmn/middlewarePubSub/internal/pb"
)

// Status é o resultado da admissão de uma publicação. ACCEPTED não confirma
// entrega ao consumidor.
type Status uint8

const (
	StatusAccepted      Status = 1
	StatusNoSubscribers Status = 2
	StatusTopicNotFound Status = 3
	StatusLimitExceeded Status = 4
	StatusOverloaded    Status = 5
	StatusInvalidSize   Status = 6
	StatusInvalidTTL    Status = 7
	StatusUnavailable   Status = 8
	StatusUnknown       Status = 9
)

var statusNames = [...]string{
	StatusAccepted:      "ACCEPTED",
	StatusNoSubscribers: "NO_SUBSCRIBERS",
	StatusTopicNotFound: "TOPIC_NOT_FOUND",
	StatusLimitExceeded: "LIMIT_EXCEEDED",
	StatusOverloaded:    "OVERLOADED",
	StatusInvalidSize:   "INVALID_SIZE",
	StatusInvalidTTL:    "INVALID_TTL",
	StatusUnavailable:   "UNAVAILABLE",
	StatusUnknown:       "UNKNOWN",
}

func (s Status) String() string {
	if int(s) < len(statusNames) && statusNames[s] != "" {
		return statusNames[s]
	}
	return fmt.Sprintf("Status(%d)", uint8(s))
}

// statusFromProto converte o status recebido do broker; valores fora do
// conjunto conhecido viram UNKNOWN.
func statusFromProto(s pb.AdmissionStatus) Status {
	if s <= 0 || int(s) >= len(statusNames) {
		return StatusUnknown
	}
	return Status(s)
}
