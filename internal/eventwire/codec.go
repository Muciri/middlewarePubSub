// Package eventwire codifica o cabeçalho UDP fixo do plano de dados.
package eventwire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/franciscovmn/middlewarePubSub/topic"
)

const (
	HeaderSize              = 34
	MaxPayload              = 1024
	Version                 = 1
	FlagPeerForwarded uint8 = 1
)

var (
	ErrInvalidSize = errors.New("eventwire: tamanho inválido")
	ErrVersion     = errors.New("eventwire: versão não suportada")
	ErrInvalidTTL  = errors.New("eventwire: validade inválida")
)

// Header segue a ordem do formato binário: versão, flags, tópico, geração,
// sessão/publicador, sequência, validade em ms e tamanho do conteúdo.
type Header struct {
	Version     uint8
	Flags       uint8
	Topic       topic.ID
	Generation  topic.Generation
	Session     uint64
	Sequence    uint64
	TTL         time.Duration
	PayloadSize uint16
}

func Encode(h Header, payload []byte) ([]byte, error) {
	if len(payload) > MaxPayload {
		return nil, fmt.Errorf("%w: conteúdo tem %d bytes (máximo %d)", ErrInvalidSize, len(payload), MaxPayload)
	}
	if h.Version == 0 {
		h.Version = Version
	}
	if h.Version != Version {
		return nil, ErrVersion
	}
	if h.TTL < time.Millisecond || h.TTL%time.Millisecond != 0 || uint64(h.TTL/time.Millisecond) > uint64(^uint32(0)) {
		return nil, ErrInvalidTTL
	}
	h.PayloadSize = uint16(len(payload))
	out := make([]byte, HeaderSize+len(payload))
	out[0], out[1] = h.Version, h.Flags
	binary.BigEndian.PutUint32(out[2:6], uint32(h.Topic))
	binary.BigEndian.PutUint32(out[6:10], uint32(h.Generation))
	binary.BigEndian.PutUint64(out[10:18], h.Session)
	binary.BigEndian.PutUint64(out[18:26], h.Sequence)
	binary.BigEndian.PutUint32(out[26:30], uint32(h.TTL/time.Millisecond))
	binary.BigEndian.PutUint32(out[30:34], uint32(h.PayloadSize))
	copy(out[HeaderSize:], payload)
	return out, nil
}

func Decode(packet []byte) (Header, []byte, error) {
	if len(packet) < HeaderSize {
		return Header{}, nil, ErrInvalidSize
	}
	h := Header{Version: packet[0], Flags: packet[1], Topic: topic.ID(binary.BigEndian.Uint32(packet[2:6])), Generation: topic.Generation(binary.BigEndian.Uint32(packet[6:10])), Session: binary.BigEndian.Uint64(packet[10:18]), Sequence: binary.BigEndian.Uint64(packet[18:26]), TTL: time.Duration(binary.BigEndian.Uint32(packet[26:30])) * time.Millisecond}
	n := binary.BigEndian.Uint32(packet[30:34])
	if n > MaxPayload || uint64(len(packet)-HeaderSize) != uint64(n) {
		return Header{}, nil, ErrInvalidSize
	}
	if h.Version != Version {
		return Header{}, nil, ErrVersion
	}
	return h, packet[HeaderSize:], nil
}
