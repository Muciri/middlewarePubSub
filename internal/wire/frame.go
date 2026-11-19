// Package wire implementa o enquadramento das mensagens TCP: cada quadro é
// um prefixo de 4 bytes big-endian com o tamanho, seguido do conteúdo.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
)

const headerSize = 4

// Limites de quadro por canal.
const (
	MaxClientFrame = 64 << 10 // porta 7000
	MaxPeerFrame   = 1 << 20  // porta 7002, comporta o snapshot do catálogo
)

var ErrFrameTooLarge = errors.New("wire: quadro acima do limite")

// WriteFrame escreve cabeçalho e conteúdo numa única chamada a w.Write.
func WriteFrame(w io.Writer, payload []byte, maxSize int) error {
	if len(payload) > maxSize {
		return fmt.Errorf("%w: %d > %d bytes", ErrFrameTooLarge, len(payload), maxSize)
	}
	buf := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint32(buf, uint32(len(payload)))
	copy(buf[headerSize:], payload)
	_, err := w.Write(buf)
	return err
}

// ReadFrame lê um quadro completo. O tamanho é verificado antes da alocação,
// então um prefixo forjado não reserva memória além de maxSize.
// Um fluxo encerrado entre quadros devolve io.EOF; no meio de um quadro,
// io.ErrUnexpectedEOF.
func ReadFrame(r io.Reader, maxSize int) ([]byte, error) {
	var hdr [headerSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if uint64(n) > uint64(maxSize) {
		return nil, fmt.Errorf("%w: %d > %d bytes", ErrFrameTooLarge, n, maxSize)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return payload, nil
}

func WriteMessage(w io.Writer, m proto.Message, maxSize int) error {
	payload, err := proto.Marshal(m)
	if err != nil {
		return fmt.Errorf("wire: serializar: %w", err)
	}
	return WriteFrame(w, payload, maxSize)
}

func ReadMessage(r io.Reader, m proto.Message, maxSize int) error {
	payload, err := ReadFrame(r, maxSize)
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(payload, m); err != nil {
		return fmt.Errorf("wire: desserializar: %w", err)
	}
	return nil
}
