package wire

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

var ErrConnBroken = errors.New("wire: conexão encerrada após falha de escrita")

type Options struct {
	MaxFrame int
	// WriteTimeout limita cada Send. Uma escrita que não termina no prazo
	// deixa um quadro parcial no fluxo, por isso a conexão é encerrada.
	// Zero desativa o prazo.
	WriteTimeout time.Duration
	// ReadTimeout limita cada Receive. Zero desativa o prazo.
	ReadTimeout time.Duration
}

// Conn envia e recebe mensagens enquadradas sobre uma net.Conn.
// Send pode ser chamado de várias goroutines; Receive deve ter um único leitor.
type Conn struct {
	nc   net.Conn
	r    *bufio.Reader
	opts Options

	mu       sync.Mutex
	writeErr error
}

func NewConn(nc net.Conn, opts Options) *Conn {
	if opts.MaxFrame <= 0 {
		opts.MaxFrame = MaxClientFrame
	}
	return &Conn{nc: nc, r: bufio.NewReader(nc), opts: opts}
}

func (c *Conn) Send(m proto.Message) error {
	payload, err := proto.Marshal(m)
	if err != nil {
		return fmt.Errorf("wire: serializar: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.writeErr != nil {
		return c.writeErr
	}
	if c.opts.WriteTimeout > 0 {
		if err := c.nc.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout)); err != nil {
			return c.breakLocked(err)
		}
	}
	if err := WriteFrame(c.nc, payload, c.opts.MaxFrame); err != nil {
		if errors.Is(err, ErrFrameTooLarge) {
			return err
		}
		return c.breakLocked(err)
	}
	return nil
}

func (c *Conn) breakLocked(cause error) error {
	c.writeErr = fmt.Errorf("%w: %w", ErrConnBroken, cause)
	c.nc.Close()
	return c.writeErr
}

func (c *Conn) Receive(m proto.Message) error {
	if c.opts.ReadTimeout > 0 {
		if err := c.nc.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout)); err != nil {
			return err
		}
	}
	return ReadMessage(c.r, m, c.opts.MaxFrame)
}

func (c *Conn) RemoteAddr() net.Addr { return c.nc.RemoteAddr() }

func (c *Conn) Close() error { return c.nc.Close() }
