package testproxy

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func echoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadBytes('\n')
					if err != nil {
						return
					}
					c.Write(line)
				}
			}()
		}
	}()
	return ln.Addr().String()
}

func newProxy(t *testing.T) *Proxy {
	t.Helper()
	p, err := New(echoServer(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func roundTrip(t *testing.T, c net.Conn, r *bufio.Reader) (time.Duration, error) {
	t.Helper()
	start := time.Now()
	if _, err := c.Write([]byte("oi\n")); err != nil {
		return 0, err
	}
	c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, err := r.ReadBytes('\n'); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

func dialProxy(t *testing.T, p *Proxy) (net.Conn, *bufio.Reader) {
	t.Helper()
	c, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, bufio.NewReader(c)
}

func TestForwardsAndDelays(t *testing.T) {
	p := newProxy(t)
	c, r := dialProxy(t, p)
	if _, err := roundTrip(t, c, r); err != nil {
		t.Fatal(err)
	}
	p.SetDelay(40 * time.Millisecond)
	rtt, err := roundTrip(t, c, r)
	if err != nil {
		t.Fatal(err)
	}
	if rtt < 80*time.Millisecond {
		t.Fatalf("ida e volta de %v com atraso de 40 ms em cada sentido", rtt)
	}
}

func TestCutAndRestore(t *testing.T) {
	p := newProxy(t)
	c, r := dialProxy(t, p)
	p.Cut()
	if _, err := roundTrip(t, c, r); err == nil {
		t.Fatal("conexão deveria ter sido cortada")
	}

	refused, _ := dialProxy(t, p)
	refused.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, err := refused.Read(make([]byte, 1)); err == nil {
		t.Fatal("nova conexão deveria ser recusada durante o corte")
	}

	p.Restore()
	c2, r2 := dialProxy(t, p)
	if _, err := roundTrip(t, c2, r2); err != nil {
		t.Fatalf("após Restore: %v", err)
	}
}

func TestBlackholeDropsSilently(t *testing.T) {
	p := newProxy(t)
	c, r := dialProxy(t, p)
	p.Blackhole(true)
	if _, err := roundTrip(t, c, r); err == nil {
		t.Fatal("dados deveriam ser descartados")
	}
	p.Blackhole(false)
	c.SetReadDeadline(time.Time{})
	if _, err := roundTrip(t, c, r); err != nil {
		t.Fatalf("após fim da partição: %v", err)
	}
}
