// Package testproxy é um proxy TCP para testes que simula falhas de rede
// entre dois processos: corte de conexões, atraso e partição silenciosa.
package testproxy

import (
	"net"
	"sync"
	"time"
)

type Proxy struct {
	ln     net.Listener
	target string

	mu        sync.Mutex
	conns     map[net.Conn]struct{}
	refusing  bool
	blackhole bool
	delay     time.Duration
	closed    bool
	wg        sync.WaitGroup
}

// New escuta numa porta livre de 127.0.0.1 e repassa ao endereço target.
func New(target string) (*Proxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &Proxy{ln: ln, target: target, conns: make(map[net.Conn]struct{})}
	p.wg.Add(1)
	go p.acceptLoop()
	return p, nil
}

func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// Cut encerra as conexões ativas e recusa novas até Restore.
func (p *Proxy) Cut() {
	p.mu.Lock()
	p.refusing = true
	for c := range p.conns {
		c.Close()
	}
	p.mu.Unlock()
}

// Restore volta a aceitar conexões e a repassar dados.
func (p *Proxy) Restore() {
	p.mu.Lock()
	p.refusing = false
	p.blackhole = false
	p.mu.Unlock()
}

// SetDelay atrasa cada trecho repassado, nos dois sentidos.
func (p *Proxy) SetDelay(d time.Duration) {
	p.mu.Lock()
	p.delay = d
	p.mu.Unlock()
}

// Blackhole mantém as conexões abertas, mas descarta tudo o que chega
// enquanto ativo, como numa partição em que nenhum lado recebe RST.
func (p *Proxy) Blackhole(on bool) {
	p.mu.Lock()
	p.blackhole = on
	p.mu.Unlock()
}

func (p *Proxy) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.ln.Close()
	p.Cut()
	p.wg.Wait()
}

func (p *Proxy) acceptLoop() {
	defer p.wg.Done()
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		refuse := p.refusing || p.closed
		p.mu.Unlock()
		if refuse {
			client.Close()
			continue
		}
		server, err := net.Dial("tcp", p.target)
		if err != nil {
			client.Close()
			continue
		}
		p.mu.Lock()
		if p.refusing || p.closed {
			p.mu.Unlock()
			client.Close()
			server.Close()
			continue
		}
		p.conns[client] = struct{}{}
		p.conns[server] = struct{}{}
		p.wg.Add(2)
		p.mu.Unlock()

		go p.pipe(client, server)
		go p.pipe(server, client)
	}
}

type chunk struct {
	data []byte
	due  time.Time
}

// pipe copia de src para dst respeitando o atraso e a partição. A leitura e
// a escrita ficam em goroutines separadas para que o atraso não reduza a
// vazão.
func (p *Proxy) pipe(src, dst net.Conn) {
	defer p.wg.Done()
	queue := make(chan chunk, 1024)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for c := range queue {
			if wait := time.Until(c.due); wait > 0 {
				time.Sleep(wait)
			}
			if _, err := dst.Write(c.data); err != nil {
				return
			}
		}
	}()

	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			p.mu.Lock()
			drop, delay := p.blackhole, p.delay
			p.mu.Unlock()
			if !drop {
				data := append([]byte(nil), buf[:n]...)
				queue <- chunk{data: data, due: time.Now().Add(delay)}
			}
		}
		if err != nil {
			break
		}
	}
	close(queue)
	<-done
	src.Close()
	dst.Close()
	p.mu.Lock()
	delete(p.conns, src)
	delete(p.conns, dst)
	p.mu.Unlock()
}
