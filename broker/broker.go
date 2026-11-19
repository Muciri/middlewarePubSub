package broker

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/contract"
	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/peer"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

// Contracts reúne as dependências que o broker oferece ao plano de dados.
type Contracts struct {
	Sessions      contract.Sessions
	Topics        topic.Registry
	Subscriptions contract.Subscriptions
	Interest      contract.InterestView
	Link          contract.LinkState
	Results       contract.ResultSink
}

type Broker struct {
	cfg      Config
	started  time.Time
	catalog  *topic.Catalog
	sessions *contract.SessionTable
	subs     *contract.SubscriptionTable
	interest contract.InterestView
	linkView contract.LinkState
	// link é nil sem par configurado.
	link *peer.Link

	// topicMu impede que uma inscrição seja registrada numa versão de
	// tópico que está sendo apagada.
	topicMu sync.RWMutex
	// adminMu serializa as alterações do catálogo no broker administrativo.
	adminMu sync.Mutex
	// interestMu serializa as mudanças de interesse local e seus anúncios
	// ao par. Ordem de aquisição: interestMu antes de topicMu.
	interestMu sync.Mutex

	// helloMu serializa a admissão de sessões.
	helloMu sync.Mutex

	mu        sync.Mutex
	closed    bool
	listeners map[net.Listener]struct{}
	raw       map[net.Conn]struct{}
	conns     map[contract.SessionID]*clientConn
	wg        sync.WaitGroup
}

var _ contract.ResultSink = (*Broker)(nil)

var ErrClosed = errors.New("broker: encerrado")

// New cria o broker. Com par configurado como administrativo, a discagem
// ao par começa aqui; o outro broker passa a aceitar o par em ServePeer.
func New(cfg Config) (*Broker, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	b := &Broker{
		cfg:       cfg,
		started:   time.Now(),
		catalog:   topic.NewCatalog(),
		sessions:  contract.NewSessionTable(),
		subs:      contract.NewSubscriptionTable(),
		interest:  standaloneInterest{},
		linkView:  &contract.MemoryLink{},
		listeners: make(map[net.Listener]struct{}),
		raw:       make(map[net.Conn]struct{}),
		conns:     make(map[contract.SessionID]*clientConn),
	}
	if cfg.Peer != nil {
		pc := *cfg.Peer
		pc.LocalID = cfg.ID
		link, err := peer.New(pc, peerStore{b})
		if err != nil {
			return nil, err
		}
		b.link = link
		b.interest = link.Interest()
		b.linkView = link
		link.Start()
	}
	return b, nil
}

var ErrNoPeer = errors.New("broker: sem par configurado")

// ServePeer aceita a conexão do broker administrativo até Close.
func (b *Broker) ServePeer(ln net.Listener) error {
	if b.link == nil {
		ln.Close()
		return ErrNoPeer
	}
	return b.link.Serve(ln)
}

func (b *Broker) ListenAndServePeer(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return b.ServePeer(ln)
}

func (b *Broker) Contracts() Contracts {
	return Contracts{
		Sessions:      b.sessions,
		Topics:        b.catalog,
		Subscriptions: b.subs,
		Interest:      b.interest,
		Link:          b.linkView,
		Results:       b,
	}
}

// Serve aceita conexões até Close.
func (b *Broker) Serve(ln net.Listener) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		ln.Close()
		return ErrClosed
	}
	b.listeners[ln] = struct{}{}
	b.mu.Unlock()

	for {
		nc, err := ln.Accept()
		if err != nil {
			b.mu.Lock()
			closed := b.closed
			delete(b.listeners, ln)
			b.mu.Unlock()
			if closed {
				return nil
			}
			return err
		}
		if !b.track(nc) {
			nc.Close()
			return nil
		}
		go b.handle(nc)
	}
}

func (b *Broker) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return b.Serve(ln)
}

func (b *Broker) track(nc net.Conn) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return false
	}
	b.raw[nc] = struct{}{}
	b.wg.Add(1)
	return true
}

func (b *Broker) untrack(nc net.Conn) {
	b.mu.Lock()
	delete(b.raw, nc)
	b.mu.Unlock()
	b.wg.Done()
}

// Close encerra os listeners e todas as conexões, e espera a limpeza.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	for ln := range b.listeners {
		ln.Close()
	}
	for nc := range b.raw {
		nc.Close()
	}
	b.mu.Unlock()
	if b.link != nil {
		b.link.Close()
	}
	b.wg.Wait()
	return nil
}

// Report envia o resultado de admissão à conexão da sessão sem bloquear.
func (b *Broker) Report(r contract.AdmissionResult) {
	b.push(r.Session, &pb.Envelope{Body: &pb.Envelope_PublishResult{PublishResult: &pb.PublishResult{
		TopicId:    uint32(r.Topic.ID),
		Generation: uint32(r.Topic.Generation),
		Sequence:   r.Sequence,
		Status:     r.Status,
	}}})
}

func (b *Broker) push(session contract.SessionID, env *pb.Envelope) bool {
	b.mu.Lock()
	c := b.conns[session]
	b.mu.Unlock()
	return c != nil && c.push(env)
}

// standaloneInterest é a visão de interesse sem par configurado: sempre
// recente e vazia, pois não há assinantes remotos possíveis.
type standaloneInterest struct{}

func (standaloneInterest) RemoteInterest(topic.Ref) contract.Interest {
	return contract.Interest{UpdatedAt: time.Now()}
}
