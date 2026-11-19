// Package client é a biblioteca usada pelas aplicações publicadoras e
// consumidoras. Ela escolhe um broker, mantém o canal TCP de controle e o
// UDP de eventos, e reconecta ao outro broker em caso de falha.
package client

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/franciscovmn/middlewarePubSub/internal/pb"
	"github.com/franciscovmn/middlewarePubSub/internal/protoconv"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

const (
	DefaultPublishTimeout   = time.Second
	DefaultHeartbeatTimeout = 5 * time.Second
	DefaultMaxPending       = 4096
)

type Config struct {
	// Brokers lista os endereços TCP de comando (porta 7000) em ordem de
	// preferência. A biblioteca usa um por vez.
	Brokers  []string
	Identity string
	Token    string

	// PublishTimeout é o prazo para o resultado de admissão; esgotado,
	// o resultado é UNKNOWN. Zero usa DefaultPublishTimeout.
	PublishTimeout time.Duration
	// HeartbeatTimeout é o silêncio no canal de controle que caracteriza
	// falha do broker. Zero usa DefaultHeartbeatTimeout.
	HeartbeatTimeout time.Duration
	// MaxPending limita as publicações sem resultado. Zero usa
	// DefaultMaxPending.
	MaxPending int

	// DataPlane é o transporte UDP de eventos. Nil usa um MemoryDataPlane
	// sem porta, que guarda os datagramas sem enviá-los.
	DataPlane DataPlane
}

var ErrInvalidConfig = errors.New("client: configuração inválida")

func (c Config) withDefaults() (Config, error) {
	switch {
	case len(c.Brokers) == 0:
		return c, fmt.Errorf("%w: nenhum broker", ErrInvalidConfig)
	case c.Identity == "":
		return c, fmt.Errorf("%w: identidade vazia", ErrInvalidConfig)
	case c.PublishTimeout < 0 || c.HeartbeatTimeout < 0 || c.MaxPending < 0:
		return c, fmt.Errorf("%w: valores negativos", ErrInvalidConfig)
	}
	for i, addr := range c.Brokers {
		if addr == "" {
			return c, fmt.Errorf("%w: broker %d sem endereço", ErrInvalidConfig, i)
		}
	}
	if c.PublishTimeout == 0 {
		c.PublishTimeout = DefaultPublishTimeout
	}
	if c.HeartbeatTimeout == 0 {
		c.HeartbeatTimeout = DefaultHeartbeatTimeout
	}
	if c.MaxPending == 0 {
		c.MaxPending = DefaultMaxPending
	}
	if c.DataPlane == nil {
		c.DataPlane = NewMemoryDataPlane(0)
	}
	return c, nil
}

var (
	ErrClosed            = errors.New("client: cliente encerrado")
	ErrConnectionLost    = errors.New("client: canal de controle perdido")
	ErrNoBroker          = errors.New("client: nenhum broker disponível")
	ErrTooManyPending    = errors.New("client: limite de publicações pendentes atingido")
	ErrAlreadySubscribed = errors.New("client: já inscrito no tópico")
	ErrTopicDeleted      = errors.New("client: tópico apagado")

	ErrUnauthenticated = errors.New("client: autenticação recusada")
	ErrForbidden       = errors.New("client: operação não permitida para a identidade")
	ErrSessionConflict = errors.New("client: sessão ligada a outra identidade")
	ErrInvalidArgument = errors.New("client: argumento inválido")
	ErrTopicExists     = errors.New("client: tópico já existe")
	ErrTopicNotFound   = errors.New("client: tópico não encontrado")
	ErrUnavailable     = errors.New("client: estado global indisponível")
	ErrLimitExceeded   = errors.New("client: cota de comandos excedida")
	ErrBroker          = errors.New("client: erro interno do broker")
)

func statusError(st pb.Status, detail string) error {
	var base error
	switch st {
	case pb.Status_STATUS_OK:
		return nil
	case pb.Status_STATUS_UNAUTHENTICATED:
		base = ErrUnauthenticated
	case pb.Status_STATUS_FORBIDDEN:
		base = ErrForbidden
	case pb.Status_STATUS_SESSION_CONFLICT:
		base = ErrSessionConflict
	case pb.Status_STATUS_INVALID_ARGUMENT:
		base = ErrInvalidArgument
	case pb.Status_STATUS_TOPIC_EXISTS:
		base = ErrTopicExists
	case pb.Status_STATUS_TOPIC_NOT_FOUND:
		base = ErrTopicNotFound
	case pb.Status_STATUS_UNAVAILABLE:
		base = ErrUnavailable
	case pb.Status_STATUS_LIMIT_EXCEEDED:
		base = ErrLimitExceeded
	default:
		base = ErrBroker
	}
	if detail == "" {
		return base
	}
	return fmt.Errorf("%w: %s", base, detail)
}

type Client struct {
	cfg     Config
	dp      DataPlane
	session uint64
	conn    *controlConn
	pending *pendingTable

	mu       sync.Mutex
	err      error // ErrClosed ou ErrConnectionLost
	topics   map[string]topic.Info
	byName   map[string]*Subscription
	byRef    map[topic.Ref]*Subscription
	sequence map[topic.Ref]uint64
}

// Dial autentica no primeiro broker disponível da lista. Recusa de
// credenciais encerra a tentativa sem passar ao próximo broker.
func Dial(ctx context.Context, cfg Config) (*Client, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	session, err := newSessionID()
	if err != nil {
		return nil, err
	}

	var conn *controlConn
	var errs []error
	for _, addr := range cfg.Brokers {
		conn, err = dialControl(ctx, addr, cfg, session, cfg.DataPlane.Port())
		if err == nil {
			break
		}
		if ctx.Err() != nil || errors.Is(err, ErrUnauthenticated) ||
			errors.Is(err, ErrInvalidArgument) || errors.Is(err, ErrSessionConflict) {
			return nil, err
		}
		errs = append(errs, fmt.Errorf("%s: %w", addr, err))
	}
	if conn == nil {
		return nil, fmt.Errorf("%w: %w", ErrNoBroker, errors.Join(errs...))
	}

	c := &Client{
		cfg:      cfg,
		dp:       cfg.DataPlane,
		session:  session,
		conn:     conn,
		pending:  newPendingTable(cfg.MaxPending, cfg.PublishTimeout),
		topics:   make(map[string]topic.Info),
		byName:   make(map[string]*Subscription),
		byRef:    make(map[topic.Ref]*Subscription),
		sequence: make(map[topic.Ref]uint64),
	}
	c.dp.SetBroker(conn.udpAddr)
	c.dp.SetDeliver(c.deliver)
	conn.start(c.handlePush, c.handleLost)
	return c, nil
}

func newSessionID() (uint64, error) {
	var b [8]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			return 0, err
		}
		if id := binary.BigEndian.Uint64(b[:]); id != 0 {
			return id, nil
		}
	}
}

// BrokerID identifica o broker atual.
func (c *Client) BrokerID() string { return c.conn.brokerID }

func (c *Client) state() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *Client) command(ctx context.Context, env *pb.Envelope) (*pb.Envelope, error) {
	if err := c.state(); err != nil {
		return nil, err
	}
	return c.conn.call(ctx, env)
}

func (c *Client) CreateTopic(ctx context.Context, spec topic.Spec) (topic.Info, error) {
	if err := spec.Validate(); err != nil {
		return topic.Info{}, fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	env, err := c.command(ctx, &pb.Envelope{Body: &pb.Envelope_CreateTopic{CreateTopic: protoconv.SpecToProto(spec)}})
	if err != nil {
		return topic.Info{}, err
	}
	reply := env.GetCreateTopicReply()
	return c.topicReply(reply.GetStatus(), reply.GetDetail(), reply.GetTopic())
}

func (c *Client) DeleteTopic(ctx context.Context, name string) error {
	env, err := c.command(ctx, &pb.Envelope{Body: &pb.Envelope_DeleteTopic{DeleteTopic: &pb.DeleteTopic{Name: name}}})
	if err != nil {
		return err
	}
	c.mu.Lock()
	delete(c.topics, name)
	c.mu.Unlock()
	reply := env.GetDeleteTopicReply()
	return statusError(reply.GetStatus(), reply.GetDetail())
}

func (c *Client) LookupTopic(ctx context.Context, name string) (topic.Info, error) {
	env, err := c.command(ctx, &pb.Envelope{Body: &pb.Envelope_LookupTopic{LookupTopic: &pb.LookupTopic{Name: name}}})
	if err != nil {
		return topic.Info{}, err
	}
	reply := env.GetLookupTopicReply()
	info, err := c.topicReply(reply.GetStatus(), reply.GetDetail(), reply.GetTopic())
	if errors.Is(err, ErrTopicNotFound) {
		c.mu.Lock()
		delete(c.topics, name)
		c.mu.Unlock()
	}
	return info, err
}

// topicReply converte a resposta e atualiza o cache de tópicos.
func (c *Client) topicReply(st pb.Status, detail string, m *pb.TopicInfo) (topic.Info, error) {
	if err := statusError(st, detail); err != nil {
		return topic.Info{}, err
	}
	info, err := protoconv.TopicInfoFromProto(m)
	if err != nil {
		return topic.Info{}, fmt.Errorf("%w: %w", ErrBroker, err)
	}
	c.mu.Lock()
	c.topics[info.Name] = info
	c.mu.Unlock()
	return info, nil
}

// Subscribe registra a inscrição no broker atual. O retorno ocorre depois
// que o par confirma a inscrição.
func (c *Client) Subscribe(ctx context.Context, name string) (*Subscription, error) {
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		return nil, err
	}
	if _, ok := c.byName[name]; ok {
		c.mu.Unlock()
		return nil, ErrAlreadySubscribed
	}
	c.byName[name] = nil // reserva o nome durante o pedido
	c.mu.Unlock()

	sub, err := c.subscribe(ctx, name)

	c.mu.Lock()
	defer c.mu.Unlock()
	if err == nil && c.err != nil {
		err = c.err
		sub.finish(err)
	}
	if err != nil {
		delete(c.byName, name)
		return nil, err
	}
	c.byName[name] = sub
	c.byRef[sub.info.Ref] = sub
	return sub, nil
}

func (c *Client) subscribe(ctx context.Context, name string) (*Subscription, error) {
	env, err := c.conn.call(ctx, &pb.Envelope{Body: &pb.Envelope_Subscribe{Subscribe: &pb.Subscribe{Name: name}}})
	if err != nil {
		return nil, err
	}
	reply := env.GetSubscribeReply()
	info, err := c.topicReply(reply.GetStatus(), reply.GetDetail(), reply.GetTopic())
	if err != nil {
		return nil, err
	}
	return newSubscription(c, info), nil
}

// unsubscribe pede a remoção ao broker. Tópico inexistente também encerra
// a inscrição local, pois não há mais o que receber.
func (c *Client) unsubscribe(ctx context.Context, s *Subscription) error {
	env, err := c.command(ctx, &pb.Envelope{Body: &pb.Envelope_Unsubscribe{Unsubscribe: &pb.Unsubscribe{
		Name: s.info.Name, Generation: uint32(s.info.Generation),
	}}})
	if err != nil {
		return err
	}
	reply := env.GetUnsubscribeReply()
	if err := statusError(reply.GetStatus(), reply.GetDetail()); err != nil && !errors.Is(err, ErrTopicNotFound) {
		return err
	}
	c.endSubscription(s, nil)
	return nil
}

// endSubscription tira a inscrição dos índices e fecha seu canal.
func (c *Client) endSubscription(s *Subscription, err error) {
	c.mu.Lock()
	if c.byName[s.info.Name] == s {
		delete(c.byName, s.info.Name)
	}
	if c.byRef[s.info.Ref] == s {
		delete(c.byRef, s.info.Ref)
	}
	c.mu.Unlock()
	s.finish(err)
}

// Publish envia o evento por UDP e retorna sem aguardar a admissão. ttl zero
// usa a validade máxima do tópico. Com MaxPending publicações sem resultado,
// falha imediatamente com ErrTooManyPending. Se o tópico não estiver em
// cache, é consultado no broker com o prazo PublishTimeout.
func (c *Client) Publish(topicName string, payload []byte, ttl time.Duration) (*PendingPublish, error) {
	if ttl < 0 {
		return nil, fmt.Errorf("%w: validade negativa", ErrInvalidArgument)
	}
	info, err := c.cachedTopic(topicName)
	if err != nil {
		return nil, err
	}
	if ttl == 0 {
		ttl = info.Policy.MaxTTL
	}

	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		return nil, err
	}
	seq := c.sequence[info.Ref] + 1
	p, err := c.pending.add(info.Ref, seq)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.sequence[info.Ref] = seq
	c.mu.Unlock()

	err = c.dp.Send(Datagram{Topic: info.Ref, Session: c.session, Sequence: seq, TTL: ttl, Payload: payload})
	if err != nil {
		c.pending.resolve(info.Ref, seq, StatusUnknown)
		return nil, err
	}
	return p, nil
}

func (c *Client) cachedTopic(name string) (topic.Info, error) {
	c.mu.Lock()
	info, ok := c.topics[name]
	c.mu.Unlock()
	if ok {
		return info, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.PublishTimeout)
	defer cancel()
	return c.LookupTopic(ctx, name)
}

type Stats struct {
	BrokerID string
	Uptime   time.Duration
	Counters map[string]uint64
	Gauges   map[string]int64
}

// Stats consulta o diagnóstico do broker atual; exige identidade
// administrativa.
func (c *Client) Stats(ctx context.Context) (Stats, error) {
	env, err := c.command(ctx, &pb.Envelope{Body: &pb.Envelope_GetStats{GetStats: &pb.GetStats{}}})
	if err != nil {
		return Stats{}, err
	}
	reply := env.GetGetStatsReply()
	if err := statusError(reply.GetStatus(), reply.GetDetail()); err != nil {
		return Stats{}, err
	}
	s := Stats{
		BrokerID: reply.GetBrokerId(),
		Uptime:   time.Duration(reply.GetUptimeMs()) * time.Millisecond,
		Counters: reply.GetCounters(),
		Gauges:   reply.GetGauges(),
	}
	if s.Counters == nil {
		s.Counters = map[string]uint64{}
	}
	if s.Gauges == nil {
		s.Gauges = map[string]int64{}
	}
	return s, nil
}

// Close encerra o canal de controle e o plano de dados. Publicações
// pendentes terminam com UNKNOWN e as inscrições fecham com ErrClosed.
func (c *Client) Close() error {
	if !c.shutdown(ErrClosed) {
		return nil
	}
	c.conn.close()
	return c.dp.Close()
}

// shutdown marca o cliente como inutilizável e encerra pendências.
// Devolve falso se já estava encerrado.
func (c *Client) shutdown(cause error) bool {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return false
	}
	c.err = cause
	subs := make([]*Subscription, 0, len(c.byRef))
	for _, s := range c.byRef {
		subs = append(subs, s)
	}
	c.byRef = make(map[topic.Ref]*Subscription)
	c.byName = make(map[string]*Subscription)
	c.mu.Unlock()

	c.pending.resolveAll(StatusUnknown)
	for _, s := range subs {
		s.finish(cause)
	}
	return true
}

func (c *Client) handleLost() {
	c.shutdown(ErrConnectionLost)
}

func (c *Client) handlePush(env *pb.Envelope) {
	switch body := env.Body.(type) {
	case *pb.Envelope_PublishResult:
		r := body.PublishResult
		ref := topic.Ref{ID: topic.ID(r.GetTopicId()), Generation: topic.Generation(r.GetGeneration())}
		status := statusFromProto(r.GetStatus())
		c.pending.resolve(ref, r.GetSequence(), status)
		if status == StatusTopicNotFound {
			c.forgetTopic(ref)
		}
	case *pb.Envelope_SubscriptionEnded:
		m := body.SubscriptionEnded.GetTopic()
		ref := topic.Ref{ID: topic.ID(m.GetId()), Generation: topic.Generation(m.GetGeneration())}
		c.forgetTopic(ref)
		c.mu.Lock()
		s := c.byRef[ref]
		c.mu.Unlock()
		if s != nil {
			c.endSubscription(s, ErrTopicDeleted)
		}
	}
}

// forgetTopic tira do cache a versão de tópico que deixou de valer.
func (c *Client) forgetTopic(ref topic.Ref) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, info := range c.topics {
		if info.Ref == ref {
			delete(c.topics, name)
		}
	}
}

// deliver recebe os eventos do plano de dados e os coloca na fila da
// inscrição correspondente.
func (c *Client) deliver(e Event) {
	c.mu.Lock()
	s := c.byRef[e.Ref]
	c.mu.Unlock()
	if s == nil {
		return
	}
	e.Topic = s.info.Name
	s.queue.push(e)
}
