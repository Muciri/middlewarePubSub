// Comando sonda é o cliente dos ensaios de falha. Usa só o canal de
// controle: observa inscrições, reconexões e a disponibilidade dos comandos,
// e executa comandos administrativos avulsos. Todo registro sai com horário
// UTC em milissegundos.
//
//	sonda observar -brokers a:7000,b:7000 -topicos sala-1/lances,sala-1/placar=estado -criar
//	sonda criar -brokers a:7000 -topico sala-1/placar -tipo estado
//	sonda apagar -brokers a:7000 -topico sala-1/lances
//	sonda consultar -brokers b:7000 -topico sala-1/lances
//	sonda inscrever -brokers b:7000 -topico sala-1/lances
//	sonda stats -brokers a:7000 -id admin -token ...
//	sonda marcar -texto "injeção: kill -9 no A"
//
// As opções comuns também podem vir de SONDA_BROKERS, SONDA_ID e
// SONDA_TOKEN.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/franciscovmn/middlewarePubSub/client"
	"github.com/franciscovmn/middlewarePubSub/internal/logging"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

func usage() {
	fmt.Fprintln(os.Stderr, "uso: sonda {observar|criar|apagar|consultar|inscrever|stats|marcar} [opções]")
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	common := commonFlags(fs)

	var run func(ctx context.Context, log *slog.Logger, cfg client.Config) error
	switch cmd {
	case "observar":
		topics := fs.String("topicos", "", "tópicos a observar, separados por vírgula; nome=estado para tópico de estado")
		create := fs.Bool("criar", false, "cria os tópicos ausentes antes de se inscrever")
		probe := fs.Duration("sondagem", 500*time.Millisecond, "intervalo da consulta que verifica o canal de comandos")
		run = func(ctx context.Context, log *slog.Logger, cfg client.Config) error {
			specs, err := parseTopics(*topics)
			if err != nil {
				return err
			}
			return observe(ctx, log, cfg, specs, *create, *probe)
		}
	case "criar":
		name := fs.String("topico", "", "nome do tópico")
		kind := fs.String("tipo", "efemero", "efemero ou estado")
		run = func(ctx context.Context, log *slog.Logger, cfg client.Config) error {
			spec, err := parseSpec(*name + "=" + *kind)
			if err != nil {
				return err
			}
			return once(ctx, log, cfg, func(c *client.Client) error {
				info, err := c.CreateTopic(ctx, spec)
				if err == nil {
					log.Info("tópico criado", topicAttrs(info)...)
				}
				return err
			})
		}
	case "apagar":
		name := fs.String("topico", "", "nome do tópico")
		run = func(ctx context.Context, log *slog.Logger, cfg client.Config) error {
			return once(ctx, log, cfg, func(c *client.Client) error {
				err := c.DeleteTopic(ctx, *name)
				if err == nil {
					log.Info("tópico apagado", "topico", *name)
				}
				return err
			})
		}
	case "consultar":
		name := fs.String("topico", "", "nome do tópico")
		run = func(ctx context.Context, log *slog.Logger, cfg client.Config) error {
			return once(ctx, log, cfg, func(c *client.Client) error {
				info, err := c.LookupTopic(ctx, *name)
				if err == nil {
					log.Info("tópico encontrado", topicAttrs(info)...)
				}
				return err
			})
		}
	case "inscrever":
		name := fs.String("topico", "", "nome do tópico")
		run = func(ctx context.Context, log *slog.Logger, cfg client.Config) error {
			return once(ctx, log, cfg, func(c *client.Client) error {
				sub, err := c.Subscribe(ctx, *name)
				if err != nil {
					return err
				}
				log.Info("inscrição aceita", topicAttrs(sub.Topic())...)
				return sub.Unsubscribe(ctx)
			})
		}
	case "marcar":
		text := fs.String("texto", "", "descrição do instante marcado")
		run = func(_ context.Context, log *slog.Logger, _ client.Config) error {
			log.Info("marca", "texto", *text)
			return nil
		}
	case "stats":
		run = func(ctx context.Context, log *slog.Logger, cfg client.Config) error {
			return once(ctx, log, cfg, func(c *client.Client) error {
				s, err := c.Stats(ctx)
				if err == nil {
					log.Info("diagnóstico", "broker_id", s.BrokerID, "uptime", s.Uptime,
						"contadores", s.Counters, "medidores", s.Gauges)
				}
				return err
			})
		}
	default:
		usage()
	}
	fs.Parse(args)

	log, err := logging.New(os.Stderr, common.logFormat, common.logLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	log = log.With("sonda", common.identity)
	cfg := client.Config{
		Brokers:          splitList(common.brokers),
		Identity:         common.identity,
		Token:            common.token,
		HeartbeatTimeout: common.silence,
		Logger:           log,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, log, cfg); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("comando falhou", "comando", cmd, "erro", err)
		os.Exit(1)
	}
}

type common struct {
	brokers, identity, token string
	logFormat, logLevel      string
	silence                  time.Duration
}

func commonFlags(fs *flag.FlagSet) *common {
	c := &common{}
	fs.StringVar(&c.brokers, "brokers", env("SONDA_BROKERS", "127.0.0.1:7000"), "endereços de comando, em ordem de preferência")
	fs.StringVar(&c.identity, "id", env("SONDA_ID", "ana"), "identidade")
	fs.StringVar(&c.token, "token", env("SONDA_TOKEN", ""), "token da identidade")
	fs.DurationVar(&c.silence, "silencio", client.DefaultHeartbeatTimeout, "silêncio do broker que caracteriza falha")
	fs.StringVar(&c.logFormat, "log-format", "text", "formato do log: text ou json")
	fs.StringVar(&c.logLevel, "log-level", "info", "nível do log: debug, info, warn ou error")
	return c
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func parseTopics(s string) ([]topic.Spec, error) {
	var specs []topic.Spec
	for _, item := range splitList(s) {
		spec, err := parseSpec(item)
		if err != nil {
			return nil, err
		}
		specs = append(specs, spec)
	}
	if len(specs) == 0 {
		return nil, errors.New("nenhum tópico informado")
	}
	return specs, nil
}

func parseSpec(item string) (topic.Spec, error) {
	name, kind, _ := strings.Cut(item, "=")
	spec := topic.Spec{Name: name, Kind: topic.KindEphemeral}
	switch kind {
	case "", "efemero":
	case "estado":
		spec.Kind = topic.KindState
	default:
		return spec, fmt.Errorf("tipo de tópico desconhecido em %q", item)
	}
	return spec, spec.Validate()
}

func topicAttrs(info topic.Info) []any {
	return []any{"topico", info.Name, "id", uint32(info.ID), "geracao", uint32(info.Generation)}
}

// dial conecta tentando de novo até conseguir ou ctx terminar, para que a
// sonda possa subir antes dos brokers.
func dial(ctx context.Context, log *slog.Logger, cfg client.Config) (*client.Client, error) {
	for {
		c, err := client.Dial(ctx, cfg)
		if err == nil {
			log.Info("conectada", "broker_id", c.BrokerID())
			return c, nil
		}
		if ctx.Err() != nil || errors.Is(err, client.ErrUnauthenticated) {
			return nil, err
		}
		log.Warn("nenhum broker disponível, nova tentativa em 1s", "erro", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func once(ctx context.Context, log *slog.Logger, cfg client.Config, fn func(*client.Client) error) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, cfg)
	if err != nil {
		return err
	}
	defer c.Close()
	log.Info("conectada", "broker_id", c.BrokerID())
	return fn(c)
}

// observe mantém as inscrições, registra cada reconexão com a lacuna e
// consulta periodicamente o canal de comandos, registrando as transições
// entre disponível e indisponível.
func observe(ctx context.Context, log *slog.Logger, cfg client.Config, specs []topic.Spec, create bool, every time.Duration) error {
	cfg.OnReconnect = func(r client.Reconnect) {
		log.Info("failover concluído", "de", r.From, "para", r.To,
			"deteccao", r.DetectedAt.UTC().Format("15:04:05.000"),
			"restauracao", r.RestoredAt.UTC().Format("15:04:05.000"),
			"lacuna_ms", float64(r.Gap().Microseconds())/1000,
			"tentativas", r.Attempts, "restauradas", r.Restored, "encerradas", r.Ended)
	}
	c, err := dial(ctx, log, cfg)
	if err != nil {
		return err
	}
	defer c.Close()

	o := &observer{log: log, c: c, subs: make(map[string]*client.Subscription)}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		o.ensure(ctx, specs, create)
		o.probe(ctx, specs[0].Name)
		select {
		case <-ctx.Done():
			log.Info("sonda encerrada")
			return nil
		case <-ticker.C:
		}
	}
}

type observer struct {
	log *slog.Logger
	c   *client.Client

	mu   sync.Mutex
	subs map[string]*client.Subscription

	down      bool
	downSince time.Time
	lastErr   string
}

// ensure inscreve nos tópicos ainda sem inscrição ativa. Uma inscrição
// encerrada por tópico apagado só volta se o tópico for recriado.
func (o *observer) ensure(ctx context.Context, specs []topic.Spec, create bool) {
	for _, spec := range specs {
		o.mu.Lock()
		_, active := o.subs[spec.Name]
		o.mu.Unlock()
		if active {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		if create {
			if _, err := o.c.CreateTopic(cctx, spec); err == nil {
				o.log.Info("tópico criado", "topico", spec.Name)
			} else if !errors.Is(err, client.ErrTopicExists) {
				o.log.Debug("criação falhou", "topico", spec.Name, "erro", err)
			}
		}
		sub, err := o.c.Subscribe(cctx, spec.Name)
		cancel()
		if err != nil {
			o.log.Debug("inscrição falhou", "topico", spec.Name, "erro", err)
			continue
		}
		o.log.Info("inscrita", "broker_id", o.c.BrokerID(), "topico", spec.Name,
			"geracao", uint32(sub.Topic().Generation))
		o.mu.Lock()
		o.subs[spec.Name] = sub
		o.mu.Unlock()
		go o.watch(spec.Name, sub)
	}
}

func (o *observer) watch(name string, sub *client.Subscription) {
	var events uint64
	for range sub.Events() {
		events++
	}
	o.log.Warn("inscrição encerrada", "topico", name, "geracao", uint32(sub.Topic().Generation),
		"erro", sub.Err(), "eventos", events)
	o.mu.Lock()
	delete(o.subs, name)
	o.mu.Unlock()
}

// probe registra só as mudanças de disponibilidade do canal de comandos.
func (o *observer) probe(ctx context.Context, name string) {
	cctx, cancel := context.WithTimeout(ctx, time.Second)
	_, err := o.c.LookupTopic(cctx, name)
	cancel()
	if err != nil && errors.Is(err, client.ErrTopicNotFound) {
		err = nil
	}
	switch {
	case err != nil && !o.down:
		o.down, o.downSince, o.lastErr = true, time.Now(), err.Error()
		o.log.Warn("comandos indisponíveis", "broker_id", o.c.BrokerID(), "erro", err)
	case err != nil && err.Error() != o.lastErr:
		o.lastErr = err.Error()
		o.log.Warn("comandos ainda indisponíveis", "broker_id", o.c.BrokerID(), "erro", err)
	case err == nil && o.down:
		o.down = false
		o.log.Info("comandos disponíveis", "broker_id", o.c.BrokerID(),
			"indisponivel_ms", float64(time.Since(o.downSince).Microseconds())/1000)
	}
}
