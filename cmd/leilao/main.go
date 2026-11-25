// Comando leilao é a aplicação de exemplo: um leilão ao vivo organizado em
// salas, com os tópicos <sala>/lances, <sala>/reacoes e <sala>/placar. Usa
// a biblioteca cliente com o plano de dados UDP. Todo registro sai com
// horário UTC em milissegundos.
//
//	leilao leiloeiro    -brokers a:7000,b:7000 -id leiloeiro -token ... -sala sala-1 -criar
//	leilao participante -brokers a:7000,b:7000 -id participante-1 -token ... -sala sala-1
//	leilao espectador   -brokers b:7000,a:7000 -id espectador-1 -token ... -sala sala-1
//
// O leiloeiro cria os tópicos da sala, acompanha os lances e publica o
// placar em intervalo fixo. O participante envia lances acima do placar
// atual e reações, com rajadas periódicas. O espectador recebe os três
// tópicos e registra, a cada resumo, o que chegou de cada publicador.
//
// As opções comuns também podem vir de LEILAO_BROKERS, LEILAO_ID,
// LEILAO_TOKEN e LEILAO_SALA.
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
	"syscall"
	"time"

	"github.com/franciscovmn/middlewarePubSub/client"
	"github.com/franciscovmn/middlewarePubSub/internal/logging"
)

func usage() {
	fmt.Fprintln(os.Stderr, "uso: leilao {leiloeiro|participante|espectador} [opções]")
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	papel, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet(papel, flag.ExitOnError)
	com := flagsComuns(fs)

	var run func(ctx context.Context, a *app) error
	switch papel {
	case "leiloeiro":
		cfg := &leiloeiroConfig{}
		fs.BoolVar(&cfg.criar, "criar", false, "cria os tópicos da sala que ainda não existem")
		fs.DurationVar(&cfg.intervalo, "placar", 100*time.Millisecond, "intervalo entre publicações do placar")
		run = func(ctx context.Context, a *app) error { return leiloeiro(ctx, a, cfg) }
	case "participante":
		cfg := &participanteConfig{}
		fs.Float64Var(&cfg.lances, "lances", 5, "lances por segundo; zero não envia")
		fs.Float64Var(&cfg.reacoes, "reacoes", 5, "reações por segundo; zero não envia")
		fs.IntVar(&cfg.rajada, "rajada", 10, "reações enviadas de uma vez em cada rajada; zero desativa")
		fs.DurationVar(&cfg.rajadaCada, "rajada-cada", 10*time.Second, "intervalo entre rajadas")
		fs.Int64Var(&cfg.inicial, "lance-inicial", 100, "valor de partida dos lances")
		fs.Int64Var(&cfg.incremento, "incremento", 10, "incremento mínimo sobre o maior lance")
		run = func(ctx context.Context, a *app) error { return participante(ctx, a, cfg) }
	case "espectador":
		fs.DurationVar(&com.processamento, "processamento", 0, "atraso artificial por evento, para ensaio de consumidor lento")
		run = func(ctx context.Context, a *app) error { return espectar(ctx, a) }
	default:
		usage()
	}
	fs.Parse(args)

	log, err := logging.New(os.Stderr, com.logFormat, com.logLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	log = log.With("cliente", com.identity)
	cfg := client.Config{
		Brokers:          splitList(com.brokers),
		Identity:         com.identity,
		Token:            com.token,
		HeartbeatTimeout: com.silence,
		Logger:           log,
		OnReconnect:      registrarFailover(log),
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if com.duracao > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, com.duracao)
		defer cancel()
	}

	c, err := conectar(ctx, log, cfg, com.udp)
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			log.Error("conexão falhou", "erro", err)
			os.Exit(1)
		}
		return
	}
	defer c.Close()

	a := &app{log: log, c: c, id: com.identity, sala: com.sala, resumo: com.resumo, processamento: com.processamento}
	log.Info("leilão iniciado", "papel", papel, "sala", com.sala)
	if err := run(ctx, a); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		log.Error("papel encerrado com erro", "papel", papel, "erro", err)
		os.Exit(1)
	}
	log.Info("leilão encerrado", "papel", papel)
}

// app reúne o que os três papéis compartilham.
type app struct {
	log           *slog.Logger
	c             *client.Client
	id            string
	sala          string
	resumo        time.Duration
	processamento time.Duration
}

func (a *app) topico(nome string) string { return a.sala + "/" + nome }

type comuns struct {
	brokers, identity, token, sala, udp     string
	logFormat, logLevel                     string
	silence, resumo, duracao, processamento time.Duration
}

func flagsComuns(fs *flag.FlagSet) *comuns {
	c := &comuns{}
	fs.StringVar(&c.brokers, "brokers", env("LEILAO_BROKERS", "127.0.0.1:7000"), "endereços de comando, em ordem de preferência")
	fs.StringVar(&c.identity, "id", env("LEILAO_ID", ""), "identidade")
	fs.StringVar(&c.token, "token", env("LEILAO_TOKEN", ""), "token da identidade")
	fs.StringVar(&c.sala, "sala", env("LEILAO_SALA", "sala-1"), "sala do leilão, prefixo dos tópicos")
	fs.StringVar(&c.udp, "udp", ":0", "endereço UDP local para receber eventos")
	fs.DurationVar(&c.silence, "silencio", client.DefaultHeartbeatTimeout, "silêncio do broker que caracteriza falha")
	fs.DurationVar(&c.resumo, "resumo", 5*time.Second, "intervalo entre os registros de resumo")
	fs.DurationVar(&c.duracao, "duracao", 0, "encerra depois desse tempo; zero roda até receber um sinal")
	fs.DurationVar(&c.processamento, "processamento", 0, "atraso artificial por evento para ensaio de consumidor lento")
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

// conectar abre o plano de dados UDP e conecta, tentando de novo até
// conseguir ou ctx terminar, para que a aplicação possa subir antes dos
// brokers.
func conectar(ctx context.Context, log *slog.Logger, cfg client.Config, udp string) (*client.Client, error) {
	dp, err := client.ListenUDPDataPlane(udp)
	if err != nil {
		return nil, err
	}
	cfg.DataPlane = dp
	for {
		c, err := client.Dial(ctx, cfg)
		if err == nil {
			log.Info("conectado", "broker_id", c.BrokerID(), "udp_local", dp.Port())
			return c, nil
		}
		if ctx.Err() != nil || errors.Is(err, client.ErrUnauthenticated) {
			dp.Close()
			return nil, err
		}
		log.Warn("nenhum broker disponível, nova tentativa em 1s", "erro", err)
		if !pausa(ctx, time.Second) {
			dp.Close()
			return nil, ctx.Err()
		}
	}
}

func registrarFailover(log *slog.Logger) func(client.Reconnect) {
	return func(r client.Reconnect) {
		log.Info("failover concluído", "de", r.From, "para", r.To,
			"deteccao", hora(r.DetectedAt), "restauracao", hora(r.RestoredAt),
			"lacuna_ms", ms(r.Gap()),
			"tentativas", r.Attempts, "restauradas", r.Restored, "encerradas", r.Ended)
	}
}

// manterInscricao mantém a inscrição no tópico enquanto ctx durar. Tenta de
// novo enquanto o tópico não existe ou o broker não responde, e volta a se
// inscrever quando a inscrição termina, como num tópico apagado e recriado.
// aoInscrever, se definido, recebe cada inscrição nova.
func manterInscricao(ctx context.Context, a *app, nome string, aoInscrever func(*client.Subscription), tratar func(client.Event)) {
	for ctx.Err() == nil {
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		sub, err := a.c.Subscribe(cctx, nome)
		cancel()
		if err != nil {
			if errors.Is(err, client.ErrClosed) {
				return
			}
			a.log.Debug("inscrição falhou", "topico", nome, "erro", err)
			pausa(ctx, time.Second)
			continue
		}
		a.log.Info("inscrito", "broker_id", a.c.BrokerID(), "topico", nome, "geracao", uint32(sub.Topic().Generation))
		if aoInscrever != nil {
			aoInscrever(sub)
		}
		if !drenar(ctx, sub, tratar, a.processamento) {
			return
		}
		a.log.Warn("inscrição encerrada", "topico", nome, "erro", sub.Err())
	}
}

// drenar entrega os eventos até a inscrição terminar (true) ou ctx acabar
// (false).
func drenar(ctx context.Context, sub *client.Subscription, tratar func(client.Event), processamento time.Duration) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case ev, ok := <-sub.Events():
			if !ok {
				return true
			}
			if processamento > 0 && !pausa(ctx, processamento) {
				return false
			}
			tratar(ev)
		}
	}
}

// pausa espera d ou o fim de ctx; retorna false se ctx terminou.
func pausa(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func hora(t time.Time) string { return t.UTC().Format("15:04:05.000") }

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
