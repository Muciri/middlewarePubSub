package main

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/franciscovmn/middlewarePubSub/client"
)

type participanteConfig struct {
	lances, reacoes     float64
	rajada              int
	rajadaCada          time.Duration
	inicial, incremento int64
}

var tiposReacao = []string{"palmas", "uau", "risos", "fogo"}

// participante envia lances e reações em taxa constante, com rajadas
// periódicas de reações. Cada lance supera o maior valor que ele conhece,
// vindo do placar ou do próprio último lance.
func participante(ctx context.Context, a *app, cfg *participanteConfig) error {
	var atual atomic.Int64
	go manterInscricao(ctx, a, a.topico("placar"), nil, func(ev client.Event) {
		var p mensagemPlacar
		if json.Unmarshal(ev.Payload, &p) == nil {
			atual.Store(p.Maior)
		}
	})

	pub := newPublicacoes()
	var proprio int64
	lance := func() {
		base := max(atual.Load(), proprio, cfg.inicial)
		proprio = base + cfg.incremento*int64(1+rand.IntN(5))
		pub.publicar(a, a.topico("lances"), mensagemLance{Publicador: a.id, Enviado: agora(), Valor: proprio})
	}
	reacao := func() {
		tipo := tiposReacao[rand.IntN(len(tiposReacao))]
		pub.publicar(a, a.topico("reacoes"), mensagemReacao{Publicador: a.id, Enviado: agora(), Tipo: tipo})
	}

	lances, pararLances := relogio(intervaloDe(cfg.lances))
	defer pararLances()
	reacoes, pararReacoes := relogio(intervaloDe(cfg.reacoes))
	defer pararReacoes()
	var cada time.Duration
	if cfg.rajada > 0 {
		cada = cfg.rajadaCada
	}
	rajadas, pararRajadas := relogio(cada)
	defer pararRajadas()
	resumo := time.NewTicker(a.resumo)
	defer resumo.Stop()

	for {
		select {
		case <-ctx.Done():
			pub.registrar(a)
			return nil
		case <-lances:
			lance()
		case <-reacoes:
			reacao()
		case <-rajadas:
			for range cfg.rajada {
				reacao()
			}
		case <-resumo.C:
			pub.registrar(a)
		}
	}
}

// intervaloDe converte eventos por segundo em intervalo; zero ou negativo
// desativa.
func intervaloDe(taxa float64) time.Duration {
	if taxa <= 0 {
		return 0
	}
	return time.Duration(float64(time.Second) / taxa)
}

// relogio devolve um canal periódico, ou nil (que nunca dispara) quando o
// intervalo é zero.
func relogio(d time.Duration) (<-chan time.Time, func()) {
	if d <= 0 {
		return nil, func() {}
	}
	t := time.NewTicker(d)
	return t.C, t.Stop
}
