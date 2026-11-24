package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/franciscovmn/middlewarePubSub/client"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

type leiloeiroConfig struct {
	criar     bool
	intervalo time.Duration
}

func topicosDaSala(sala string) []topic.Spec {
	return []topic.Spec{
		{Name: sala + "/lances", Kind: topic.KindEphemeral},
		{Name: sala + "/reacoes", Kind: topic.KindEphemeral},
		{Name: sala + "/placar", Kind: topic.KindState},
	}
}

// estadoLeilao guarda o maior lance da sala. O placar publicado é uma cópia
// desse estado.
type estadoLeilao struct {
	mu       sync.Mutex
	maior    int64
	vencedor string
	lances   uint64
	versao   uint64
}

// lance conta o lance e informa se ele passou a ser o maior.
func (e *estadoLeilao) lance(l mensagemLance) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lances++
	if l.Valor > e.maior {
		e.maior, e.vencedor = l.Valor, l.Publicador
		return true
	}
	return false
}

// placar devolve o estado atual com a versão seguinte.
func (e *estadoLeilao) placar(sala, id string) mensagemPlacar {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.versao++
	return mensagemPlacar{
		Publicador: id, Enviado: agora(), Sala: sala,
		Maior: e.maior, Vencedor: e.vencedor, Lances: e.lances, Versao: e.versao,
	}
}

func (e *estadoLeilao) copia() (maior int64, vencedor string, lances, versao uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.maior, e.vencedor, e.lances, e.versao
}

// leiloeiro acompanha os lances e publica o placar em intervalo fixo. A
// publicação periódica mantém o placar atual mesmo para quem acabou de se
// inscrever ou perdeu atualizações, e serve de fluxo regular para medir a
// lacuna de eventos.
func leiloeiro(ctx context.Context, a *app, cfg *leiloeiroConfig) error {
	if cfg.criar {
		if err := criarSala(ctx, a); err != nil {
			return err
		}
	}
	estado := &estadoLeilao{}
	go manterInscricao(ctx, a, a.topico("lances"), nil, func(ev client.Event) {
		var l mensagemLance
		if err := json.Unmarshal(ev.Payload, &l); err != nil {
			a.log.Debug("lance ilegível", "erro", err)
			return
		}
		estado.lance(l)
	})

	pub := newPublicacoes()
	placar := time.NewTicker(cfg.intervalo)
	defer placar.Stop()
	resumo := time.NewTicker(a.resumo)
	defer resumo.Stop()
	for {
		select {
		case <-ctx.Done():
			pub.registrar(a)
			return nil
		case <-placar.C:
			pub.publicar(a, a.topico("placar"), estado.placar(a.sala, a.id))
		case <-resumo.C:
			maior, vencedor, lances, versao := estado.copia()
			a.log.Info("leilão", "maior", maior, "de", vencedor, "lances", lances, "versao", versao)
			pub.registrar(a)
		}
	}
}

// criarSala cria os tópicos da sala. Um tópico que já existe é aceito; as
// demais falhas, como o enlace entre brokers fora, são tentadas de novo.
func criarSala(ctx context.Context, a *app) error {
	for _, spec := range topicosDaSala(a.sala) {
		for {
			cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			info, err := a.c.CreateTopic(cctx, spec)
			cancel()
			if err == nil {
				a.log.Info("tópico criado", "topico", info.Name, "id", uint32(info.ID), "geracao", uint32(info.Generation))
				break
			}
			if errors.Is(err, client.ErrTopicExists) {
				a.log.Info("tópico já existe", "topico", spec.Name)
				break
			}
			if errors.Is(err, client.ErrClosed) {
				return err
			}
			a.log.Warn("criação do tópico falhou, nova tentativa em 1s", "topico", spec.Name, "erro", err)
			if !pausa(ctx, time.Second) {
				return ctx.Err()
			}
		}
	}
	return nil
}
