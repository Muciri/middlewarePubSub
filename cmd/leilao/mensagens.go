package main

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/franciscovmn/middlewarePubSub/client"
)

// Todas as mensagens levam a identidade de quem publicou (p) e o horário de
// envio em nanossegundos Unix (t). O espectador usa os dois campos para
// nomear o fluxo e medir a latência sem conhecer o tipo da mensagem.
type cabecalho struct {
	Publicador string `json:"p"`
	Enviado    int64  `json:"t"`
}

type mensagemLance struct {
	Publicador string `json:"p"`
	Enviado    int64  `json:"t"`
	Valor      int64  `json:"v"`
}

type mensagemReacao struct {
	Publicador string `json:"p"`
	Enviado    int64  `json:"t"`
	Tipo       string `json:"r"`
}

// mensagemPlacar é o estado completo da sala; cada publicação substitui a
// anterior no tópico de estado.
type mensagemPlacar struct {
	Publicador string `json:"p"`
	Enviado    int64  `json:"t"`
	Sala       string `json:"sala"`
	Maior      int64  `json:"maior"`
	Vencedor   string `json:"de"`
	Lances     uint64 `json:"lances"`
	Versao     uint64 `json:"versao"`
}

func agora() int64 { return time.Now().UnixNano() }

// publicacoes conta, por tópico, os resultados de admissão entre dois
// resumos. Publish não espera a entrega: o resultado chega depois pelo TCP
// e é aguardado numa goroutine. Publish recusado na hora conta como erro.
type publicacoes struct {
	mu       sync.Mutex
	contagem map[string]map[string]uint64
}

func newPublicacoes() *publicacoes {
	return &publicacoes{contagem: make(map[string]map[string]uint64)}
}

func (p *publicacoes) publicar(a *app, topico string, msg any) {
	payload, err := json.Marshal(msg)
	if err != nil {
		p.somar(topico, "erro_outro")
		return
	}
	pend, err := a.c.Publish(topico, payload, 0)
	if err != nil {
		p.somar(topico, motivoErro(err))
		a.log.Debug("publicação recusada", "topico", topico, "erro", err)
		return
	}
	p.somar(topico, "enviadas")
	go func() {
		st, err := pend.Wait(context.Background())
		if err != nil {
			p.somar(topico, "erro_outro")
			return
		}
		p.somar(topico, st.String())
	}()
}

func motivoErro(err error) string {
	switch {
	case errors.Is(err, client.ErrReconnecting):
		return "erro_reconectando"
	case errors.Is(err, client.ErrTooManyPending):
		return "erro_pendentes"
	case errors.Is(err, client.ErrTopicNotFound):
		return "erro_sem_topico"
	case errors.Is(err, client.ErrClosed):
		return "erro_encerrado"
	default:
		return "erro_outro"
	}
}

func (p *publicacoes) somar(topico, chave string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	m := p.contagem[topico]
	if m == nil {
		m = make(map[string]uint64)
		p.contagem[topico] = m
	}
	m[chave]++
}

// registrar escreve uma linha por tópico e zera as contagens. Os
// resultados de uma janela podem chegar na seguinte, porque o prazo do
// resultado é de 1 s.
func (p *publicacoes) registrar(a *app) {
	p.mu.Lock()
	contagem := p.contagem
	p.contagem = make(map[string]map[string]uint64)
	p.mu.Unlock()

	topicos := make([]string, 0, len(contagem))
	for t := range contagem {
		topicos = append(topicos, t)
	}
	sort.Strings(topicos)
	for _, t := range topicos {
		chaves := make([]string, 0, len(contagem[t]))
		for k := range contagem[t] {
			chaves = append(chaves, k)
		}
		sort.Strings(chaves)
		attrs := []any{"topico", t}
		for _, k := range chaves {
			attrs = append(attrs, k, contagem[t][k])
		}
		a.log.Info("publicações", attrs...)
	}
}
