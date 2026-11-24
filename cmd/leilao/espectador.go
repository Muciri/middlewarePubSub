package main

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/franciscovmn/middlewarePubSub/client"
)

// fluxo acompanha os eventos de um publicador num tópico. As perdas vêm dos
// saltos na sequência, que o publicador numera por tópico; incluem eventos
// recusados na admissão, perdidos na rede ou descartados nas filas.
type fluxo struct {
	nome      string
	ultimaSeq uint64
	ultima    time.Time
	janela    janelaFluxo

	// recentes guarda os últimos intervalos normais; a cadência é o maior
	// deles. O máximo, e não a média, evita que as rajadas de reações
	// façam um intervalo comum parecer lacuna.
	recentes       [amostrasCadencia]time.Duration
	nRecentes, pos int
}

// Uma lacuna é um intervalo acima de fatorLacuna vezes a cadência e com ao
// menos margemLacuna a mais, depois de amostrasCadencia intervalos.
const (
	amostrasCadencia = 20
	fatorLacuna      = 1.5
	margemLacuna     = 20 * time.Millisecond
)

// lacuna é um intervalo entre eventos consecutivos bem maior que a cadência
// do fluxo: vai do último evento antes da interrupção ao primeiro depois.
type lacuna struct {
	de, ate           time.Time
	duracao, cadencia time.Duration
	perdidos          uint64
}

type janelaFluxo struct {
	recebidos, perdidos uint64
	latencias           []time.Duration
	maiorIntervalo      time.Duration
	de, ate             time.Time
}

func (f *fluxo) cadencia() time.Duration {
	var c time.Duration
	for _, d := range f.recentes[:f.nRecentes] {
		c = max(c, d)
	}
	return c
}

// registrar conta um evento recebido em chegada, enviado pelo publicador em
// enviado (zero quando a mensagem não traz o horário), e informa se o
// intervalo desde o evento anterior foi uma lacuna. Uma lacuna não entra na
// cadência.
func (f *fluxo) registrar(seq uint64, enviado, chegada time.Time) (lacuna, bool) {
	var saltou uint64
	if f.ultimaSeq != 0 && seq > f.ultimaSeq+1 {
		saltou = seq - f.ultimaSeq - 1
		f.janela.perdidos += saltou
	}
	if seq > f.ultimaSeq {
		f.ultimaSeq = seq
	}
	var l lacuna
	var houve bool
	if !f.ultima.IsZero() {
		d := chegada.Sub(f.ultima)
		if d > f.janela.maiorIntervalo {
			f.janela.maiorIntervalo, f.janela.de, f.janela.ate = d, f.ultima, chegada
		}
		c := f.cadencia()
		if f.nRecentes == amostrasCadencia && d > time.Duration(fatorLacuna*float64(c)) && d > c+margemLacuna {
			l, houve = lacuna{de: f.ultima, ate: chegada, duracao: d, cadencia: c, perdidos: saltou}, true
		} else {
			f.recentes[f.pos] = d
			f.pos = (f.pos + 1) % amostrasCadencia
			f.nRecentes = min(f.nRecentes+1, amostrasCadencia)
		}
	}
	f.ultima = chegada
	f.janela.recebidos++
	if !enviado.IsZero() {
		f.janela.latencias = append(f.janela.latencias, chegada.Sub(enviado))
	}
	return l, houve
}

type resumoFluxo struct {
	recebidos, perdidos       uint64
	temLatencia               bool
	latMedia, latP95          time.Duration
	latMax                    time.Duration
	maiorIntervalo            time.Duration
	intervaloDe, intervaloAte time.Time
	ultima                    time.Time
	cadencia                  time.Duration
}

// fechar resume a janela e começa outra. O maior intervalo é atribuído à
// janela em que termina, então uma lacuna que atravessa o fim da janela
// aparece inteira na seguinte.
func (f *fluxo) fechar() resumoFluxo {
	j := f.janela
	r := resumoFluxo{
		recebidos: j.recebidos, perdidos: j.perdidos,
		maiorIntervalo: j.maiorIntervalo, intervaloDe: j.de, intervaloAte: j.ate,
		ultima: f.ultima, cadencia: f.cadencia(),
	}
	if n := len(j.latencias); n > 0 {
		lat := append([]time.Duration(nil), j.latencias...)
		sort.Slice(lat, func(i, k int) bool { return lat[i] < lat[k] })
		var soma time.Duration
		for _, d := range lat {
			soma += d
		}
		r.temLatencia = true
		r.latMedia = soma / time.Duration(n)
		r.latP95 = percentil(lat, 0.95)
		r.latMax = lat[n-1]
	}
	f.janela = janelaFluxo{latencias: j.latencias[:0]}
	return r
}

// percentil recebe os valores em ordem crescente.
func percentil(ordenados []time.Duration, q float64) time.Duration {
	i := int(float64(len(ordenados))*q+0.999999) - 1
	if i < 0 {
		i = 0
	}
	return ordenados[min(i, len(ordenados)-1)]
}

type chaveFluxo struct {
	sessao uint64
	topico string
}

type inscricaoVista struct {
	sub        *client.Subscription
	descartado uint64
}

// espectador recebe os tópicos da sala e registra, a cada resumo, uma
// linha por fluxo com eventos, perdas, latência, cadência e o maior
// intervalo entre eventos consecutivos. Cada lacuna é registrada na hora,
// quando o evento que a encerra chega.
type espectador struct {
	a *app

	mu          sync.Mutex
	fluxos      map[chaveFluxo]*fluxo
	ordem       []chaveFluxo
	inscr       map[string]*inscricaoVista
	placar      *mensagemPlacar
	eventos     uint64
	totais      map[string]uint64
	ultimoMaior int64
}

func newEspectador(a *app) *espectador {
	return &espectador{
		a:      a,
		fluxos: make(map[chaveFluxo]*fluxo),
		inscr:  make(map[string]*inscricaoVista),
		totais: make(map[string]uint64),
	}
}

func espectar(ctx context.Context, a *app) error {
	e := newEspectador(a)
	for _, spec := range topicosDaSala(a.sala) {
		go manterInscricao(ctx, a, spec.Name, e.inscrito(spec.Name), e.receber)
	}
	resumo := time.NewTicker(a.resumo)
	defer resumo.Stop()
	for {
		select {
		case <-ctx.Done():
			e.resumir()
			return nil
		case <-resumo.C:
			e.resumir()
		}
	}
}

func (e *espectador) inscrito(topico string) func(*client.Subscription) {
	return func(sub *client.Subscription) {
		e.mu.Lock()
		e.inscr[topico] = &inscricaoVista{sub: sub}
		e.mu.Unlock()
	}
}

func (e *espectador) receber(ev client.Event) {
	chegada := time.Now()
	var cab cabecalho
	_ = json.Unmarshal(ev.Payload, &cab)

	e.mu.Lock()
	defer e.mu.Unlock()
	k := chaveFluxo{ev.Session, ev.Topic}
	f := e.fluxos[k]
	if f == nil {
		quem := cab.Publicador
		if quem == "" {
			quem = fmt.Sprintf("%016x", ev.Session)
		}
		f = &fluxo{nome: quem + "/" + path.Base(ev.Topic)}
		e.fluxos[k] = f
		e.ordem = append(e.ordem, k)
	}
	var enviado time.Time
	if cab.Enviado > 0 {
		enviado = time.Unix(0, cab.Enviado)
	}
	if l, ok := f.registrar(ev.Sequence, enviado, chegada); ok {
		e.a.log.Warn("lacuna de eventos", "fluxo", f.nome, "lacuna_ms", ms(l.duracao),
			"de", hora(l.de), "ate", hora(l.ate), "cadencia_ms", ms(l.cadencia), "perdidos", l.perdidos)
	}
	e.eventos++
	e.totais[f.nome]++
	if ev.Topic == e.a.topico("placar") {
		var p mensagemPlacar
		if json.Unmarshal(ev.Payload, &p) == nil {
			e.placar = &p
			e.ultimoMaior = p.Maior
		}
	}
}

func (e *espectador) resumir() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, k := range e.ordem {
		f := e.fluxos[k]
		r := f.fechar()
		attrs := []any{"fluxo", f.nome, "recebidos", r.recebidos, "perdidos", r.perdidos}
		if r.temLatencia {
			attrs = append(attrs, "latencia_media_ms", ms(r.latMedia),
				"latencia_p95_ms", ms(r.latP95), "latencia_max_ms", ms(r.latMax))
		}
		if r.maiorIntervalo > 0 {
			attrs = append(attrs, "maior_intervalo_ms", ms(r.maiorIntervalo),
				"intervalo_de", hora(r.intervaloDe), "intervalo_ate", hora(r.intervaloAte))
		}
		if r.cadencia > 0 {
			attrs = append(attrs, "cadencia_ms", ms(r.cadencia))
		}
		if r.recebidos == 0 {
			attrs = append(attrs, "ultimo", hora(r.ultima))
		}
		e.a.log.Info("fluxo", attrs...)
	}

	var descartados uint64
	for _, v := range e.inscr {
		d := v.sub.Dropped()
		descartados += d - v.descartado
		v.descartado = d
	}
	e.a.log.Info("resumo", "eventos", e.eventos, "descartados_fila", descartados)
	e.eventos = 0
	if p := e.placar; p != nil {
		e.a.log.Info("placar", "maior", p.Maior, "de", p.Vencedor, "lances", p.Lances, "versao", p.Versao)
		e.placar = nil
	}
}
