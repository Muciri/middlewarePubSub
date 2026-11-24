package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/franciscovmn/middlewarePubSub/broker"
	"github.com/franciscovmn/middlewarePubSub/client"
)

func TestFluxoContaPerdasEMaiorIntervalo(t *testing.T) {
	base := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	f := &fluxo{nome: "p/lances"}
	f.registrar(1, base, base.Add(2*time.Millisecond))
	f.registrar(2, base.Add(100*time.Millisecond), base.Add(103*time.Millisecond))
	f.registrar(5, base.Add(900*time.Millisecond), base.Add(904*time.Millisecond))
	f.registrar(6, base.Add(1000*time.Millisecond), base.Add(1001*time.Millisecond))

	r := f.fechar()
	if r.recebidos != 4 || r.perdidos != 2 {
		t.Fatalf("recebidos=%d perdidos=%d, esperado 4 e 2", r.recebidos, r.perdidos)
	}
	if r.maiorIntervalo != 801*time.Millisecond ||
		!r.intervaloDe.Equal(base.Add(103*time.Millisecond)) || !r.intervaloAte.Equal(base.Add(904*time.Millisecond)) {
		t.Fatalf("maior intervalo %v de %v até %v", r.maiorIntervalo, r.intervaloDe, r.intervaloAte)
	}
	if r.latMax != 4*time.Millisecond || r.latMedia != 2500*time.Microsecond {
		t.Fatalf("latência média %v, máxima %v", r.latMedia, r.latMax)
	}

	f.registrar(7, time.Time{}, base.Add(3*time.Second))
	r = f.fechar()
	if r.recebidos != 1 || r.perdidos != 0 || r.temLatencia {
		t.Fatalf("segunda janela: %+v", r)
	}
	if r.maiorIntervalo != 1999*time.Millisecond {
		t.Fatalf("lacuna entre janelas %v, esperado 1,999s", r.maiorIntervalo)
	}
	if r = f.fechar(); r.recebidos != 0 || r.maiorIntervalo != 0 || !r.ultima.Equal(base.Add(3*time.Second)) {
		t.Fatalf("janela vazia: %+v", r)
	}
}

func TestFluxoDetectaLacunaPelaCadencia(t *testing.T) {
	base := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	f := &fluxo{nome: "p/reacoes"}
	at := base
	seq := uint64(0)
	evento := func(d time.Duration) (lacuna, bool) {
		at = at.Add(d)
		seq++
		return f.registrar(seq, time.Time{}, at)
	}
	evento(0)
	// Cadência de 200 ms com uma rajada no meio: a rajada não reduz a
	// cadência, porque ela é o maior intervalo recente.
	for i := 0; i < amostrasCadencia; i++ {
		d := 200 * time.Millisecond
		if i%5 == 0 {
			d = time.Millisecond
		}
		if _, ok := evento(d); ok {
			t.Fatalf("intervalo %d de %v virou lacuna", i, d)
		}
	}
	if _, ok := evento(290 * time.Millisecond); ok {
		t.Fatal("290 ms está abaixo de 1,5 vez a cadência")
	}
	seq += 3
	l, ok := evento(5 * time.Second)
	if !ok || l.duracao != 5*time.Second || l.cadencia != 290*time.Millisecond || l.perdidos != 3 {
		t.Fatalf("lacuna %+v, ok=%v", l, ok)
	}
	if !l.ate.Equal(at) || !l.de.Equal(at.Add(-5*time.Second)) {
		t.Fatalf("limites da lacuna %v a %v", l.de, l.ate)
	}
	if f.cadencia() != 290*time.Millisecond {
		t.Fatalf("a lacuna não pode entrar na cadência: %v", f.cadencia())
	}
}

func TestPercentil(t *testing.T) {
	var v []time.Duration
	for i := 1; i <= 20; i++ {
		v = append(v, time.Duration(i)*time.Millisecond)
	}
	if p := percentil(v, 0.95); p != 19*time.Millisecond {
		t.Fatalf("p95 = %v", p)
	}
	if p := percentil(v[:1], 0.95); p != time.Millisecond {
		t.Fatalf("p95 de um valor = %v", p)
	}
}

func TestEstadoLeilaoGuardaMaiorLance(t *testing.T) {
	e := &estadoLeilao{}
	if !e.lance(mensagemLance{Publicador: "p1", Valor: 110}) {
		t.Fatal("primeiro lance deveria ser o maior")
	}
	if e.lance(mensagemLance{Publicador: "p2", Valor: 105}) {
		t.Fatal("lance menor não pode passar o maior")
	}
	e.lance(mensagemLance{Publicador: "p2", Valor: 130})
	p := e.placar("sala-1", "leiloeiro")
	if p.Maior != 130 || p.Vencedor != "p2" || p.Lances != 3 || p.Versao != 1 {
		t.Fatalf("placar %+v", p)
	}
	if e.placar("sala-1", "leiloeiro").Versao != 2 {
		t.Fatal("cada placar deve ter versão nova")
	}
}

func TestIntervaloDe(t *testing.T) {
	if intervaloDe(4) != 250*time.Millisecond || intervaloDe(0) != 0 {
		t.Fatal("conversão de taxa em intervalo")
	}
}

// TestLeilaoComBrokerReal roda os três papéis contra um broker com TCP e
// UDP reais e confere que lances, reações e placar chegam ao espectador.
func TestLeilaoComBrokerReal(t *testing.T) {
	probe, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	port := uint16(probe.LocalAddr().(*net.UDPAddr).Port)
	probe.Close()
	b, err := broker.New(broker.Config{
		ID: "A", EventPort: port,
		Credentials: map[string]broker.Credential{
			"leiloeiro":    {Token: "l", Admin: true},
			"participante": {Token: "p"},
			"espectador":   {Token: "e"},
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go b.Serve(ln)
	t.Cleanup(func() { b.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	novo := func(id, token string) *app {
		log := slog.New(slog.NewTextHandler(io.Discard, nil))
		c, err := conectar(ctx, log, client.Config{
			Brokers: []string{ln.Addr().String()}, Identity: id, Token: token, Logger: log,
		}, "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return &app{log: log, c: c, id: id, sala: "sala-1", resumo: 500 * time.Millisecond}
	}
	leilo, esp, part := novo("leiloeiro", "l"), novo("espectador", "e"), novo("participante", "p")

	if err := criarSala(ctx, leilo); err != nil {
		t.Fatal(err)
	}
	e := newEspectador(esp)
	done := make(chan struct{}, 3)
	go func() {
		leiloeiro(ctx, leilo, &leiloeiroConfig{intervalo: 50 * time.Millisecond})
		done <- struct{}{}
	}()
	go func() {
		for _, spec := range topicosDaSala("sala-1") {
			go manterInscricao(ctx, esp, spec.Name, e.inscrito(spec.Name), e.receber)
		}
		<-ctx.Done()
		done <- struct{}{}
	}()
	go func() {
		participante(ctx, part, &participanteConfig{lances: 20, reacoes: 20, rajada: 5, rajadaCada: 500 * time.Millisecond, inicial: 100, incremento: 10})
		done <- struct{}{}
	}()
	for range 3 {
		<-done
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	for _, nome := range []string{"participante/lances", "participante/reacoes", "leiloeiro/placar"} {
		if e.totais[nome] == 0 {
			t.Errorf("nenhum evento em %s; totais %v", nome, e.totais)
		}
	}
	if e.ultimoMaior <= 100 {
		t.Errorf("placar não refletiu os lances: maior %d", e.ultimoMaior)
	}
}
