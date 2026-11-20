// Comando ensaio executa uma carga local reprodutível sobre sockets TCP/UDP reais.
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/franciscovmn/middlewarePubSub/broker"
	"github.com/franciscovmn/middlewarePubSub/client"
	"github.com/franciscovmn/middlewarePubSub/topic"
)

func main() {
	dir := flag.String("saida", "ensaios/resultados/local", "diretório dos resultados")
	n := flag.Int("eventos", 1000, "número de publicações")
	rate := flag.Int("taxa", 100, "eventos por segundo")
	flag.Parse()
	if *n < 1 || *rate < 1 {
		log.Fatal("eventos e taxa devem ser positivos")
	}
	if err := os.MkdirAll(*dir, 0755); err != nil {
		log.Fatal(err)
	}
	udpProbe, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero})
	port := uint16(udpProbe.LocalAddr().(*net.UDPAddr).Port)
	udpProbe.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	b, err := broker.New(broker.Config{ID: "ensaio", EventPort: port, Credentials: map[string]broker.Credential{"admin": {Token: "admin", Admin: true}, "load": {Token: "load"}}})
	if err != nil {
		log.Fatal(err)
	}
	go b.Serve(ln)
	defer b.Close()
	dial := func(id, token string) (*client.Client, error) {
		dp, e := client.ListenUDPDataPlane("127.0.0.1:0")
		if e != nil {
			return nil, e
		}
		c, e := client.Dial(context.Background(), client.Config{Brokers: []string{ln.Addr().String()}, Identity: id, Token: token, DataPlane: dp})
		if e != nil {
			dp.Close()
		}
		return c, e
	}
	admin, err := dial("admin", "admin")
	if err != nil {
		log.Fatal(err)
	}
	defer admin.Close()
	pub, err := dial("load", "load")
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()
	subc, err := dial("load", "load")
	if err != nil {
		log.Fatal(err)
	}
	defer subc.Close()
	info, err := admin.CreateTopic(context.Background(), topic.Spec{Name: "ensaio/carga", Kind: topic.KindEphemeral, MaxTTL: 5 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	sub, err := subc.Subscribe(context.Background(), info.Name)
	if err != nil {
		log.Fatal(err)
	}
	var delivered atomic.Uint64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range sub.Events() {
			delivered.Add(1)
		}
	}()
	f, e := os.Create(filepath.Join(*dir, "carga.csv"))
	if e != nil {
		log.Fatal(e)
	}
	w := csv.NewWriter(f)
	w.Write([]string{"sequence", "payload_bytes", "admission_ms", "status", "elapsed_s", "memory_alloc_bytes", "cpu_user_s", "dropped"})
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	cpu0 := cpuSeconds()
	cpuSample, cpuSampleAt := cpu0, time.Now()
	start := time.Now()
	payload := make([]byte, 256)
	interval := time.Second / time.Duration(*rate)
	var lats []float64
	for i := 0; i < *n; i++ {
		target := start.Add(time.Duration(i) * interval)
		if d := time.Until(target); d > 0 {
			time.Sleep(d)
		}
		t0 := time.Now()
		p, e := pub.Publish(info.Name, payload, 0)
		if e != nil {
			log.Fatal(e)
		}
		status, e := p.Wait(context.Background())
		lat := float64(time.Since(t0).Microseconds()) / 1000
		if e != nil {
			log.Fatal(e)
		}
		lats = append(lats, lat)
		if time.Since(cpuSampleAt) >= time.Second {
			cpuSample = cpuSeconds()
			cpuSampleAt = time.Now()
		}
		runtime.ReadMemStats(&m1)
		w.Write([]string{strconv.Itoa(i + 1), strconv.Itoa(len(payload)), fmt.Sprintf("%.3f", lat), status.String(), fmt.Sprintf("%.3f", time.Since(start).Seconds()), strconv.FormatUint(m1.Alloc-m0.Alloc, 10), fmt.Sprintf("%.6f", cpuSample-cpu0), strconv.FormatUint(sub.Dropped(), 10)})
	}
	w.Flush()
	if err = f.Close(); err != nil {
		log.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	loadDelivered := delivered.Load()
	loadElapsed := time.Since(start)
	cpuDelta := cpuSeconds() - cpu0
	stats, _ := admin.Stats(context.Background())
	writeSummary(filepath.Join(*dir, "resumo.txt"), *n, loadDelivered, loadElapsed, lats, m0, m1, cpuDelta, sub.Dropped(), stats.Counters)
	writeSVG(filepath.Join(*dir, "metricas.svg"), *n, loadDelivered, loadElapsed, m0, m1, cpuDelta, sub.Dropped(), lats)
	time.Sleep(1100 * time.Millisecond) // recarrega a rajada do token bucket antes do ensaio de tamanho
	mtuSweep(filepath.Join(*dir, "mtu.csv"), pub, info.Name)
	writeInterfaceMTU(filepath.Join(*dir, "mtu-interface.txt"))
	sub.Unsubscribe(context.Background())
	<-done
	fmt.Printf("resultados: %s; eventos_de_carga=%d/%d taxa_alvo=%d/s\n", *dir, loadDelivered, *n, *rate)
}

func writeInterfaceMTU(path string) {
	f, e := os.Create(path)
	if e != nil {
		return
	}
	defer f.Close()
	ifs, _ := net.Interfaces()
	for _, i := range ifs {
		if i.Flags&net.FlagLoopback != 0 && i.MTU > 0 {
			fmt.Fprintf(f, "interface=%s\nmtu_bytes=%d\nmax_ipv4_udp_payload_without_ip_fragmentation=%d\n", i.Name, i.MTU, i.MTU-28)
			return
		}
	}
	for _, i := range ifs {
		if i.Flags&net.FlagUp != 0 && i.Flags&net.FlagLoopback == 0 && i.MTU > 0 {
			fmt.Fprintf(f, "interface=%s\nmtu_bytes=%d\nmax_ipv4_udp_payload_without_ip_fragmentation=%d\n", i.Name, i.MTU, i.MTU-28)
			return
		}
	}
	fmt.Fprintln(f, "loopback_interface=unavailable")
}

func mtuSweep(path string, c *client.Client, name string) {
	f, e := os.Create(path)
	if e != nil {
		return
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"payload_bytes", "status", "latency_ms"})
	for _, n := range []int{0, 64, 256, 512, 768, 900, 1024} {
		p, e := c.Publish(name, make([]byte, n), 0)
		if e != nil {
			w.Write([]string{strconv.Itoa(n), e.Error(), ""})
			continue
		}
		t := time.Now()
		s, _ := p.Wait(context.Background())
		w.Write([]string{strconv.Itoa(n), s.String(), fmt.Sprintf("%.3f", float64(time.Since(t).Microseconds())/1000)})
	}
}
func writeSummary(path string, n int, d uint64, elapsed time.Duration, l []float64, a, b runtime.MemStats, cpu float64, dropped uint64, counters map[string]uint64) {
	f, _ := os.Create(path)
	defer f.Close()
	avg := 0.0
	for _, v := range l {
		avg += v
	}
	if len(l) > 0 {
		avg /= float64(len(l))
	}
	fmt.Fprintf(f, "events=%d\ndelivered=%d\nelapsed_s=%.3f\nthroughput_events_s=%.2f\nadmission_mean_ms=%.3f\nadmission_p50_ms=%.3f\nadmission_p95_ms=%.3f\nadmission_p99_ms=%.3f\nalloc_delta_bytes=%d\ncpu_user_s=%.6f\ndropped=%d\nbroker_counters=%v\n", n, d, elapsed.Seconds(), float64(n)/elapsed.Seconds(), avg, percentile(l, 0.50), percentile(l, 0.95), percentile(l, 0.99), int64(b.Alloc)-int64(a.Alloc), cpu, dropped, counters)
}
func writeSVG(path string, n int, d uint64, elapsed time.Duration, a, b runtime.MemStats, cpu float64, dropped uint64, l []float64) {
	f, _ := os.Create(path)
	defer f.Close()
	fmt.Fprint(f, `<svg xmlns="http://www.w3.org/2000/svg" width="900" height="560"><rect width="100%" height="100%" fill="white"/><text x="24" y="30" font-size="20">Ensaio local Pub/Sub - Marco 4</text>`)
	vals := []struct {
		name string
		v    float64
		unit string
	}{{"Throughput", float64(n) / elapsed.Seconds(), "eventos/s"}, {"Latência média de admissão", mean(l), "ms"}, {"Memória alocada delta", float64(int64(b.Alloc)-int64(a.Alloc)) / 1048576, "MiB"}, {"CPU user", cpu, "s"}, {"Eventos entregues", float64(d), "eventos"}, {"Descartes na inscrição", float64(dropped), "eventos"}}
	for i, x := range vals {
		y := 75 + i*78
		bar := x.v * 5
		if bar > 550 {
			bar = 550
		}
		fmt.Fprintf(f, `<text x="25" y="%d" font-size="15">%s: %.2f %s</text><rect x="300" y="%d" width="%.1f" height="24" fill="#2878b5"/>`, y, x.name, x.v, x.unit, y-18, bar)
	}
	fmt.Fprint(f, "</svg>")
}
func mean(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	var s float64
	for _, x := range v {
		s += x
	}
	return s / float64(len(v))
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	index := int(float64(len(ordered))*p+0.999999) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(ordered) {
		index = len(ordered) - 1
	}
	return ordered[index]
}
