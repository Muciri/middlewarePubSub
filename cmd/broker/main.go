// Comando broker inicia um broker atendendo o canal de controle dos clientes.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/franciscovmn/middlewarePubSub/broker"
)

func main() {
	var (
		id         = flag.String("id", "A", "identificador do broker")
		listen     = flag.String("listen", fmt.Sprintf(":%d", broker.DefaultControlPort), "endereço TCP de comandos")
		eventPort  = flag.Uint("udp-port", broker.DefaultEventPort, "porta UDP de eventos anunciada aos clientes")
		credsPath  = flag.String("credentials", "config/credentials.json", "arquivo JSON de credenciais")
		restricted = flag.Bool("restrict-topic-admin", false, "limita criação e exclusão de tópicos a administradores")
	)
	flag.Parse()

	if *eventPort == 0 || *eventPort > 65535 {
		log.Fatalf("porta UDP inválida: %d", *eventPort)
	}
	creds, err := broker.LoadCredentials(*credsPath)
	if err != nil {
		log.Fatal(err)
	}
	b, err := broker.New(broker.Config{
		ID:                 *id,
		EventPort:          uint16(*eventPort),
		Credentials:        creds,
		RestrictTopicAdmin: *restricted,
	})
	if err != nil {
		log.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		b.Close()
	}()

	log.Printf("broker %s ouvindo comandos em %s", *id, *listen)
	if err := b.ListenAndServe(*listen); err != nil {
		log.Fatal(err)
	}
	log.Printf("broker %s encerrado", *id)
}
