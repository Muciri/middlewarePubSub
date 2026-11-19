// Comando broker inicia um broker atendendo o canal de controle dos clientes
// e, se configurado, o canal com o outro broker.
//
// Broker administrativo (A):
//
//	broker -id A -peer-id B -peer-admin -peer-addr hostB:7002
//
// Outro broker (B):
//
//	broker -id B -peer-id A -peer-listen :7002
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/franciscovmn/middlewarePubSub/broker"
	"github.com/franciscovmn/middlewarePubSub/peer"
)

func main() {
	var (
		id         = flag.String("id", "A", "identificador do broker")
		listen     = flag.String("listen", fmt.Sprintf(":%d", broker.DefaultControlPort), "endereço TCP de comandos")
		eventPort  = flag.Uint("udp-port", broker.DefaultEventPort, "porta UDP de eventos anunciada aos clientes")
		credsPath  = flag.String("credentials", "config/credentials.json", "arquivo JSON de credenciais")
		restricted = flag.Bool("restrict-topic-admin", false, "limita criação e exclusão de tópicos a administradores")

		peerID     = flag.String("peer-id", "", "identificador do outro broker; vazio opera sem par")
		peerAdmin  = flag.Bool("peer-admin", false, "este é o broker administrativo, que disca o par")
		peerAddr   = flag.String("peer-addr", "", "endereço do canal entre brokers do par (broker administrativo)")
		peerListen = flag.String("peer-listen", fmt.Sprintf(":%d", peer.DefaultPort), "endereço do canal entre brokers (outro broker)")
		peerToken  = flag.String("peer-token", "", "segredo compartilhado entre os brokers")
	)
	flag.Parse()

	if *eventPort == 0 || *eventPort > 65535 {
		log.Fatalf("porta UDP inválida: %d", *eventPort)
	}
	creds, err := broker.LoadCredentials(*credsPath)
	if err != nil {
		log.Fatal(err)
	}
	cfg := broker.Config{
		ID:                 *id,
		EventPort:          uint16(*eventPort),
		Credentials:        creds,
		RestrictTopicAdmin: *restricted,
	}
	if *peerID != "" {
		cfg.Peer = &peer.Config{PeerID: *peerID, Admin: *peerAdmin, DialAddr: *peerAddr, Token: *peerToken}
	}
	b, err := broker.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		b.Close()
	}()

	if cfg.Peer != nil && !cfg.Peer.Admin {
		go func() {
			log.Printf("broker %s aguardando o par em %s", *id, *peerListen)
			if err := b.ListenAndServePeer(*peerListen); err != nil {
				log.Fatal(err)
			}
		}()
	}
	log.Printf("broker %s ouvindo comandos em %s", *id, *listen)
	if err := b.ListenAndServe(*listen); err != nil {
		log.Fatal(err)
	}
	log.Printf("broker %s encerrado", *id)
}
