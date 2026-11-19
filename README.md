# middlewarePubSub

Middleware Publish/Subscribe em Go com dois brokers (A e B), feito para a
disciplina de Programação Distribuída (IFPB). A especificação está em
`docs/Etapa1-middlewarePubSub.pdf`.

Portas de cada broker:

- 7000/TCP: comandos dos clientes (sessão, tópicos, inscrições);
- 7001/UDP: eventos;
- 7002/TCP: canal entre os brokers.

O canal de controle e o canal entre brokers estão implementados. O plano de
dados UDP ainda está em integração; até lá, publicações e entrega de eventos
dependem das interfaces em `internal/contract` e `client/dataplane.go`.

## Organização

| Pasta | Conteúdo |
|---|---|
| `proto` | mensagens Protobuf (cliente e broker, broker e broker) |
| `internal/wire` | enquadramento por tamanho das mensagens no TCP |
| `topic` | tópicos, gerações e catálogo versionado |
| `peer` | canal entre brokers: snapshot, replicação e heartbeat |
| `broker` | servidor do broker |
| `client` | biblioteca cliente, com troca de broker em caso de falha |
| `cmd/broker`, `cmd/sonda` | executáveis do broker e da sonda de ensaio |
| `scripts` | roteiros dos ensaios de falha |
| `docs` | decisões de implementação e ensaios |

## Compilar e testar

Requer Go 1.23. Para regenerar o Protobuf, também `protoc` e
`protoc-gen-go`.

```
make build      # bin/broker e bin/sonda
make test       # go test -race ./...
make proto      # regenera internal/pb
```

## Executar os dois brokers

Copie `config/credentials.example.json` para `config/credentials.json` e
troque os tokens. Depois, em dois terminais:

```
# B espera o A no canal entre brokers
bin/broker -id B -listen :7100 -udp-port 7101 -peer-id A -peer-listen :7002 -peer-token segredo

# A disca o B
bin/broker -id A -peer-id B -peer-admin -peer-addr 127.0.0.1:7002 -peer-token segredo
```

`bin/broker -h` lista as demais opções. Sem `-peer-id`, o broker roda
sozinho.

No macOS, a porta 7000 costuma estar ocupada pelo Receptor AirPlay. Desative
o receptor nos Ajustes do Sistema ou use outra porta no A (`-listen :7200`).

Exemplo com a sonda:

```
bin/sonda criar -brokers 127.0.0.1:7000 -id leiloeiro -token <token> -topico sala-1/lances
bin/sonda observar -brokers 127.0.0.1:7000,127.0.0.1:7100 -id espectador-1 -token <token> -topicos sala-1/lances
```

## Biblioteca cliente

```go
c, err := client.Dial(ctx, client.Config{
	Brokers:  []string{"127.0.0.1:7000", "127.0.0.1:7100"},
	Identity: "espectador-1",
	Token:    "...",
})
sub, err := c.Subscribe(ctx, "sala-1/lances")
```

Se o broker cair, a biblioteca passa para o próximo da lista, mantém a
sessão e restaura as inscrições.

## Ensaios

Os ensaios de falha rodam em contêineres (`make imagem`, depois
`scripts/ensaio.sh cenario <n>`). Procedimentos e resultados estão em
`docs/ensaios.md`. As decisões tomadas durante a implementação estão em
`docs/decisoes.md`.
