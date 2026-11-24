# middlewarePubSub

Middleware Publish/Subscribe em Go com dois brokers (A e B), feito para a
disciplina de Programação Distribuída (IFPB). A especificação está em
`docs/Etapa1-middlewarePubSub.pdf`.

Portas de cada broker:

- 7000/TCP: comandos dos clientes (sessão, tópicos, inscrições);
- 7001/UDP: eventos;
- 7002/TCP: canal entre os brokers.

Comandos e sincronização entre os brokers usam TCP; os eventos usam UDP,
com cabeçalho binário fixo de 34 bytes e até 1.024 bytes de conteúdo. Um
evento só é encaminhado ao outro broker quando ele tem inscritos no tópico.

## Organização

| Pasta | Conteúdo |
|---|---|
| `proto` | mensagens Protobuf (cliente e broker, broker e broker) |
| `internal/wire` | enquadramento por tamanho das mensagens no TCP |
| `internal/eventwire` | cabeçalho UDP dos eventos, filas limitadas e rodízio entre publicadores |
| `topic` | tópicos, gerações e catálogo versionado |
| `peer` | canal entre brokers: snapshot, replicação e heartbeat |
| `broker` | servidor do broker: TCP de comandos, UDP de eventos, cotas e encaminhamento ao par |
| `client` | biblioteca cliente, com troca de broker em caso de falha |
| `diagnostics` | contadores, cotas por identidade e dados do GetStats |
| `cmd/broker` | executável do broker |
| `cmd/sonda` | sonda dos ensaios de falha (canal de controle) |
| `cmd/ensaio` | ensaio de carga local com TCP e UDP reais |
| `scripts` | roteiros dos ensaios de falha e medição de MTU |
| `docs` | decisões de implementação, ensaios e resultados |

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
dp, err := client.ListenUDPDataPlane(":0")
c, err := client.Dial(ctx, client.Config{
	Brokers:   []string{"127.0.0.1:7000", "127.0.0.1:7100"},
	Identity:  "espectador-1",
	Token:     "...",
	DataPlane: dp,
})
sub, err := c.Subscribe(ctx, "sala-1/lances")
for ev := range sub.Events() {
	// ev.Payload
}
```

Sem `DataPlane`, a biblioteca usa um plano de dados em memória, que não
envia nem recebe eventos pela rede. Se o broker cair, a biblioteca passa
para o próximo da lista, mantém a sessão, troca o destino UDP e restaura as
inscrições.

## Ensaios

Os ensaios de falha rodam em contêineres (`make imagem`, depois
`scripts/ensaio.sh cenario <n>`). O ensaio de carga local roda com
`go run ./cmd/ensaio`. Procedimentos estão em `docs/ensaios.md` e os
resultados de carga, MTU, RTT/2 e lacuna de failover em
`docs/resultados-marco4.md`. As decisões tomadas durante a implementação
estão em `docs/decisoes.md`.
