# Decisões de implementação

Decisões tomadas durante a implementação que o documento da Etapa 1 não
define. Cada item traz a justificativa e a alternativa descartada.

## Organização do código

**D1. Dependência do Protobuf com código gerado versionado.**
O módulo usa `google.golang.org/protobuf` e o código gerado em `internal/pb`
fica no repositório; `make proto` o recria.
Justificativa: quem só compila ou testa não precisa instalar o `protoc`.
Descartada: gerar o código no build, o que exigiria `protoc` e
`protoc-gen-go` em todas as máquinas.

**D2. Versão mínima do Go 1.23 no `go.mod`.**
Justificativa: os recursos usados existem desde essa versão, e nenhum
integrante é obrigado a atualizar o compilador.
Descartada: declarar a versão instalada localmente (1.27), que forçaria a
troca de toolchain nas outras máquinas.

**D3. Pacotes de apoio fora dos cinco módulos.**
`internal/pb` (código gerado), `internal/wire` (enquadramento),
`internal/contract` (interfaces entre os planos) e `internal/protoconv`
(conversões).
Justificativa: `broker` e `peer` dependem das mesmas interfaces; colocá-las em
um deles criaria import circular.
Descartada: definir as interfaces dentro de `broker`.

**D4. Identificadores em inglês e comentários em português.**
Justificativa: segue a convenção da linguagem e da biblioteca padrão.
Descartada: identificadores em português, que misturariam idiomas nas
chamadas à biblioteca padrão.

## Canal TCP de comandos

**D5. Prefixo de tamanho com 4 bytes big-endian.**
Justificativa: tamanho fixo, simples de ler, inspecionar e testar.
Descartada: varint (`protodelim`), de tamanho variável.

**D6. Teto de quadro configurável: 64 KiB no canal do cliente e 1 MiB no
canal entre brokers.**
O tamanho é verificado antes de alocar memória para o conteúdo.
Justificativa: um prefixo forjado não consegue reservar memória arbitrária;
1 MiB comporta o snapshot do catálogo.
Descartada: sem teto.

**D7. Mensagem `Envelope` com `oneof` e `request_id`.**
Mensagens enviadas pelo broker por iniciativa própria (resultado de
admissão, heartbeat, aviso de inscrição encerrada) usam `request_id` zero.
Justificativa: um único fluxo TCP carrega pedidos, respostas e avisos, e a
biblioteca casa cada resposta com seu pedido.
Descartada: um byte de tipo antes de cada mensagem, que repetiria o que o
`oneof` já faz.

**D8. Mensagens além da lista do documento.**
`PublishResult` (resultado de admissão), `Heartbeat` e `SubscriptionEnded`.
Justificativa: o resultado de admissão vai pelo TCP (seção 5.1), a falha é
detectada pela ausência de heartbeat (seção 8), e o cliente precisa saber
quando uma inscrição deixou de existir.
Descartada: detectar o fim da inscrição pela ausência de eventos, que não
distingue tópico apagado de tópico sem publicações.

**D9. `PublishResult` identificado por tópico, geração e sequência.**
Justificativa: a sequência é por publicador e tópico (seção 6.3); só a
sequência seria ambígua entre tópicos.
Descartada: identificar apenas pela sequência.

**D10. `HelloReply` informa a porta UDP do broker e o intervalo de
heartbeat.**
Justificativa: permite dois brokers na mesma máquina e ajustar o intervalo
sem mudar a biblioteca.
Descartada: porta 7001 fixa na biblioteca.

**D11. Violação de protocolo encerra a conexão.**
Primeira mensagem que não é Hello, segundo Hello, resposta enviada pelo
cliente ou corpo desconhecido.
Justificativa: o estado da conversa fica incerto e não há como recuperá-lo.
Descartada: responder erro e continuar.

**D12. Escrita com prazo; estouro encerra a conexão.**
O broker usa 1 s como prazo de escrita no canal do cliente.
Justificativa: uma escrita interrompida deixa um quadro parcial no fluxo; o
prazo também aplica a regra do consumidor que deixa de ler o canal de
controle por 1 s (seção 7.3).
Descartada: escrita sem prazo, em que um cliente parado bloquearia a
goroutine do broker.

## Autenticação e sessão

**D13. Credenciais estáticas em arquivo JSON.**
Cada entrada tem identidade, token e marca de administrador. O token é
comparado em tempo constante.
Justificativa: suficiente para o escopo e fácil de configurar nos ensaios.
Descartada: serviço externo de autenticação.

**D14. Sessão `uint64` gerada pela biblioteca e mantida na reconexão.**
Justificativa: a sequência por publicador continua válida ao trocar de
broker.
Descartada: sessão gerada pelo broker, que mudaria a cada reconexão.

**D15. Endereço UDP do cliente formado pelo IP de origem da conexão TCP e
pela porta informada no Hello.**
Justificativa: o cliente não consegue direcionar eventos para o endereço de
terceiros.
Descartada: o cliente informar o endereço UDP completo.

**D16. Sessão amarrada à identidade e ao endereço UDP do Hello.**
Hello que reutiliza sessão ligada a outra identidade é recusado. A mesma
identidade substitui o vínculo anterior: o broker encerra a conexão antiga
e espera a limpeza dela antes de responder.
Justificativa: impede o uso de sessão alheia; a limpeza atrasada da conexão
antiga não apaga as inscrições da nova.
Descartadas: aceitar qualquer sessão (permitiria sequestro) e recusar também
a mesma identidade (o cliente que reconecta antes de o broker notar a queda
ficaria bloqueado por até 5 s).

**D17. CreateTopic e DeleteTopic liberados a qualquer identidade
autenticada.**
A opção `RestrictTopicAdmin`, desligada por padrão, limita essas operações a
administradores. GetStats é sempre restrito, como define o documento.
Justificativa: o documento só restringe GetStats.
Descartada: restringir sempre, sem opção.

## Tópicos

**D18. Identificador e geração `uint32`; sessão `uint64`.**
Justificativa: cabem no cabeçalho UDP de 34 bytes. As larguras são
combinadas com o formato do cabeçalho.
Descartada: identificadores de 64 bits, que aumentariam o cabeçalho.

**D19. Identificador fixo por nome; a geração aumenta a cada recriação.**
Nomes apagados permanecem no catálogo com a última geração.
Justificativa: dá sentido a "geração antiga" no TOPIC_NOT_FOUND e impede que
a geração volte a 1 após apagar e recriar.
Descartada: novo identificador a cada recriação, em que a geração perderia a
função.

**D20. Nome com 1 a 128 bytes entre `a-z`, `0-9`, `_`, `-` e `/`.**
Justificativa: cobre nomes como `sala-1/lances` e evita problemas de
normalização e comparação de texto.
Descartada: qualquer texto UTF-8.

**D21. Validade máxima opcional na criação do tópico.**
Zero usa o padrão do tipo (500 ms ou 2 s); o limite é o que cabe em `uint32`
milissegundos.
Justificativa: ensaios podem ajustar a validade sem mudar o código.
Descartada: validade fixa por tipo.

**D22. Subscribe e Unsubscribe informam a geração; zero aceita a atual.**
Justificativa: na restauração após reconexão, a inscrição não pode passar
para um tópico recriado com o mesmo nome.
Descartada: inscrição só pelo nome.

## Prazos e limites

**D23. Heartbeat entre cliente e broker a cada 1 s, nos dois sentidos.**
Cada lado encerra a conexão após 5 s sem receber nada; qualquer mensagem
conta. O Hello também precisa chegar em até 5 s.
Justificativa: o intervalo bem menor que o limite tolera perdas pontuais;
o heartbeat do broker permite ao cliente detectar um broker travado.
Descartada: heartbeat só do cliente.

**D24. Prazo do resultado de admissão de 1 s, configurável.**
`Wait` devolve UNKNOWN sem erro quando o prazo da biblioteca se esgota e só
devolve erro se o contexto terminar.
Justificativa: UNKNOWN é um resultado previsto (seção 5.2), não uma falha.
Descartada: usar a validade do evento como prazo, que variaria por tópico.

**D25. Mais de 4.096 publicações pendentes falham na hora, sem consumir
sequência.**
Justificativa: a publicação continua assíncrona, e o assinante não vê
lacuna de sequência causada por uma recusa local.
Descartada: bloquear `Publish` até abrir vaga.

**D26. Fila de saída de 4.096 mensagens por conexão para o que o broker
envia por iniciativa própria.**
Com a fila cheia, o resultado é descartado e o cliente conclui UNKNOWN pelo
prazo.
Justificativa: o plano de dados nunca bloqueia esperando o TCP; 4.096 é o
máximo de pendentes de um cliente.
Descartada: bloquear o plano de dados até a escrita.

## Biblioteca cliente

**D27. Eventos entregues por canal, com a fila seguindo a política do
tópico.**
256 eventos descartando o mais antigo no efêmero; um evento, com a nova
versão substituindo a pendente, no de estado. `Err()` informa por que a
inscrição terminou.
Justificativa: segue o uso de canais previsto no plano de implementação.
Descartada: função de retorno chamada a cada evento.

**D28. `Publish` consulta o tópico no broker quando não está em cache.**
A consulta usa o prazo de publicação. Um resultado TOPIC_NOT_FOUND tira o
tópico do cache.
Justificativa: a aplicação publica pelo nome sem chamadas preparatórias, e
uma recriação do tópico é percebida na publicação seguinte.
Descartada: exigir LookupTopic antes da primeira publicação.

**D29. Uma inscrição por tópico em cada cliente.**
Assinar de novo devolve `ErrAlreadySubscribed`; Unsubscribe de algo não
inscrito responde OK.
Justificativa: duas inscrições no mesmo tópico duplicariam a entrega no
mesmo processo.
Descartada: várias inscrições por tópico.

**D30. `Dial` não tenta o outro broker quando as credenciais são
recusadas.**
Justificativa: as credenciais valem para os dois brokers; tentar o outro só
adiaria o erro.
Descartada: percorrer toda a lista em qualquer erro.

**D31. Interface `DataPlane` para o lado UDP do cliente.**
Deduplicação por sequência e descarte de eventos expirados ficam nessa
implementação, antes da entrega à inscrição.
Justificativa: separa o canal de controle do transporte de eventos.
Descartada: o canal de controle tratar datagramas.

**D32. Restauração recusada por geração antiga fecha a inscrição com erro
explícito.**
Justificativa: a aplicação precisa saber que o tópico que assinava deixou de
existir.
Descartada: restaurar silenciosamente na nova geração.

## Contratos com o plano de dados

**D33. Interfaces de leitura para o plano de dados; escrita nos tipos
concretos.**
Justificativa: só o plano de controle altera sessões, catálogo e inscrições.
Descartada: interfaces com métodos de escrita expostos aos dois planos.

**D34. Canal `Done()` em cada inscrição.**
Fechado em Unsubscribe, queda da conexão, encerramento por consumidor lento
ou exclusão do tópico.
Justificativa: o plano de dados libera a fila da inscrição sem varrer a
tabela.
Descartada: varredura periódica.

**D35. Idade única para a visão de interesse do par.**
Qualquer mensagem do par renova a visão inteira; a queda do enlace a
descarta.
Justificativa: a idade da visão já reflete o estado do enlace, e a admissão
decide NO_SUBSCRIBERS ou UNAVAILABLE consultando só a visão.
Descartada: idade por tópico, que exigiria mensagens por tópico para
renová-la.

**D36. `LinkStatus` com RTT.**
Justificativa: o encaminhamento desconta RTT/2 da validade (seção 6.3).
Descartada: estado do enlace só com vivo ou fora.

**D37. Sem par configurado, a visão do par é sempre recente e vazia.**
Justificativa: com um único broker não há assinantes remotos, então
NO_SUBSCRIBERS pode ser confirmado.
Descartada: tratar a ausência de par como enlace fora, o que faria toda
publicação sem assinantes locais receber UNAVAILABLE.

**D38. Estatísticas e cota de comandos como contratos.**
`StatsSource` alimenta o GetStats e `CommandLimiter` aplica a cota de 20
comandos/s; os stubs não restringem nada.
Justificativa: os dois pertencem ao plano de dados e ao diagnóstico, mas são
consultados pelo canal de controle.
Descartada: implementar contadores e cotas dentro do broker.
