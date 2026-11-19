#!/usr/bin/env bash
# ensaio.sh: executa os ensaios de falha descritos em docs/ensaios.md com
# os brokers A e B em contêineres separados.
#
#   scripts/ensaio.sh construir             compila a imagem
#   scripts/ensaio.sh preparar              sobe o ambiente limpo, cria os tópicos e
#                                           espera as sondas se inscreverem
#   scripts/ensaio.sh cenario <n> [variante]
#                                           prepara, executa o cenário e guarda os registros
#   scripts/ensaio.sh todos                 executa todos os cenários e variantes
#   scripts/ensaio.sh coletar [nome]        guarda os registros do ambiente atual
#   scripts/ensaio.sh descer                remove o ambiente
#
# Cenários e variantes (a primeira é o padrão):
#   1 matar|parar|travar|isolar   falha do broker A, preferido pela sonda-a
#   2 matar|parar|travar|isolar   falha do broker B, preferido pela sonda-b
#   3 silencioso|explicito        corte do enlace entre os brokers
#   4 silencioso|explicito        partição entre a sonda-a e o broker A
#   5 a-primeiro|b-primeiro       os dois brokers caem e voltam um de cada vez
#   6 matar                       retorno do broker B após falha
#   7 matar                       reinício do broker A com adoção do catálogo
#
# Variáveis: ENSAIO_ENV (arquivo de ambiente do compose, padrão
# scripts/ensaio.env), RESULTADOS (padrão ensaios/resultados) e
# ESPERA_MAX (segundos, padrão 20).
set -euo pipefail
cd "$(dirname "$0")/.."

ENSAIO_ENV=${ENSAIO_ENV:-scripts/ensaio.env}
RESULTADOS=${RESULTADOS:-ensaios/resultados}
ESPERA_MAX=${ESPERA_MAX:-20}
TOPICOS_INICIAIS="sala-1/lances sala-1/reacoes sala-1/placar=estado"

dc() {
	if [ -f "$ENSAIO_ENV" ]; then
		docker compose --env-file "$ENSAIO_ENV" "$@"
	else
		docker compose "$@"
	fi
}

DIR=""
info() { printf 'ensaio: %s\n' "$*" >&2; }
maiuscula() { printf '%s' "$1" | tr '[:lower:]' '[:upper:]'; }

# marcar registra um instante com o relógio e o formato dos contêineres.
marcar() {
	local linha
	linha=$(dc exec -T sonda-b sonda marcar -texto "$*" 2>&1 | tail -n 1)
	printf '%s\n' "$linha" | tee -a "$DIR/marcas.log" >&2
}

resultado() {
	printf '%s\n' "$*" | tee -a "$DIR/verificacoes.log" >&2
}

contar() {
	dc logs --no-color --no-log-prefix "$1" 2>/dev/null | grep -c -- "$2" || true
}

# esperar <serviço> <padrão> <mínimo> <descrição>
esperar() {
	local svc=$1 padrao=$2 minimo=$3 desc=$4 fim=$((SECONDS + ESPERA_MAX))
	while [ "$(contar "$svc" "$padrao")" -lt "$minimo" ]; do
		if [ $SECONDS -ge $fim ]; then
			resultado "divergente: $desc (sem \"$padrao\" em $svc após ${ESPERA_MAX}s)"
			return 0
		fi
		sleep 0.2
	done
	resultado "ok: $desc"
}

# sem_aumento <serviço> <padrão> <contagem anterior> <descrição>
sem_aumento() {
	local atual
	atual=$(contar "$1" "$2")
	if [ "$atual" -gt "$3" ]; then
		resultado "divergente: $4 ($((atual - $3)) ocorrências de \"$2\" em $1)"
	else
		resultado "ok: $4"
	fi
}

# comando <descrição> <padrão esperado na saída> <serviço> <args da sonda...>
comando() {
	local desc=$1 padrao=$2 svc=$3 saida
	shift 3
	saida=$(dc exec -T "$svc" sonda "$@" 2>&1 || true)
	printf '%s\n' "$saida" >>"$DIR/comandos.log"
	if printf '%s' "$saida" | grep -q -- "$padrao"; then
		resultado "ok: $desc"
	else
		resultado "divergente: $desc (esperado \"$padrao\"): $(printf '%s' "$saida" | tail -n 1)"
	fi
}

container() { dc ps -a -q "$1"; }

rede_do_ambiente() {
	docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$(container a)"
}

falhar_broker() {
	local svc=$1 variante=$2
	case $variante in
	matar) dc kill -s KILL "$svc" >/dev/null 2>&1 ;;
	parar) dc stop -t 3 "$svc" >/dev/null 2>&1 ;;
	travar) dc pause "$svc" >/dev/null 2>&1 ;;
	isolar) docker network disconnect "$REDE" "$(container "$svc")" ;;
	*) info "variante desconhecida: $variante"; exit 2 ;;
	esac
}

recuperar_broker() {
	local svc=$1 variante=$2
	case $variante in
	matar | parar) dc start "$svc" >/dev/null 2>&1 ;;
	travar) dc unpause "$svc" >/dev/null 2>&1 ;;
	isolar) docker network connect --alias "$svc" "$REDE" "$(container "$svc")" ;;
	esac
}

construir() { dc build; }

descer() { dc down --remove-orphans --timeout 3 >/dev/null 2>&1 || true; }

preparar() {
	descer
	dc up -d >/dev/null
	REDE=$(rede_do_ambiente)
	local fim=$((SECONDS + ESPERA_MAX))
	until [ "$(contar a 'enlace: ativo')" -ge 1 ] && [ "$(contar b 'enlace: ativo')" -ge 1 ]; do
		[ $SECONDS -lt $fim ] || { info "enlace não ficou ativo"; exit 1; }
		sleep 0.2
	done
	local t nome tipo
	for t in $TOPICOS_INICIAIS; do
		nome=${t%%=*}
		tipo=efemero
		[ "$t" != "$nome" ] && tipo=${t#*=}
		dc exec -T sonda-a sonda criar -brokers a:7000 -id leiloeiro -token ensaio-leiloeiro \
			-topico "$nome" -tipo "$tipo" >/dev/null 2>&1
	done
	fim=$((SECONDS + ESPERA_MAX))
	until [ "$(contar sonda-a 'msg=inscrita')" -ge 3 ] && [ "$(contar sonda-b 'msg=inscrita')" -ge 3 ]; do
		[ $SECONDS -lt $fim ] || { info "sondas não se inscreveram"; exit 1; }
		sleep 0.2
	done
	info "ambiente pronto"
}

admin_args() { printf '%s\n' -id leiloeiro -token ensaio-leiloeiro; }

cenario_falha_broker() {
	local alvo=$1 variante=$2 sonda_alvo sonda_outra outro
	if [ "$alvo" = a ]; then
		sonda_alvo=sonda-a sonda_outra=sonda-b outro=b
	else
		sonda_alvo=sonda-b sonda_outra=sonda-a outro=a
	fi
	local fo_outra
	fo_outra=$(contar $sonda_outra 'failover concluído')

	marcar "injeção: $variante no broker $(maiuscula $alvo)"
	falhar_broker "$alvo" "$variante"
	marcar "injeção concluída"

	esperar $sonda_alvo 'failover concluído' 1 "$sonda_alvo trocou de broker e restaurou as inscrições"
	esperar $outro 'enlace: perdido' 1 "broker $(maiuscula $outro) detectou a perda do par"
	sem_aumento $sonda_outra 'failover concluído' "$fo_outra" "$sonda_outra seguiu no broker $(maiuscula $outro)"
	sem_aumento $sonda_alvo 'inscrição encerrada' 0 "nenhuma inscrição da $sonda_alvo foi encerrada"
	# shellcheck disable=SC2046
	comando "criação de tópico no broker $(maiuscula $outro) retorna UNAVAILABLE" 'estado global indisponível' \
		$sonda_outra criar -brokers $outro:7000 $(admin_args) -topico sala-9/teste
	comando "inscrição nova no broker $(maiuscula $outro) retorna UNAVAILABLE" 'estado global indisponível' \
		$sonda_outra inscrever -brokers $outro:7000 -topico sala-1/lances
	comando "consulta ao catálogo no broker $(maiuscula $outro) segue respondendo" 'tópico encontrado' \
		$sonda_outra consultar -brokers $outro:7000 -topico sala-1/lances

	recuperar_broker "$alvo" "$variante"
	marcar "broker $(maiuscula $alvo) recuperado ($variante)"
	esperar $alvo 'enlace: ativo' 2 "enlace restabelecido após a recuperação"
}

cenario_enlace() {
	local variante=$1 fo_a fo_b
	fo_a=$(contar sonda-a 'failover concluído')
	fo_b=$(contar sonda-b 'failover concluído')

	marcar "injeção: corte $variante do enlace entre A e B"
	case $variante in
	silencioso)
		dc exec -T a rede cortar b silencioso
		dc exec -T b rede cortar a silencioso
		;;
	explicito) dc exec -T a rede cortar b explicito ;;
	*) info "variante desconhecida: $variante"; exit 2 ;;
	esac
	marcar "injeção concluída"

	esperar a 'enlace: perdido' 1 "broker A detectou a perda do par"
	esperar b 'enlace: perdido' 1 "broker B detectou a perda do par"
	# shellcheck disable=SC2046
	comando "criação de tópico no A retorna UNAVAILABLE" 'estado global indisponível' \
		sonda-a criar -brokers a:7000 $(admin_args) -topico sala-9/teste
	# shellcheck disable=SC2046
	comando "criação de tópico no B retorna UNAVAILABLE" 'estado global indisponível' \
		sonda-b criar -brokers b:7000 $(admin_args) -topico sala-9/teste
	comando "inscrição nova retorna UNAVAILABLE" 'estado global indisponível' \
		sonda-b inscrever -brokers b:7000 -topico sala-1/lances
	sleep 3
	sem_aumento sonda-a 'failover concluído' "$fo_a" "sonda-a seguiu no A durante a partição"
	sem_aumento sonda-b 'failover concluído' "$fo_b" "sonda-b seguiu no B durante a partição"
	sem_aumento sonda-a 'inscrição encerrada' 0 "inscrições existentes seguiram ativas"

	local snap_b int_a int_b
	snap_b=$(contar b 'snapshot do catálogo aplicado')
	int_a=$(contar a 'interesse do par recebido')
	int_b=$(contar b 'interesse do par recebido')
	marcar "restauração do enlace"
	dc exec -T a rede restaurar
	dc exec -T b rede restaurar
	esperar b 'snapshot do catálogo aplicado' $((snap_b + 1)) "B recebeu novo snapshot do catálogo"
	esperar a 'interesse do par recebido' $((int_a + 1)) "A recebeu o interesse do B"
	esperar b 'interesse do par recebido' $((int_b + 1)) "B recebeu o interesse do A"
	# shellcheck disable=SC2046
	comando "criação de tópico volta a funcionar" 'tópico criado' \
		sonda-a criar -brokers a:7000 $(admin_args) -topico sala-9/teste
}

cenario_particao_cliente() {
	local variante=$1
	marcar "injeção: partição $variante entre sonda-a e broker A"
	case $variante in
	silencioso)
		dc exec -T sonda-a rede cortar a silencioso
		dc exec -T a rede cortar sonda-a silencioso
		;;
	explicito) dc exec -T sonda-a rede cortar a explicito ;;
	*) info "variante desconhecida: $variante"; exit 2 ;;
	esac
	marcar "injeção concluída"

	esperar sonda-a 'failover concluído' 1 "sonda-a trocou para o B e restaurou as inscrições"
	esperar a 'sessão encerrada.*identidade=sonda-a' 1 "broker A encerrou a sessão da sonda-a"
	sem_aumento a 'enlace: perdido' 0 "enlace entre os brokers não foi afetado"
	sem_aumento sonda-a 'inscrição encerrada' 0 "nenhuma inscrição da sonda-a foi encerrada"

	marcar "restauração da rede da sonda-a"
	dc exec -T sonda-a rede restaurar
	dc exec -T a rede restaurar
	sleep 3
	sem_aumento sonda-a 'failover concluído' 1 "sonda-a não voltou ao A sem nova falha"
}

cenario_dois_brokers() {
	local ordem=$1 primeiro segundo
	case $ordem in
	a-primeiro) primeiro=a segundo=b ;;
	b-primeiro) primeiro=b segundo=a ;;
	*) info "variante desconhecida: $ordem"; exit 2 ;;
	esac

	marcar "injeção: kill -9 nos brokers A e B"
	dc kill -s KILL a b >/dev/null 2>&1
	marcar "injeção concluída"
	esperar sonda-a 'falha no canal de controle' 1 "sonda-a detectou a falha"
	esperar sonda-b 'falha no canal de controle' 1 "sonda-b detectou a falha"
	sleep 2

	local ativo_p ativo_s adotado
	adotado=$(contar a 'catálogo adotado do par')
	ativo_p=$(contar $primeiro 'enlace: ativo')
	ativo_s=$(contar $segundo 'enlace: ativo')
	marcar "retorno do broker $(maiuscula $primeiro) sozinho"
	dc start $primeiro >/dev/null 2>&1
	sleep 3
	comando "broker $(maiuscula $primeiro) sozinho não responde pelo catálogo" 'estado global indisponível' \
		sonda-a consultar -brokers $primeiro:7000 -topico sala-1/lances
	# shellcheck disable=SC2046
	comando "broker $(maiuscula $primeiro) sozinho não cria tópicos" 'estado global indisponível' \
		sonda-a criar -brokers $primeiro:7000 $(admin_args) -topico sala-1/lances
	sem_aumento sonda-a 'inscrição encerrada' 0 "inscrições seguem abertas à espera de um broker sincronizado"

	marcar "retorno do broker $(maiuscula $segundo)"
	dc start $segundo >/dev/null 2>&1
	esperar $primeiro 'enlace: ativo' $((ativo_p + 1)) "enlace restabelecido"
	esperar $segundo 'enlace: ativo' $((ativo_s + 1)) "enlace restabelecido no outro lado"
	esperar a 'catálogo adotado do par.*ativos=0' $((adotado + 1)) "A adotou o catálogo vazio do B"
	esperar sonda-a 'inscrição encerrada.*tópico apagado' 3 "inscrições da sonda-a encerradas por tópico inexistente"
	esperar sonda-b 'inscrição encerrada.*tópico apagado' 3 "inscrições da sonda-b encerradas por tópico inexistente"

	marcar "recriação dos tópicos"
	local t nome tipo
	for t in $TOPICOS_INICIAIS; do
		nome=${t%%=*}
		tipo=efemero
		[ "$t" != "$nome" ] && tipo=${t#*=}
		# shellcheck disable=SC2046
		comando "tópico $nome recriado com geração 1" 'tópico criado.*geracao=1' \
			sonda-a criar -brokers a:7000 $(admin_args) -topico "$nome" -tipo "$tipo"
	done
	esperar sonda-a 'msg=inscrita' 6 "sonda-a voltou a se inscrever"
	esperar sonda-b 'msg=inscrita' 6 "sonda-b voltou a se inscrever"
}

cenario_retorno() {
	marcar "injeção: kill -9 no broker B"
	dc kill -s KILL b >/dev/null 2>&1
	marcar "injeção concluída"
	esperar sonda-b 'failover concluído' 1 "sonda-b trocou para o A"
	sleep 2

	marcar "retorno do broker B"
	dc start b >/dev/null 2>&1
	esperar b 'snapshot do catálogo aplicado.*ativos=3' 1 "B recebeu o snapshot com os 3 tópicos"
	esperar a 'snapshot do catálogo enviado' 2 "A enviou o snapshot na reconexão"
	esperar b 'interesse do par recebido.*topicos=3' 1 "B recebeu o interesse do A com os 3 tópicos"
	esperar a 'interesse do par recebido.*topicos=0' 2 "A recebeu o interesse vazio do B reiniciado"
	sleep 2
	sem_aumento sonda-b 'failover concluído' 1 "sonda-b seguiu no A, sem retorno automático"
	comando "inscrição nova no B volta a ser aceita" 'inscrição aceita' \
		sonda-b inscrever -brokers b:7000 -topico sala-1/lances
}

cenario_reinicio_admin() {
	# shellcheck disable=SC2046
	comando "sala-1/reacoes apagado antes do reinício" 'tópico apagado' \
		sonda-a apagar -brokers a:7000 $(admin_args) -topico sala-1/reacoes
	esperar sonda-a 'inscrição encerrada.*sala-1/reacoes' 1 "inscrição da sonda-a em sala-1/reacoes encerrada"
	esperar sonda-b 'inscrição encerrada.*sala-1/reacoes' 1 "inscrição da sonda-b em sala-1/reacoes encerrada"
	local fim_a fim_b
	fim_a=$(contar sonda-a 'inscrição encerrada')
	fim_b=$(contar sonda-b 'inscrição encerrada')

	marcar "injeção: kill -9 no broker A"
	dc kill -s KILL a >/dev/null 2>&1
	marcar "injeção concluída"
	esperar sonda-a 'failover concluído' 1 "sonda-a trocou para o B"
	sleep 1

	marcar "retorno do broker A sem catálogo"
	dc start a >/dev/null 2>&1
	esperar a 'catálogo adotado do par.*ativos=2 apagados=1' 1 "A adotou o catálogo do B com o nome apagado"
	esperar a 'enlace: ativo' 2 "enlace restabelecido"
	sem_aumento b 'tópicos removidos pelo snapshot' 0 "B não perdeu tópicos"
	sem_aumento sonda-a 'inscrição encerrada' "$fim_a" "inscrições da sonda-a sobreviveram"
	sem_aumento sonda-b 'inscrição encerrada' "$fim_b" "inscrições da sonda-b sobreviveram"
	# shellcheck disable=SC2046
	comando "recriar sala-1/reacoes no A continua a geração" 'tópico criado.*id=2 geracao=2' \
		sonda-a criar -brokers a:7000 $(admin_args) -topico sala-1/reacoes
	# shellcheck disable=SC2046
	comando "tópico novo no A recebe o id seguinte" 'tópico criado.*id=4 geracao=1' \
		sonda-a criar -brokers a:7000 $(admin_args) -topico sala-2/lances
}

# coletar guarda os registros de cada serviço e monta a linha do tempo, o
# resumo e as medidas com scripts/linha-do-tempo.sh.
coletar() {
	local svc
	for svc in a b sonda-a sonda-b; do
		dc logs --no-color --no-log-prefix "$svc" >"$DIR/$svc.log" 2>/dev/null || true
	done
	scripts/linha-do-tempo.sh "$DIR"
}

novo_diretorio() {
	DIR="$RESULTADOS/$(date -u +%Y%m%dT%H%M%SZ)-$1"
	mkdir -p "$DIR"
}

cenario() {
	local n=$1 variante=${2:-}
	case $n in
	1 | 2) variante=${variante:-matar} ;;
	3 | 4) variante=${variante:-silencioso} ;;
	5) variante=${variante:-a-primeiro} ;;
	6 | 7) variante=${variante:-matar} ;;
	*) info "cenário desconhecido: $n"; exit 2 ;;
	esac
	novo_diretorio "cenario$n-$variante"
	info "cenário $n ($variante)"
	preparar
	marcar "início do cenário $n ($variante)"
	case $n in
	1) cenario_falha_broker a "$variante" ;;
	2) cenario_falha_broker b "$variante" ;;
	3) cenario_enlace "$variante" ;;
	4) cenario_particao_cliente "$variante" ;;
	5) cenario_dois_brokers "$variante" ;;
	6) cenario_retorno ;;
	7) cenario_reinicio_admin ;;
	esac
	marcar "fim do cenário $n ($variante)"
	sleep 1
	coletar
}

[ $# -ge 1 ] || { sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
case $1 in
construir) construir ;;
preparar) preparar ;;
cenario) shift; cenario "$@" ;;
todos)
	for c in "1 matar" "1 parar" "1 travar" "1 isolar" "2 matar" "2 travar" \
		"3 silencioso" "3 explicito" "4 silencioso" "4 explicito" \
		"5 a-primeiro" "5 b-primeiro" "6" "7"; do
		# shellcheck disable=SC2086
		cenario $c
	done
	descer
	;;
coletar) novo_diretorio "${2:-coleta}"; coletar ;;
descer) descer ;;
*) sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
