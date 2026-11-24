#!/usr/bin/env bash
# linha-do-tempo.sh <diretório>: junta os registros *.log do diretório numa
# linha do tempo ordenada pelo horário e calcula as medidas de failover.
# Serve aos ensaios em contêineres e aos registros copiados de várias
# máquinas, desde que os relógios estejam sincronizados.
#
# Cada arquivo vira uma etiqueta com o nome dele (a.log vira [a]);
# marcas.log vira [ensaio] e comandos.log vira [comando]. Gera:
#   linha-do-tempo.log  todas as linhas, ordenadas
#   resumo.log          só os eventos de falha, enlace e inscrição
#   medidas.log         por cliente: injeção até detecção, injeção até
#                       restauração e a lacuna medida pela biblioteca; por
#                       espectador e fluxo do leilão: a lacuna de eventos
#
# A primeira marca com texto "injeção:" é o instante zero, e a marca
# seguinte, "injeção concluída", fecha o intervalo em que a falha ocorreu.
# A lacuna de eventos de um fluxo é a maior lacuna registrada pelo
# espectador que cobre esse intervalo: vai do último evento antes da falha
# ao primeiro depois. Sem lacuna, a interrupção ficou abaixo de 1,5 vez a
# cadência do fluxo. Linhas no mesmo milissegundo não têm ordem causal
# garantida.
set -euo pipefail

DIR=${1:?uso: linha-do-tempo.sh <diretório>}
GERADOS="linha-do-tempo.log resumo.log medidas.log verificacoes.log"

etiqueta() {
	case $1 in
	marcas) echo ensaio ;;
	comandos) echo comando ;;
	*) echo "$1" ;;
	esac
}

{
	for f in "$DIR"/*.log; do
		nome=$(basename "$f" .log)
		case " $GERADOS " in *" $nome.log "*) continue ;; esac
		sed -n "s/^\(time=[^ ]*\) /\1 [$(etiqueta "$nome")] /p" "$f"
	done
} | LC_ALL=C sort >"$DIR/linha-do-tempo.log"

grep -E 'marca|msg=inscrito|lacuna de eventos|enlace: (perdido|ativo)|snapshot|adotado|interesse do par|failover|falha no canal|inscrição (encerrada|restaurada)|sessão encerrada|comandos (in)?disponíveis|comando falhou|tópico (criado|apagado)|não aplicada|removidos' \
	"$DIR/linha-do-tempo.log" >"$DIR/resumo.log" || true

# O horário do dia é usado nos cálculos: o ensaio não pode atravessar a
# meia-noite UTC.
LC_ALL=C awk '
function ms(campo,  h) {
	split(substr(campo, 17, 12), h, ":")
	return (h[1] * 3600 + h[2] * 60 + h[3]) * 1000
}
function hms(campo,  h) {
	split(campo, h, ":")
	return (h[1] * 3600 + h[2] * 60 + h[3]) * 1000
}
function valor(chave,  v) {
	if (!match($0, " " chave "=[^ ]*")) return ""
	v = substr($0, RSTART + length(chave) + 2, RLENGTH - length(chave) - 2)
	gsub(/"/, "", v)
	return v
}
function sonda(  q) {
	q = valor("sonda")
	return q != "" ? q : valor("cliente")
}
/msg=marca/ && /texto="injeção:/ && inj == "" { inj = ms($1) }
inj != "" && /falha no canal de controle/ { s = sonda(); if (!(s in det)) det[s] = ms($1) - inj }
inj != "" && /failover concluído/ {
	s = sonda()
	if (!(s in res)) {
		res[s] = ms($1) - inj
		match($0, /lacuna_ms=[^ ]*/); lac[s] = substr($0, RSTART + 10, RLENGTH - 10)
		match($0, /de=[^ ]* para=[^ ]*/); rota[s] = substr($0, RSTART, RLENGTH)
	}
}
inj != "" && conc == "" && /msg=marca/ && /texto="injeção concluída"/ { conc = ms($1) }
inj != "" && /msg=fluxo/ { k = sonda() " " valor("fluxo"); visto[k] = 1; cad[k] = valor("cadencia_ms") }
inj != "" && /msg="lacuna de eventos"/ {
	de = valor("de"); ate = valor("ate")
	fim = conc != "" ? conc : inj
	if (hms(ate) >= inj && hms(de) <= fim) {
		k = sonda() " " valor("fluxo")
		g = valor("lacuna_ms") + 0
		if (!(k in gap) || g > gap[k]) {
			gap[k] = g; gde[k] = de; gate[k] = ate; gperd[k] = valor("perdidos")
		}
	}
}
END {
	for (s in res)
		printf "%s (%s): detecção %.0f ms após a injeção, restauração %.0f ms após a injeção, lacuna da biblioteca %s ms\n", s, rota[s], det[s], res[s], lac[s]
	for (k in visto) {
		if (k in gap)
			printf "%s: lacuna de eventos %.0f ms, do último evento antes (%s) ao primeiro depois (%s), %s perdidos\n", k, gap[k], gde[k], gate[k], gperd[k]
		else
			printf "%s: sem lacuna de eventos acima de 1,5 vez a cadência (%s ms)\n", k, cad[k]
	}
}' "$DIR/linha-do-tempo.log" | sort >"$DIR/medidas.log"

echo "ensaio: registros em $DIR" >&2
if [ -f "$DIR/verificacoes.log" ]; then
	echo "ensaio: verificações: $(grep -c '^ok' "$DIR/verificacoes.log" || true) ok, $(grep -c '^divergente' "$DIR/verificacoes.log" || true) divergentes" >&2
fi
sed 's/^/ensaio: /' "$DIR/medidas.log" >&2
