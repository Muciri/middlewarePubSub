#!/bin/sh
# rede: injeta falhas de rede no contêiner em que roda. Precisa da
# capacidade NET_ADMIN. As regras ficam nas cadeias ENSAIO (entrada) e
# ENSAIO_SAIDA (saída) do iptables e na fila netem da interface, e
# "rede restaurar" desfaz tudo.
#
#   rede cortar <host> silencioso   descarta tudo o que chega de <host>; aplicar
#                                   nos dois lados para a partição ser simétrica
#   rede cortar <host> explicito    encerra o TCP com <host> por RST, nos dois
#                                   sentidos, e descarta o resto; basta um lado
#   rede perda <prob> [udp|tcp|todos] [host]
#                                   descarta ao acaso o que chega (prob de 0 a 1)
#   rede atraso <ms> [variacao_ms]  atrasa tudo o que sai da interface
#   rede restaurar                  remove cortes, perdas e atrasos
#   rede estado                     mostra as regras ativas
set -eu

CADEIA=ENSAIO
SAIDA=ENSAIO_SAIDA
IFACE=${REDE_IF:-eth0}

uso() {
	sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'
	exit 2
}

garantir_cadeia() {
	iptables -N "$CADEIA" 2>/dev/null || true
	iptables -C INPUT -j "$CADEIA" 2>/dev/null || iptables -I INPUT 1 -j "$CADEIA"
	iptables -N "$SAIDA" 2>/dev/null || true
	iptables -C OUTPUT -j "$SAIDA" 2>/dev/null || iptables -I OUTPUT 1 -j "$SAIDA"
}

[ $# -ge 1 ] || uso
acao=$1
shift

case "$acao" in
cortar)
	[ $# -eq 2 ] || uso
	garantir_cadeia
	case "$2" in
	silencioso)
		iptables -A "$CADEIA" -s "$1" -j DROP
		;;
	explicito)
		# Na entrada, o RST vai ao remetente; na saída, ao socket local. Os
		# próprios RST passam, senão uma regra descartaria o da outra.
		iptables -A "$CADEIA" -s "$1" -p tcp --tcp-flags RST RST -j ACCEPT
		iptables -A "$CADEIA" -s "$1" -p tcp -j REJECT --reject-with tcp-reset
		iptables -A "$SAIDA" -d "$1" -p tcp --tcp-flags RST RST -j ACCEPT
		iptables -A "$SAIDA" -d "$1" -p tcp -j REJECT --reject-with tcp-reset
		iptables -A "$CADEIA" -s "$1" -j DROP
		;;
	*) uso ;;
	esac
	;;
perda)
	[ $# -ge 1 ] || uso
	garantir_cadeia
	proto=${2:-udp}
	origem=""
	[ $# -ge 3 ] && origem="-s $3"
	case "$proto" in
	udp | tcp) filtro="-p $proto" ;;
	todos) filtro="" ;;
	*) uso ;;
	esac
	# shellcheck disable=SC2086
	iptables -A "$CADEIA" $origem $filtro -m statistic --mode random --probability "$1" -j DROP
	;;
atraso)
	[ $# -ge 1 ] || uso
	if [ $# -ge 2 ]; then
		tc qdisc replace dev "$IFACE" root netem delay "${1}ms" "${2}ms"
	else
		tc qdisc replace dev "$IFACE" root netem delay "${1}ms"
	fi
	;;
restaurar)
	iptables -F "$CADEIA" 2>/dev/null || true
	iptables -F "$SAIDA" 2>/dev/null || true
	tc qdisc del dev "$IFACE" root 2>/dev/null || true
	;;
estado)
	iptables -S "$CADEIA" 2>/dev/null || echo "sem cadeia $CADEIA"
	iptables -S "$SAIDA" 2>/dev/null || echo "sem cadeia $SAIDA"
	tc qdisc show dev "$IFACE"
	;;
*) uso ;;
esac
