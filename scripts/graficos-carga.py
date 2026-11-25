#!/usr/bin/env python3
"""Gera CSVs e SVGs de justiça, perdas e lacuna a partir da coleta do ensaio."""
import argparse
import html
import re
from pathlib import Path


def fields(line):
    return dict(re.findall(r'([\w-]+)=(?:"([^"]*)"|(\S+))', line))


def value(line, key):
    m = re.search(rf'\b{re.escape(key)}=(?:"([^"]*)"|(\S+))', line)
    return (m.group(1) or m.group(2)) if m else ""


def svg(path, title, rows, unit):
    width, height = 900, max(180, 100 + 42 * len(rows))
    maximum = max([v for _, v in rows] or [1]) or 1
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
           '<rect width="100%" height="100%" fill="white"/>',
           f'<text x="24" y="32" font-size="20">{html.escape(title)}</text>']
    for i, (label, v) in enumerate(rows):
        y = 70 + i * 42
        bar = 580 * v / maximum
        out.append(f'<text x="24" y="{y+17}" font-size="14">{html.escape(label)}: {v:.2f} {unit}</text>')
        out.append(f'<rect x="300" y="{y}" width="{bar:.1f}" height="22" fill="#2878b5"/>')
    out.append('</svg>')
    path.write_text('\n'.join(out), encoding='utf-8')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('diretorio', type=Path, help='diretório criado por scripts/ensaio.sh coletar')
    a = p.parse_args()
    lines = (a.diretorio / 'linha-do-tempo.log').read_text(encoding='utf-8').splitlines()
    totals, gaps, dropped = {}, {}, {}
    for line in lines:
        # Só um espectador para justiça; somar os dois duplicaria cada entrega.
        if '[espectador-a]' not in line:
            continue
        flow = value(line, 'fluxo')
        if flow and '/lances' in flow and value(line, 'recebidos'):
            publisher = flow.split('/')[0]
            totals[publisher] = totals.get(publisher, 0) + int(value(line, 'recebidos'))
        if 'lacuna de eventos' in line and flow:
            gaps[flow] = max(gaps.get(flow, 0), float(value(line, 'lacuna_ms') or 0))
        if 'msg=resumo' in line:
            dropped['descartes na fila'] = dropped.get('descartes na fila', 0) + int(value(line, 'descartados_fila') or 0)
    counts = sorted(totals.items())
    fair = (sum(totals.values()) ** 2 / (len(totals) * sum(v*v for v in totals.values()))) if totals else 0
    (a.diretorio / 'justica.csv').write_text('publicador,eventos\n' + ''.join(f'{k},{v}\n' for k,v in counts) + f'jain,{fair:.6f}\n', encoding='utf-8')
    svg(a.diretorio / 'justica.svg', f'Eventos por publicador (índice de Jain={fair:.3f})', counts, 'eventos')
    svg(a.diretorio / 'lacuna-failover.svg', 'Maior lacuna de eventos por fluxo', sorted(gaps.items()), 'ms')
    svg(a.diretorio / 'descartes.svg', 'Eventos descartados na fila do consumidor', sorted(dropped.items()), 'eventos')


if __name__ == '__main__':
    main()
