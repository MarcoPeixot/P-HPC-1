import argparse
import csv
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import FixedLocator, FuncFormatter, NullLocator
import numpy as np


def palette():
    return dict(surface='#fcfcfb', text='#0b0b0b', muted='#52514e', grid='#e4e3df', band='#f0efec',
                ideal='#8a8984', corpus='#2a78d6', pi='#eb6834', calc='#1baf7a', serial='#4a3aa7',
                amdahl_low='#4a3aa7', amdahl_fit='#e87ba4', usl='#1baf7a')


def read_csv(path):
    with open(path) as stream:
        return {int(row['nprocs']): {key: float(row[key]) for key in ('t_total', 't_serial', 't_calc')} | {'nnodes': int(row['nnodes'])}
                for row in csv.DictReader(stream)}


def read_repetitions(batch):
    repetitions = {}
    for path in sorted(Path(batch).glob('w*-r*/measurement.json')):
        item = json.loads(path.read_text())
        repetitions.setdefault(item['workers'], []).append(item)
    return repetitions


def decimal(value, _=None):
    if value >= 1 and float(value).is_integer():
        return f'{int(value)}'
    return f'{value:g}'.replace('.', ',')


def style(ax, colors):
    ax.set_facecolor(colors['surface'])
    for side in ('top', 'right'):
        ax.spines[side].set_visible(False)
    for side in ('left', 'bottom'):
        ax.spines[side].set_color(colors['grid'])
    ax.tick_params(colors=colors['muted'], labelsize=9, length=0)
    ax.grid(color=colors['grid'], linewidth=0.8)
    ax.set_axisbelow(True)


def end_label(ax, x, y, text, colors, dy=0):
    ax.annotate(text, (x, y), xytext=(8, dy), textcoords='offset points', va='center', fontsize=9, color=colors['text'])


def speedup_figure(corpus, pi, repetitions, output, colors):
    ps = sorted(corpus)
    t1 = corpus[1]['t_total']
    s_corpus = np.array([t1 / corpus[p]['t_total'] for p in ps])
    s_pi = np.array([pi[1]['t_total'] / pi[p]['t_total'] for p in ps])
    low = np.array([t1 / max(r['t_total'] for r in repetitions[p]) for p in ps])
    high = np.array([t1 / min(r['t_total'] for r in repetitions[p]) for p in ps])
    figure, axes = plt.subplots(1, 2, figsize=(12, 4.8), facecolor=colors['surface'])
    for ax, scale in zip(axes, ('linear', 'log')):
        style(ax, colors)
        ax.axvspan(16, 32, color=colors['band'], zorder=0)
        ax.axvline(8, color=colors['grid'], linewidth=1, zorder=0)
        ax.plot(ps, ps, '--', color=colors['ideal'], linewidth=2, label='linear ideal (S = p)')
        ax.plot(ps, s_pi, '-o', color=colors['pi'], linewidth=2, markersize=8, markeredgecolor=colors['surface'],
                markeredgewidth=2, label='pi_mpi (Aula 3)')
        ax.errorbar(ps, s_corpus, yerr=[s_corpus - low, high - s_corpus], fmt='-o', color=colors['corpus'], linewidth=2,
                    markersize=8, markeredgecolor=colors['surface'], markeredgewidth=2, ecolor=colors['corpus'],
                    elinewidth=1, capsize=3, label='corpus B2W (mediana; barras: mín–máx de 3)')
        ax.set_xscale('log', base=2)
        ax.xaxis.set_major_locator(FixedLocator(ps))
        ax.xaxis.set_minor_locator(NullLocator())
        ax.xaxis.set_major_formatter(FuncFormatter(decimal))
        ax.set_xlim(0.85, 45)
        ax.set_xlabel('processos MPI / workers Dask (escala log₂)', color=colors['muted'], fontsize=9)
        ax.set_ylabel('speedup S(p) = T(1) / T(p)', color=colors['muted'], fontsize=9)
        if scale == 'log':
            ax.set_yscale('log', base=2)
            ticks = [0.5, 1, 2, 4, 8, 16, 32]
            ax.yaxis.set_major_locator(FixedLocator(ticks))
            ax.yaxis.set_minor_locator(NullLocator())
            ax.yaxis.set_major_formatter(FuncFormatter(decimal))
            ax.set_ylim(0.4, 45)
            ax.axhline(1, color=colors['muted'], linewidth=0.8, zorder=1)
            ax.annotate('S = 1 (sem ganho)', (1, 1), xytext=(2, -11), textcoords='offset points', fontsize=8, color=colors['muted'])
            ax.annotate('máx. 1,34× com 2 workers', (2, s_corpus[1]), xytext=(6, 10), textcoords='offset points',
                        fontsize=8, color=colors['text'])
            ax.set_title('(b) eixo y em log₂: forma das curvas', loc='left', fontsize=10, color=colors['text'])
        else:
            ax.set_ylim(0, 34)
            ax.set_title('(a) eixo y linear: distância absoluta', loc='left', fontsize=10, color=colors['text'])
            ax.legend(loc='upper left', frameon=False, fontsize=8.5, labelcolor=colors['text'])
        top = ax.get_ylim()[1]
        ax.annotate('SMT', (22.6, top), xytext=(0, -12), textcoords='offset points', ha='center', fontsize=8, color=colors['muted'])
        ax.annotate('≥ 2 nós →', (8, top), xytext=(4, -12), textcoords='offset points', fontsize=8, color=colors['muted'])
        end_label(ax, 32, 32, 'ideal 32×', colors)
        end_label(ax, 32, s_pi[-1], f'pi_mpi {s_pi[-1]:.1f}×'.replace('.', ','), colors, -4 if scale == 'linear' else 0)
        end_label(ax, 32, s_corpus[-1], f'corpus {s_corpus[-1]:.2f}×'.replace('.', ','), colors)
    figure.suptitle('Speedup do corpus, do pi_mpi e linear ideal: 1 a 32 processos', x=0.01, ha='left', fontsize=12,
                    color=colors['text'], fontweight='semibold')
    figure.tight_layout()
    figure.savefig(output, dpi=150, facecolor=colors['surface'])
    plt.close(figure)


def decomposition_figure(corpus, repetitions, output, colors):
    ps = sorted(corpus)
    runs = []
    for p in ps:
        ordered = sorted(repetitions[p], key=lambda r: r['t_total'])
        runs.append(ordered[1])
    serial = np.array([r['t_serial'] for r in runs])
    calc = np.array([r['t_calc'] for r in runs])
    total = serial + calc
    ideal = corpus[1]['t_total'] / np.array(ps)
    x = np.arange(len(ps))
    figure, ax = plt.subplots(figsize=(9, 4.8), facecolor=colors['surface'])
    style(ax, colors)
    ax.grid(axis='x', visible=False)
    ax.bar(x, calc, width=0.42, color=colors['calc'], edgecolor=colors['surface'], linewidth=1.5, label='t_calc (fases distribuídas, espera, I/O)')
    ax.bar(x, serial, width=0.42, bottom=calc, color=colors['serial'], edgecolor=colors['surface'], linewidth=1.5,
           label='t_serial (merge no cliente, broadcast de vocabulário/IDF, agregação)')
    ax.scatter(x, ideal, marker='_', s=900, linewidths=2, color=colors['ideal'], zorder=3, label='tempo ideal T(1) / p')
    for position, value, part in zip(x, total, serial):
        ax.annotate(f'{value:.2f} s\nserial {part / value:.0%}'.replace('.', ','), (position, value), xytext=(0, 5),
                    textcoords='offset points', ha='center', va='bottom', fontsize=8.5, color=colors['text'])
    labels = [f'{p} worker{"s" if p > 1 else ""}\n{corpus[p]["nnodes"]} nó{"s" if corpus[p]["nnodes"] > 1 else ""}' for p in ps]
    labels[-1] += ' (SMT)'
    ax.set_xticks(x, labels)
    ax.set_ylim(0, 14)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f'{v:g} s'))
    handles, names = ax.get_legend_handles_labels()
    ax.legend([handles[i] for i in (2, 1, 0)], [names[i] for i in (2, 1, 0)], loc='upper left', frameon=False, fontsize=8.5, labelcolor=colors['text'])
    ax.set_title('Decomposição do tempo do pipeline: execução mediana de cada configuração', loc='left', fontsize=11,
                 color=colors['text'], fontweight='semibold')
    figure.tight_layout()
    figure.savefig(output, dpi=150, facecolor=colors['surface'])
    plt.close(figure)


def models_figure(corpus, output, colors):
    ps = sorted(corpus)
    t1 = corpus[1]['t_total']
    measured = np.array([t1 / corpus[p]['t_total'] for p in ps])
    f_code = corpus[1]['t_serial'] / t1
    s2 = t1 / corpus[2]['t_total']
    f_fit = (1 / s2 - 0.5) / 0.5
    system = np.array([[1.0, 2.0], [3.0, 12.0]])
    targets = np.array([2 / s2 - 1, 4 / (t1 / corpus[4]['t_total']) - 1])
    sigma, kappa = np.linalg.solve(system, targets)
    grid = np.logspace(0, 5, 200, base=2)
    figure, ax = plt.subplots(figsize=(9, 4.8), facecolor=colors['surface'])
    style(ax, colors)
    curves = [
        (1 / (f_code + (1 - f_code) / grid), colors['amdahl_low'], (0, (6, 3)), f'Amdahl f = {f_code:.3f} (t_serial medido em p = 1)'),
        (1 / (f_fit + (1 - f_fit) / grid), colors['amdahl_fit'], (0, (6, 2, 1, 2)), f'Amdahl f = {f_fit:.3f} (ajuste em p = 1, 2)'),
        (grid / (1 + sigma * (grid - 1) + kappa * grid * (grid - 1)), colors['usl'], (0, (1, 2)),
         f'USL σ = {sigma:.3f}, κ = {kappa:.3f} (ajuste em p = 1, 2, 4)'),
    ]
    for values, color, dash, label in curves:
        ax.plot(grid, values, color=color, linewidth=2, linestyle=dash, label=label.replace('.', ','))
    ax.plot(ps, measured, '-o', color=colors['corpus'], linewidth=2, markersize=8, markeredgecolor=colors['surface'],
            markeredgewidth=2, label='corpus medido (mediana)', zorder=4)
    ax.axhline(1, color=colors['muted'], linewidth=0.8, zorder=1)
    ax.set_xscale('log', base=2)
    ax.xaxis.set_major_locator(FixedLocator(ps))
    ax.xaxis.set_minor_locator(NullLocator())
    ax.xaxis.set_major_formatter(FuncFormatter(decimal))
    ax.yaxis.set_major_formatter(FuncFormatter(decimal))
    ax.set_xlim(0.9, 48)
    ax.set_ylim(0, 6.2)
    ax.set_xlabel('workers Dask (escala log₂)', color=colors['muted'], fontsize=9)
    ax.set_ylabel('speedup', color=colors['muted'], fontsize=9)
    end_label(ax, 32, curves[0][0][-1], 'otimista', colors)
    end_label(ax, 32, curves[1][0][-1], 'Amdahl efetivo', colors)
    end_label(ax, 32, curves[2][0][-1], 'USL', colors, -7)
    end_label(ax, 32, measured[-1], 'medido', colors, 7)
    ax.legend(loc='upper left', frameon=False, fontsize=8.5, labelcolor=colors['text'])
    ax.set_title('Corpus: speedup medido contra modelos de Amdahl e USL', loc='left', fontsize=11,
                 color=colors['text'], fontweight='semibold')
    figure.tight_layout()
    figure.savefig(output, dpi=150, facecolor=colors['surface'])
    plt.close(figure)


if __name__ == '__main__':
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser()
    parser.add_argument('--corpus', default=root / 'resultados' / 'speedup_corpus.csv', type=Path)
    parser.add_argument('--pi', default=root / 'resultados' / 'speedup.csv', type=Path)
    parser.add_argument('--batch', default=root / 'resultados' / 'pipeline' / 'run-20260930-corpus02', type=Path)
    parser.add_argument('--output', default=Path(__file__).resolve().parent, type=Path)
    args = parser.parse_args()
    colors = palette()
    corpus, pi, repetitions = read_csv(args.corpus), read_csv(args.pi), read_repetitions(args.batch)
    args.output.mkdir(parents=True, exist_ok=True)
    speedup_figure(corpus, pi, repetitions, args.output / 'speedup-tres-curvas.png', colors)
    decomposition_figure(corpus, repetitions, args.output / 'decomposicao-tempo.png', colors)
    models_figure(corpus, args.output / 'modelos-amdahl-usl.png', colors)
    print(f'Figuras salvas em {args.output}')
