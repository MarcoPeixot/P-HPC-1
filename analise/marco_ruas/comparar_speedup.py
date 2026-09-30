"""Compara os CSVs reais e calcula a estimativa de Amdahl sem ocultar f > 1."""
import csv
import json
import math
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[2]
POINTS = [1, 2, 4, 8, 16, 32]
HEADER = ['nprocs', 'nnodes', 't_total', 't_serial', 't_calc']


def read_rows(path):
    with path.open(newline='') as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != HEADER:
            raise ValueError(f'Cabeçalho incompatível: {path}')
        rows = list(reader)
    if [int(row['nprocs']) for row in rows] != POINTS:
        raise ValueError(f'Exige os seis pontos ordenados: {path}')
    for row in rows:
        if not math.isfinite(float(row['t_total'])) or float(row['t_total']) <= 0:
            raise ValueError(f'Tempo inválido: {path}')
    return rows


def estimate_amdahl(times):
    x = [1 / p for p in POINTS]
    y = [time / times[0] for time in times]
    f_raw = sum((1 - a) * (b - a) for a, b in zip(x, y)) / sum((1 - a) ** 2 for a in x)
    f_bounded = min(1.0, max(0.0, f_raw))
    residuals = [b - (f_bounded + (1 - f_bounded) * a) for a, b in zip(x, y)]
    mean_x, mean_y = sum(x) / len(x), sum(y) / len(y)
    slope = sum((a - mean_x) * (b - mean_y) for a, b in zip(x, y)) / sum((a - mean_x) ** 2 for a in x)
    return dict(f_raw=f_raw, f_bounded=f_bounded,
                rmse_normalized=(sum(r * r for r in residuals) / len(residuals)) ** .5,
                f_aula03_regression_raw=mean_y - slope * mean_x,
                f_effective_by_point=[dict(workers=p, f=(b - a) / (1 - a))
                                      for p, a, b in zip(POINTS[1:], x[1:], y[1:])])


def main():
    corpus = read_rows(ROOT / 'resultados/speedup_corpus.csv')
    pi = read_rows(ROOT / 'resultados/speedup.csv')
    corpus_times = [float(row['t_total']) for row in corpus]
    pi_times = [float(row['t_total']) for row in pi]
    corpus_speedup = [corpus_times[0] / time for time in corpus_times]
    node_seconds = [int(row['nnodes']) * time for row, time in zip(corpus, corpus_times)]
    pi_speedup = [pi_times[0] / time for time in pi_times]
    plt.rcParams.update({'font.size': 11})
    fig, ax = plt.subplots(figsize=(9, 5.5))
    ax.plot(POINTS, POINTS, '--', color='#88919a', label='Linear ideal: S(p) = p')
    ax.plot(POINTS, pi_speedup, 'o-', color='#2366a2', linewidth=2, label='pi_mpi: menor tempo de duas séries')
    ax.plot(POINTS, corpus_speedup, 's-', color='#bd4b32', linewidth=2, label='Corpus Dask: mediana de três repetições')
    ax.set_xscale('log', base=2)
    ax.set_yscale('log', base=2)
    ax.set_xticks(POINTS, [str(p) for p in POINTS])
    ax.set_yticks([.5, 1, 2, 4, 8, 16, 32], ['0,5', '1', '2', '4', '8', '16', '32'])
    ax.set_xlabel('Processos MPI / workers Dask')
    ax.set_ylabel('Speedup: T(1) / T(p) — escala log base 2')
    ax.set_ylim(.45, 36)
    ax.grid(alpha=.2)
    ax.legend(loc='upper left')
    ax.set_title('Strong scaling: corpus × pi_mpi × linear ideal', pad=12)
    fig.text(.5, .015, 'Cada série usa seu próprio T(1). As janelas de medição e os critérios de agregação diferem.',
             ha='center', fontsize=9, color='#505965')
    fig.tight_layout(rect=(0, .04, 1, 1))
    output = Path(__file__).resolve().parent / 'comparativo-speedup.png'
    fig.savefig(output, dpi=180)
    plt.close(fig)
    report = dict(points=POINTS, corpus_times=corpus_times, pi_times=pi_times,
                  corpus_speedup=corpus_speedup, pi_speedup=pi_speedup,
                  corpus_node_seconds=node_seconds,
                  corpus_node_seconds_relative=[value / node_seconds[0] for value in node_seconds],
                  corpus_amdahl=estimate_amdahl(corpus_times), pi_amdahl=estimate_amdahl(pi_times))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print('Figura:', output)


if __name__ == '__main__':
    main()
