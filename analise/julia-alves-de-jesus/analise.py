"""Gera as figuras e imprime os números de analise/julia-alves-de-jesus/julia-alves-de-jesus.md.

Entradas: resultados/speedup.csv (pi_mpi), resultados/speedup_corpus.csv e as
18 medições do lote run-20260930-corpus02.

Uso, na raiz do repositório, com pipeline/requirements.txt instalado:
    python analise/julia-alves-de-jesus/analise.py
"""
import csv
import json
from pathlib import Path

import numpy as np
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

AQUI = Path(__file__).resolve().parent
REPO = AQUI.parents[1]
LOTE = REPO / 'resultados/pipeline/run-20260930-corpus02'
PS = [1, 2, 4, 8, 16, 32]
NOS = {1: 1, 2: 1, 4: 1, 8: 2, 16: 4, 32: 4}

GRENA, VERDE, CINZA = '#870A28', '#00613C', '#3C3C3C'
CINZA_CLARO, GRADE, FUNDO = '#8A8A8A', '#E6E4E0', '#FCFCFB'

plt.rcParams.update({
    'font.family': ['Avenir Next', 'Gill Sans', 'DejaVu Sans'],
    'font.size': 10.5, 'axes.titlesize': 13, 'axes.titleweight': 'demibold', 'axes.titlelocation': 'left',
    'axes.titlepad': 12, 'axes.labelcolor': CINZA, 'xtick.color': CINZA, 'ytick.color': CINZA,
    'axes.edgecolor': GRADE, 'figure.facecolor': FUNDO, 'axes.facecolor': FUNDO,
    'savefig.facecolor': FUNDO, 'legend.frameon': False,
})


def br(valor, casas=2):
    return f'{valor:.{casas}f}'.replace('.', ',')


def ler(path):
    with open(path) as f:
        return {int(r['nprocs']): {k: float(v) for k, v in r.items()} for r in csv.DictReader(f)}


def estilo(ax):
    for lado in ('top', 'right'):
        ax.spines[lado].set_visible(False)
    ax.grid(color=GRADE, linewidth=.8)
    ax.set_axisbelow(True)


def rotulo_p(p, unidade=''):
    return f'{p}{unidade}\n{NOS[p]} nó' + ('s' if NOS[p] > 1 else '')


def karp_flatt(s, p):
    return (1 / s - 1 / p) / (1 - 1 / p)


pi = ler(REPO / 'resultados/speedup.csv')
corpus = ler(REPO / 'resultados/speedup_corpus.csv')
medicoes = {p: [json.loads((LOTE / f'w{p:02d}-r{r}/measurement.json').read_text()) for r in (1, 2, 3)] for p in PS}

s_pi = {p: pi[1]['t_total'] / pi[p]['t_total'] for p in PS}
s_co = {p: corpus[1]['t_total'] / corpus[p]['t_total'] for p in PS}
kf_pi = {p: karp_flatt(s_pi[p], p) for p in PS[1:]}
kf_co = {p: karp_flatt(s_co[p], p) for p in PS[1:]}
f_med = corpus[1]['t_serial'] / corpus[1]['t_total']
f_amdahl = kf_co[2]  # Amdahl invertido no único trecho em que S cresce (1 -> 2 workers)
b_pi, a_pi = np.polyfit([1 / p for p in PS], [1 / s_pi[p] for p in PS], 1)
b_co, a_co = np.polyfit([1 / p for p in PS], [1 / s_co[p] for p in PS], 1)

print('p | nós | S_pi | E_pi | S_corpus | E_corpus | t_serial | t_calc | KF_pi | KF_corpus')
for p in PS:
    kf = lambda d: '-' if p == 1 else f'{d[p]:.3f}'
    print(f"{p} | {NOS[p]} | {s_pi[p]:.3f} | {100 * s_pi[p] / p:.1f}% | {s_co[p]:.3f} | {100 * s_co[p] / p:.1f}% | "
          f"{corpus[p]['t_serial']:.3f} | {corpus[p]['t_calc']:.3f} | {kf(kf_pi)} | {kf(kf_co)}")
print(f'\nAjuste de Amdahl nos 6 pontos: pi f = {a_pi:.4f}; corpus intercepto = {a_co:.4f}, coef. de 1/p = {b_co:.4f}')
print(f'f medido em 1 worker = {f_med:.4f}; 1/f = {1 / f_med:.2f}; S previsto: '
      + ', '.join(f'{p}: {1 / (f_med + (1 - f_med) / p):.2f}' for p in PS))
print(f'f pela lei de Amdahl (p = 2) = {f_amdahl:.4f}; 1/f = {1 / f_amdahl:.2f}; S previsto: '
      + ', '.join(f'{p}: {1 / (f_amdahl + (1 - f_amdahl) / p):.2f}' for p in PS))
print(f't_serial/t_total em 32 workers: {corpus[32]["t_serial"] / corpus[32]["t_total"]:.3f}')
print('Primeira repetição menos a mediana de t_calc: ' + ', '.join(
    f"{p}: {medicoes[p][0]['t_calc'] - corpus[p]['t_calc']:+.3f}" for p in PS))
print('Aumento de t_serial por worker: ' + ', '.join(
    f'{a}->{b}: {(corpus[b]["t_serial"] - corpus[a]["t_serial"]) / (b - a):.3f}' for a, b in zip(PS, PS[1:])))

# ---------------------------------------------------- figura 1: três curvas
fig, ax = plt.subplots(figsize=(9, 5.6))
estilo(ax)
ax.axhspan(0.35, 1, color=GRENA, alpha=.05, lw=0)
ax.text(1.06, 0.395, 'abaixo de 1: mais lento que com 1 worker', color=CINZA_CLARO, fontsize=9)
ax.axvspan(22.6, 45, color=CINZA, alpha=.06, lw=0)
ax.text(31, 2.6, 'SMT\n2 threads\npor core', color=CINZA_CLARO, fontsize=9, ha='center')
ax.plot(PS, PS, '--', color=CINZA, lw=1.6, label='Linear ideal (S = p)')
ax.plot(PS, [s_pi[p] for p in PS], 'o-', color=VERDE, lw=2.4, ms=7.5, mec=FUNDO, mew=1.6,
        label='pi_mpi, Aula 3 (MPI)')
ax.plot(PS, [s_co[p] for p in PS], 'o-', color=GRENA, lw=2.4, ms=7.5, mec=FUNDO, mew=1.6,
        label='Corpus B2W (Dask, pipeline completo)')
for p in PS:
    ax.annotate(br(s_co[p]), (p, s_co[p]), textcoords='offset points', xytext=(0, 10 if p <= 4 else -16),
                ha='center', color=GRENA, fontsize=9, fontweight='demibold')
for p in (4, 8, 16, 32):
    ax.annotate(br(s_pi[p], 1), (p, s_pi[p]), textcoords='offset points', xytext=(8, -13), ha='left',
                color=VERDE, fontsize=9, fontweight='demibold')
ax.set_xscale('log', base=2)
ax.set_yscale('log', base=2)
ax.set_xticks(PS, [rotulo_p(p) for p in PS])
ax.set_yticks([0.5, 1, 2, 4, 8, 16, 32], ['0,5', '1', '2', '4', '8', '16', '32'])
ax.minorticks_off()
ax.set_xlim(0.85, 45)
ax.set_ylim(0.35, 45)
ax.set_xlabel('Processos MPI (pi_mpi) ou workers Dask (corpus)')
ax.set_ylabel('Speedup  S(p) = T(1) / T(p)')
ax.set_title('Speedup: corpus, pi_mpi e linear ideal')
ax.legend(loc='upper left')
fig.tight_layout()
fig.savefig(AQUI / 'fig1-speedup-tres-curvas.png', dpi=170)

# ------------------------------------------ figura 2: decomposição do tempo
# Mesmos valores do CSV do grupo e do texto: mediana de cada coluna. Como as
# medianas são tiradas por coluna, t_calc + t_serial pode diferir do t_total
# mediano em alguns centésimos; o total é marcado à parte, com o valor do CSV.
fig, ax = plt.subplots(figsize=(9, 5.4))
estilo(ax)
ax.grid(axis='x', visible=False)
x = np.arange(len(PS))
calc = [corpus[p]['t_calc'] for p in PS]
ser = [corpus[p]['t_serial'] for p in PS]
tot = [corpus[p]['t_total'] for p in PS]
ax.bar(x, calc, width=.6, color=VERDE, edgecolor=FUNDO, linewidth=2,
       label='t_calc: etapas nos workers, espera, comunicação, I/O')
ax.bar(x, ser, width=.6, bottom=calc, color=GRENA, edgecolor=FUNDO, linewidth=2,
       label='t_serial: cliente (DF, vocabulário/IDF, broadcast, agregação)')
ax.hlines(tot, x - .34, x + .34, color=CINZA, lw=2, label='t_total mediano')
for i in range(len(PS)):
    ax.text(i, calc[i] * .42, br(calc[i]), ha='center', va='center', color='white', fontsize=9, fontweight='demibold')
    # Rótulo do t_serial na primeira posição que não encosta nas linhas de Amdahl.
    linhas = [corpus[1]['t_total'] * (f + (1 - f) / PS[i]) for f in (f_amdahl, f_med)]
    y = next((calc[i] + ser[i] * k for k in (.5, .7, .3, .85, .15)
              if all(abs(calc[i] + ser[i] * k - v) > .35 for v in linhas)), calc[i] + ser[i] / 2)
    ax.text(i, y, br(ser[i]), ha='center', va='center', color='white', fontsize=9, fontweight='demibold')
    ax.text(i, max(tot[i], calc[i] + ser[i]) + .2, f'{br(tot[i])} s', ha='center', va='bottom', color=CINZA,
            fontsize=9.5, fontweight='demibold')
print('Diferença entre soma das medianas e t_total mediano: ' + ', '.join(
    f'{p}: {calc[i] + ser[i] - tot[i]:+.3f}' for i, p in enumerate(PS)))
for f, estilo_linha, cor, rotulo in [(f_amdahl, 'D--', CINZA, f'Amdahl com f = {br(f_amdahl)} (estimado do speedup com 2 workers)'),
                                     (f_med, 'o:', CINZA_CLARO, f'Amdahl com f = {br(f_med)} (medido com 1 worker)')]:
    ax.plot(x, [corpus[1]['t_total'] * (f + (1 - f) / p) for p in PS], estilo_linha, color=cor, lw=1.4, ms=5,
            label=rotulo)
ax.set_xticks(x, [rotulo_p(p, ' worker' + ('s' if p > 1 else '')) for p in PS])
ax.set_ylim(0, 14)
ax.set_ylabel('Tempo de parede (s), mediana de 3 repetições')
ax.set_title('Corpus: a parte no cliente cresce com os workers')
ax.legend(loc='upper left', fontsize=9.5)
fig.tight_layout()
fig.savefig(AQUI / 'fig2-decomposicao-do-tempo.png', dpi=170)

# ------------------------------------------------------- figura 3: Karp-Flatt
fig, ax = plt.subplots(figsize=(9, 4.8))
estilo(ax)
ps2 = PS[1:]
ax.axhline(1, color=CINZA, lw=1.1, ls=':')
ax.text(32, 0.55, 'e > 1 equivale a S < 1: o tempo aumentou', color=CINZA_CLARO, fontsize=9, ha='right')
ax.axhline(f_med, color=GRENA, lw=1.1, ls='--', alpha=.7)
ax.text(2, f_med * 1.15, f'f = {br(f_med)} (medido com 1 worker)', color=GRENA, fontsize=9)
ax.plot(ps2, [kf_co[p] for p in ps2], 'o-', color=GRENA, lw=2.4, ms=7.5, mec=FUNDO, mew=1.6, label='Corpus')
ax.plot(ps2, [kf_pi[p] for p in ps2], 'o-', color=VERDE, lw=2.4, ms=7.5, mec=FUNDO, mew=1.6, label='pi_mpi')
for p in ps2:
    ax.annotate(br(kf_co[p]), (p, kf_co[p]), textcoords='offset points', xytext=(0, 10 if kf_co[p] > 1 else -16),
                ha='center', color=GRENA, fontsize=9, fontweight='demibold')
    ax.annotate(br(kf_pi[p], 3), (p, kf_pi[p]), textcoords='offset points', xytext=(0, -16), ha='center',
                color=VERDE, fontsize=9, fontweight='demibold')
ax.set_xscale('log', base=2)
ax.set_yscale('log')
ax.set_xticks(ps2, [str(p) for p in ps2])
ax.minorticks_off()
ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: br(v, 3).rstrip('0').rstrip(',')))
ax.set_ylim(5e-4, 4)
ax.set_xlabel('Processos / workers')
ax.set_ylabel('e(p) = (1/S - 1/p) / (1 - 1/p)')
ax.set_title('Fração serial experimental (Karp-Flatt)')
ax.legend(loc='center right')
fig.tight_layout()
fig.savefig(AQUI / 'fig3-karp-flatt.png', dpi=170)
print('\nFiguras geradas em', AQUI.relative_to(REPO))
