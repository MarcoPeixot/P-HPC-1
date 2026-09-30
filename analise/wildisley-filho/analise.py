"""Métricas e figuras da análise individual (analise/wildisley-filho.md).

Lê somente arquivos versionados no repositório:
  resultados/speedup.csv                      pi_mpi da Aula 3 (seis pontos)
  resultados/speedup_corpus.csv               medianas do pipeline de PLN
  resultados/pipeline/run-20260930-corpus02/  18 medições individuais
  resultados/bloco2-pingpong.csv              latência e vazão entre nós
  analise/wildisley-filho/volumes_locais.json            saída de medir_volumes.py

Uso: python analise/wildisley-filho/analise.py
Gera as figuras PNG neste diretório e imprime as tabelas usadas no texto.
"""
import csv
import json
import math
import statistics
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FixedLocator, FuncFormatter, NullLocator  # noqa: E402
import numpy as np  # noqa: E402

AQUI = Path(__file__).resolve().parent
REPO = AQUI.parents[1]
LOTE = REPO / 'resultados' / 'pipeline' / 'run-20260930-corpus02'
PS = [1, 2, 4, 8, 16, 32]

# Convenção única para todas as figuras: só azul e laranja; o cinza neutro fica para o linear
# ideal e para as linhas de referência. Azul = pipeline de PLN e sua parcela distribuída;
# laranja = termo de comparação (pi_mpi, seção serial, Lei de Amdahl clássica).
# Linha contínua com marcador = medido; linha tracejada = curva teórica ou ajustada.
SUPERFICIE, TINTA, TINTA_2, MUDA = '#fcfcfb', '#0b0b0b', '#52514e', '#898781'
GRADE, EIXO = '#e1e0d9', '#c3c2b7'
AZUL, LARANJA = '#2a78d6', '#eb6834'
COR = dict(
    pln=AZUL,        # pipeline de PLN: t_total, speedup e modelo estendido ajustado a ele
    calc=AZUL,       # t_calc, parcela distribuída do PLN (Figuras 2 e 5)
    pi=LARANJA,      # pi_mpi da Aula 3 (Figuras 1 e 3)
    serial=LARANJA,  # t_serial, seção serial do PLN (Figuras 2 e 5)
    amdahl=LARANJA,  # previsões da Lei de Amdahl clássica (Figura 4)
    ideal=MUDA,      # linear ideal, referência neutra (Figura 1)
)
MARCA = dict(pln='s', calc='s', serial='s', pi='o')   # quadrado = PLN; círculo = pi_mpi

plt.rcParams.update({
    'font.family': 'sans-serif',
    'font.sans-serif': ['Helvetica Neue', 'Helvetica', 'Arial', 'DejaVu Sans'],
    'font.size': 10, 'axes.titlesize': 11, 'axes.labelsize': 10,
    'figure.facecolor': SUPERFICIE, 'axes.facecolor': SUPERFICIE, 'savefig.facecolor': SUPERFICIE,
    'axes.edgecolor': EIXO, 'axes.linewidth': 1.0, 'axes.labelcolor': TINTA_2,
    'xtick.color': MUDA, 'ytick.color': MUDA, 'xtick.labelcolor': TINTA_2, 'ytick.labelcolor': TINTA_2,
    'axes.grid': True, 'grid.color': GRADE, 'grid.linewidth': 0.8, 'grid.linestyle': '-',
    'axes.spines.top': False, 'axes.spines.right': False, 'legend.frameon': False,
    'lines.linewidth': 2, 'lines.solid_capstyle': 'round', 'lines.solid_joinstyle': 'round',
})


def virgula(valor, casas=2):
    return f'{valor:.{casas}f}'.replace('.', ',')


def ler_csv(caminho):
    with open(caminho) as f:
        return {int(r['nprocs']): {k: float(v) for k, v in r.items()} for r in csv.DictReader(f)}


def lstsq(colunas, y):
    a = np.column_stack(colunas)
    coef, *_ = np.linalg.lstsq(a, y, rcond=None)
    ajuste = a @ coef
    r2 = 1 - np.sum((y - ajuste) ** 2) / np.sum((y - np.mean(y)) ** 2)
    return coef, r2


def karp_flatt(s, p):
    return (1 / s - 1 / p) / (1 - 1 / p)


def amdahl(f, p):
    return 1 / (f + (1 - f) / np.asarray(p, dtype=float))


def eixo_log2(ax, y=False):
    ax.set_xscale('log', base=2)
    ax.xaxis.set_major_locator(FixedLocator(PS))
    ax.xaxis.set_minor_locator(NullLocator())
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f'{v:g}'))
    ax.set_xlim(0.85, 40)
    if y:
        ax.set_yscale('log', base=2)
        ax.yaxis.set_minor_locator(NullLocator())


def medido(ax, x, y, papel, rotulo):
    ax.plot(x, y, '-', color=COR[papel], zorder=3, label=rotulo)
    ax.plot(x, y, MARCA[papel], color=COR[papel], markersize=7, markeredgecolor=SUPERFICIE,
            markeredgewidth=1.5, zorder=4)


def teorico(ax, x, y, papel, rotulo, traco='--'):
    ax.plot(x, y, traco, color=COR[papel], linewidth=1.6, zorder=2, label=rotulo)


def referencia(ax, y):
    ax.axhline(y, color=EIXO, linewidth=1, zorder=1)


def salvar(fig, nome):
    fig.savefig(AQUI / nome, dpi=200, bbox_inches='tight')
    plt.close(fig)


# ---------------------------------------------------------------- dados
pi = ler_csv(REPO / 'resultados' / 'speedup.csv')
corpus = ler_csv(REPO / 'resultados' / 'speedup_corpus.csv')
reps = {p: [json.loads((LOTE / f'w{p:02d}-r{r}' / 'measurement.json').read_text()) for r in (1, 2, 3)] for p in PS}
for p in PS:  # o CSV versionado deve ser exatamente a mediana das medições individuais
    for k in ('t_total', 't_serial', 't_calc'):
        assert math.isclose(round(statistics.median(m[k] for m in reps[p]), 6), corpus[p][k], abs_tol=1e-6)
with open(REPO / 'resultados' / 'bloco2-pingpong.csv') as f:
    ping = {int(r['bytes']): r for r in csv.DictReader(f)}
alfa = float(ping[1]['nos_diferentes_us']) / 2 * 1e-6          # latência de ida, s
beta = float(ping[1048576]['vazao_entre_nos_MB_s']) * 1e6        # bytes/s
vol = json.loads((AQUI / 'volumes_locais.json').read_text())

t = {nome: np.array([dados[p]['t_total'] for p in PS]) for nome, dados in (('pi', pi), ('corpus', corpus))}
ts = np.array([corpus[p]['t_serial'] for p in PS])
tc = np.array([corpus[p]['t_calc'] for p in PS])
pa = np.array(PS, dtype=float)
S = {k: v[0] / v for k, v in t.items()}
E = {k: S[k] / pa for k in S}
Sc = tc[0] / tc

# ---------------------------------------------------------------- estimativas de f
estimativas = {}
for nome in ('pi', 'corpus'):
    (a, b), r2 = lstsq([np.ones(6), 1 / pa], 1 / S[nome])
    estimativas[f'{nome}_mmq_todos'] = dict(a=a, b=b, f=min(max(a, 0), 1), r2=r2)
(a, b), r2 = lstsq([np.ones(3), 1 / pa[:3]], 1 / S['corpus'][:3])
estimativas['corpus_mmq_1no'] = dict(a=a, b=b, f=min(max(a, 0), 1), r2=r2)
estimativas['corpus_instrumentada'] = dict(f=ts[0] / t['corpus'][0])
cpu = vol['cpu_s_mediana']
estimativas['corpus_algoritmica'] = dict(f=(cpu['serial_combinacao'] + cpu['serial_agregacao']) / sum(cpu.values()))
estimativas['corpus_karp_flatt_p2'] = dict(f=karp_flatt(S['corpus'][1], 2))

# Amdahl estendida: T(p) = T1*[f + (1-f)/p] + c*log2(p), com T1 medido.
# Em T(p) - T1/p = f*T1*(1 - 1/p) + c*log2(p), o problema é linear em (f, c).
T1 = t['corpus'][0]
y_ext = (t['corpus'] - T1 / pa)[1:]
g_ext = np.log2(pa)[1:]


def sobrecarga(f):
    """Ajusta somente c para f fixo; devolve c, R² sobre T(p) e p ótimo analítico."""
    residuo = y_ext - f * T1 * (1 - 1 / pa[1:])
    c = float(g_ext @ residuo / (g_ext @ g_ext))
    previsto = T1 * (f + (1 - f) / pa) + c * np.log2(pa)
    r2 = 1 - np.sum((t['corpus'] - previsto) ** 2) / np.sum((t['corpus'] - t['corpus'].mean()) ** 2)
    return c, r2, T1 * (1 - f) * math.log(2) / c


# Ajuste conjunto de (f, c) por mínimos quadrados, com f restrito a [0, 1] (varredura fina de f).
grade_f = np.linspace(0, 1, 1001)
sse = [np.sum((y_ext - f * T1 * (1 - 1 / pa[1:]) - max(sobrecarga(f)[0], 0) * g_ext) ** 2) for f in grade_f]
f_conjunto = float(grade_f[int(np.argmin(sse))])
# Componentes: t_calc ~ a + b/p (Amdahl na parcela distribuída); t_serial ~ s0 + s1*log2(p)
(ac, bc), r2_calc = lstsq([np.ones(6), 1 / pa], tc)
(s0, s1), r2_serial = lstsq([np.ones(6), np.log2(pa)], ts)

# ---------------------------------------------------------------- orçamento de rede (Hockney)
B = vol['bytes']
bcast = B['vocabulario'] + B['idf']
orcamento = {}
for p, n in ((1, 1), (32, 4)):
    remota = (n - 1) / n                       # cliente e scheduler ficam em c1
    fases = {
        'leitura NFS dos shards': B['shards_jsonl'],
        'escrita NFS das matrizes NPZ': B['npz_escrita'],
        'gather de metadados (DF, comprimentos)': B['metadados_gather'] * remota,
        'broadcast de vocabulário e IDF': bcast * (p - p // n),
        'gather de resultados (somas TF-IDF)': B['resultados_gather'] * remota,
    }
    orcamento[p] = {k: (v, v / beta) for k, v in fases.items()}


# ---------------------------------------------------------------- relatório no terminal
def linha(*celulas):
    print('| ' + ' | '.join(str(c) for c in celulas) + ' |')


print('## Speedup e eficiência')
for i, p in enumerate(PS):
    linha(p, virgula(S['pi'][i], 3), virgula(100 * E['pi'][i], 1), virgula(S['corpus'][i], 3),
          virgula(100 * E['corpus'][i], 1), virgula(Sc[i], 3),
          '-' if p == 1 else virgula(karp_flatt(S['pi'][i], p), 4),
          '-' if p == 1 else virgula(karp_flatt(S['corpus'][i], p), 3))
print('\n## Estimativas de f')
for k, v in estimativas.items():
    print(k, {x: round(y, 5) for x, y in v.items()})
f0, fa = estimativas['corpus_instrumentada']['f'], estimativas['corpus_algoritmica']['f']
print(f'Amdahl estendida, ajuste conjunto com f em [0, 1]: f = {f_conjunto:.3f}, c/r2/p* =',
      [round(x, 4) for x in sobrecarga(f_conjunto)])
for rotulo, f in (('f algorítmica', fa), ('f instrumentada', f0)):
    c, r2, pot = sobrecarga(f)
    previsto = T1 / (T1 * (f + (1 - f) / pa) + c * np.log2(pa))
    print(f'Amdahl estendida com {rotulo} = {f:.4f}: c = {c:.4f} s por duplicação, R2 = {r2:.4f}, p* = {pot:.2f};',
          'S previsto:', [virgula(x, 3) for x in previsto])
print(f't_calc = {ac:.4f} + {bc:.4f}/p  R2={r2_calc:.4f}  f_calc={ac / (ac + bc):.4f}')
print(f't_serial = {s0:.4f} + {s1:.4f}*log2(p)  R2={r2_serial:.4f}')
print('Amdahl f0:', [virgula(x, 2) for x in amdahl(f0, PS)], 'limite', virgula(1 / f0, 2))
print('Amdahl f_alg:', [virgula(x, 2) for x in amdahl(fa, PS)], 'limite', virgula(1 / fa, 2))
print('\n## Variabilidade (t_total, t_serial, t_calc): min / mediana / max / CV%')
for p in PS:
    cel = [p]
    for k in ('t_total', 't_serial', 't_calc'):
        v = [m[k] for m in reps[p]]
        cel.append(f"{virgula(min(v), 3)} / {virgula(statistics.median(v), 3)} / {virgula(max(v), 3)} / "
                   f"{virgula(100 * statistics.stdev(v) / statistics.mean(v), 1)}")
    cel.append(virgula(reps[p][0]['t_total'] - statistics.mean(m['t_total'] for m in reps[p][1:]), 3))
    cel.append(' '.join(f'{h}:{n}' for h, n in sorted(reps[p][0]['partitions_by_host'].items())))
    linha(*cel)
print('\n## Incrementos de t_serial por duplicação de workers')
print([virgula(x, 3) for x in np.diff(ts)])
print('\n## Orçamento de rede: alfa =', virgula(alfa * 1e6, 1), 'us; beta =', virgula(beta / 1e6, 1), 'MB/s')
for p, fases in orcamento.items():
    total_b = sum(b for b, _ in fases.values())
    total_t = sum(x for _, x in fases.values())
    for k, (b, x) in fases.items():
        linha(p, k, virgula(b / 1e6, 2), virgula(x, 3))
    linha(p, 'TOTAL', virgula(total_b / 1e6, 2), virgula(total_t, 3))
excesso = t['corpus'][-1] - t['corpus'][0] / 32
print('excesso sobre o ideal em p=32:', virgula(excesso, 3), 's; fração explicável pela banda:',
      virgula(100 * sum(x for _, x in orcamento[32].values()) / excesso, 1), '%')
print('granularidade: t_calc(1)/256 tarefas =', virgula(1e3 * tc[0] / 256, 1), 'ms; partições por worker:',
      [128 // p for p in PS])
print('SMT 16->32: corpus t_calc', virgula(tc[4] / tc[5], 3), '; pi t_calc',
      virgula(pi[16]['t_calc'] / pi[32]['t_calc'], 3), '; pi t_total', virgula(t['pi'][4] / t['pi'][5], 3))

# ---------------------------------------------------------------- Figura 1: speedup, três curvas
fig, ax = plt.subplots(figsize=(7.6, 4.8))
eixo_log2(ax, y=True)
ax.set_ylim(0.4, 45)
ax.yaxis.set_major_locator(FixedLocator([0.5, 1, 2, 4, 8, 16, 32]))
ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f'{v:g}'.replace('.', ',')))
referencia(ax, 1)
teorico(ax, PS, PS, 'ideal', 'Linear ideal (S = p)')
medido(ax, PS, S['pi'], 'pi', 'pi_mpi, Aula 3 (MPI)')
medido(ax, PS, S['corpus'], 'pln', 'Pipeline de PLN (Dask)')
ax.annotate('Ideal: 32', (32, 32), xytext=(6, 0), textcoords='offset points', va='center', color=TINTA_2)
ax.annotate(f"pi_mpi: {virgula(S['pi'][-1], 1)}", (32, S['pi'][-1]), xytext=(6, -2),
            textcoords='offset points', va='center', color=TINTA)
ax.annotate(f"PLN: {virgula(S['corpus'][-1], 2)}", (32, S['corpus'][-1]), xytext=(6, 0),
            textcoords='offset points', va='center', color=TINTA)
ax.annotate(f"máximo do PLN: {virgula(S['corpus'][1], 2)} (p = 2)", (2, S['corpus'][1]), xytext=(10, 9),
            textcoords='offset points', ha='left', color=TINTA_2, fontsize=9)
ax.annotate('S = 1 (sem ganho)', (30, 1), xytext=(0, 4), textcoords='offset points', ha='right', color=MUDA, fontsize=9)
ax.set_xlabel('Processos MPI (pi_mpi) ou workers Dask (PLN), escala logarítmica de base 2')
ax.set_ylabel('Speedup S(p) = T(1) / T(p), escala logarítmica de base 2')
ax.legend(loc='upper left')
salvar(fig, 'fig1-speedup-tres-curvas.png')

# ---------------------------------------------------------------- Figura 2: decomposição do tempo
fig, ax = plt.subplots(figsize=(7.6, 4.4))
x = np.arange(6)
largura = 0.5
ax.bar(x, tc, largura, color=COR['calc'], edgecolor=SUPERFICIE, linewidth=1.5, label='t_calc: fases distribuídas, espera, comunicação e E/S', zorder=3)
ax.bar(x, ts, largura, bottom=tc, color=COR['serial'], edgecolor=SUPERFICIE, linewidth=1.5, label='t_serial: coordenação no cliente e broadcast', zorder=3)
for i, p in enumerate(PS):
    for m in reps[p]:
        ax.plot(i + largura / 2 + 0.1, m['t_total'], '_', color=TINTA_2, markersize=9, markeredgewidth=1.8, zorder=4)
    ax.annotate(f"{virgula(t['corpus'][i], 2)} s", (i, t['corpus'][i]), xytext=(0, 5), textcoords='offset points',
                ha='center', color=TINTA, fontsize=9)
ax.plot([], [], '_', color=TINTA_2, markersize=9, markeredgewidth=1.8, label='t_total de cada repetição (n = 3)')
nos = {p: int(corpus[p]['nnodes']) for p in PS}
ax.set_xticks(x, [f'{p}\n{nos[p]} nó' + ('s' if nos[p] > 1 else '') for p in PS])
ax.grid(axis='x', visible=False)
ax.set_ylim(0, 13.5)
ax.set_xlabel('Workers Dask e nós alocados')
ax.set_ylabel('Tempo de parede, mediana (s)')
ax.legend(loc='upper left', fontsize=9)
salvar(fig, 'fig2-decomposicao-tempo.png')

# ---------------------------------------------------------------- Figura 3: métrica de Karp-Flatt
fig, ax = plt.subplots(figsize=(7.6, 4.2))
eixo_log2(ax)
ax.set_xlim(1.7, 40)
ax.set_yscale('log')
ax.yaxis.set_minor_locator(NullLocator())
ax.yaxis.set_major_locator(FixedLocator([0.001, 0.01, 0.1, 1]))
ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f'{v:g}'.replace('.', ',')))
ax.set_ylim(0.0007, 3)
kf = {k: karp_flatt(S[k][1:], pa[1:]) for k in S}
referencia(ax, 1)
ax.annotate('e = 1: acima desta linha, S(p) < 1', (38, 1), xytext=(0, -5), textcoords='offset points',
            ha='right', va='top', color=MUDA, fontsize=9)
medido(ax, PS[1:], kf['corpus'], 'pln', 'Pipeline de PLN (Dask)')
medido(ax, PS[1:], kf['pi'], 'pi', 'pi_mpi, Aula 3 (MPI)')
ax.annotate(f"{virgula(kf['corpus'][-1], 2)}", (32, kf['corpus'][-1]), xytext=(7, 0), textcoords='offset points', va='center', color=TINTA)
ax.annotate(f"{virgula(kf['pi'][-1], 4)}", (32, kf['pi'][-1]), xytext=(7, 0), textcoords='offset points', va='center', color=TINTA)
ax.set_xlabel('Processos ou workers, escala logarítmica de base 2')
ax.set_ylabel('Fração serial experimental e(p), escala log')
ax.legend(loc='lower right')
salvar(fig, 'fig3-karp-flatt.png')

# ---------------------------------------------------------------- Figura 4: modelos para o pipeline
pf = np.geomspace(1, 32, 200)
fig, ax = plt.subplots(figsize=(7.6, 4.6))
eixo_log2(ax, y=True)
ax.set_ylim(0.4, 24)
ax.yaxis.set_major_locator(FixedLocator([0.5, 1, 2, 4, 8, 16]))
ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f'{v:g}'.replace('.', ',')))
referencia(ax, 1)
c_fa, r2_fa, _ = sobrecarga(fa)
modelo_ext = T1 / (T1 * (fa + (1 - fa) / pf) + c_fa * np.log2(pf))
teorico(ax, pf, amdahl(fa, pf), 'amdahl', f'Amdahl clássica, f algorítmica = {virgula(100 * fa, 1)}%')
teorico(ax, pf, amdahl(f0, pf), 'amdahl', f'Amdahl clássica, f instrumentada = {virgula(100 * f0, 1)}%', '-.')
teorico(ax, pf, modelo_ext, 'pln',
        rf'Amdahl estendida: f = {virgula(100 * fa, 1)}% + {virgula(c_fa, 2)} s · $\log_2 p$ (R² = {virgula(r2_fa, 3)})')
medido(ax, PS, S['corpus'], 'pln', 'PLN medido (mediana)')
ax.annotate(f'f = {virgula(100 * fa, 1)}%: {virgula(float(amdahl(fa, 32)), 2)}', (32, float(amdahl(fa, 32))),
            xytext=(6, 0), textcoords='offset points', va='center', color=TINTA_2)
ax.annotate(f'f = {virgula(100 * f0, 1)}%: {virgula(float(amdahl(f0, 32)), 2)}', (32, float(amdahl(f0, 32))),
            xytext=(6, 0), textcoords='offset points', va='center', color=TINTA_2)
ax.annotate(f"medido: {virgula(S['corpus'][-1], 2)}", (32, S['corpus'][-1]), xytext=(6, 0),
            textcoords='offset points', va='center', color=TINTA)
ax.set_xlabel('Workers Dask, escala logarítmica de base 2')
ax.set_ylabel('Speedup S(p), escala logarítmica de base 2')
ax.legend(loc='upper left', fontsize=9)
salvar(fig, 'fig4-modelos-amdahl.png')

# ---------------------------------------------------------------- Figura 5: leis de escala das componentes
fig, (e1, e2) = plt.subplots(1, 2, figsize=(9.2, 3.9))
for ax in (e1, e2):
    eixo_log2(ax)
teorico(e1, pf, ac + bc / pf, 'calc', f'ajuste a + b/p (R² = {virgula(r2_calc, 3)})')
medido(e1, PS, tc, 'calc', 't_calc medido (mediana)')
e1.set_title('(a) Parcela distribuída: t_calc', loc='left', color=TINTA)
e1.set_ylim(0, 6)
e1.annotate('passa de 1 para 2 nós', (8, tc[3]), xytext=(8, 14), textcoords='offset points', color=TINTA_2, fontsize=9)
teorico(e2, pf, s0 + s1 * np.log2(pf), 'serial', rf'ajuste $s_0 + s_1 \log_2 p$ (R² = {virgula(r2_serial, 3)})')
medido(e2, PS, ts, 'serial', 't_serial medido (mediana)')
e2.set_title('(b) Seção serial: t_serial', loc='left', color=TINTA)
e2.set_ylim(0, 11)
for ax in (e1, e2):
    ax.set_xlabel('Workers Dask, escala logarítmica de base 2')
    ax.set_ylabel('Tempo (s)')
    ax.legend(loc='upper left' if ax is e2 else 'upper right', fontsize=8.5)
fig.tight_layout(w_pad=3)
salvar(fig, 'fig5-componentes.png')
print('\nfiguras salvas em', AQUI)
