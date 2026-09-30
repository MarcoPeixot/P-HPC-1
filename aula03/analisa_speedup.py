#!/usr/bin/env python3
"""analisa_speedup.py: le resultados/speedup.csv e calcula speedup,
eficiencia e a fracao serial estimada pela lei de Amdahl.

Uso: python3 analisa_speedup.py [arquivo.csv]

Se houver mais de uma medicao para o mesmo numero de processos, usa a
menor (a mais representativa; as outras costumam ter cache frio ou
interferencia). Se o matplotlib estiver instalado, salva speedup.png.
So depende da biblioteca padrao para a tabela.
"""
import csv
import sys

ARQ = sys.argv[1] if len(sys.argv) > 1 else "resultados/speedup.csv"

# ---- 1. ler e ficar com o menor tempo por numero de processos ----
melhor = {}
with open(ARQ) as f:
    for lin in csv.DictReader(f):
        try:
            p = int(lin["nprocs"]); t = float(lin["t_total"])
        except (KeyError, ValueError):
            continue
        if p not in melhor or t < melhor[p]["t_total"]:
            melhor[p] = {"t_total": t, "t_serial": float(lin["t_serial"]),
                         "t_calc": float(lin["t_calc"]), "nnodes": lin["nnodes"]}

if 1 not in melhor:
    sys.exit("preciso da medicao com 1 processo para calcular o speedup")

ps = sorted(melhor)
t1 = melhor[1]["t_total"]

# ---- 2. tabela ----
print("%6s %6s %10s %9s %9s %9s %8s" % ("procs", "nos", "t_total", "t_serial", "t_calc", "speedup", "efic."))
for p in ps:
    m = melhor[p]
    S = t1 / m["t_total"]
    print("%6d %6s %10.3f %9.3f %9.3f %9.2f %7.0f%%" %
          (p, m["nnodes"], m["t_total"], m["t_serial"], m["t_calc"], S, 100 * S / p))

# ---- 3. Amdahl: 1/S = f + (1-f)/p  ->  reta em x = 1/p ----
# Ajuste por minimos quadrados de y = a + b*x, com f = a e (1-f) = b.
xs = [1.0 / p for p in ps]
ys = [melhor[p]["t_total"] / t1 for p in ps]     # = 1/S
n = len(xs)
mx, my = sum(xs) / n, sum(ys) / n
b = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / max(sum((x - mx) ** 2 for x in xs), 1e-12)
a = my - b * mx
f = min(max(a, 0.0), 1.0)
print()
print("fracao serial estimada (Amdahl): f = %.3f  ->  speedup maximo teorico 1/f = %s"
      % (f, ("%.1f" % (1 / f)) if f > 0 else "infinito"))
print("com f = %.3f, Amdahl preve:" % f, "  ".join(
    "S(%d)=%.2f" % (p, 1 / (f + (1 - f) / p)) for p in ps))

# ---- 4. grafico, se der ----
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    S = [t1 / melhor[p]["t_total"] for p in ps]
    ideal = ps
    amd = [1 / (f + (1 - f) / p) for p in ps]
    plt.figure(figsize=(7, 4.5))
    plt.plot(ps, ideal, "--", color="#999999", label="ideal (S = p)")
    plt.plot(ps, amd, "-", color="#FF4546", label="Amdahl, f = %.3f" % f)
    plt.plot(ps, S, "o-", color="#2E2540", label="medido")
    plt.xscale("log", base=2); plt.xticks(ps, [str(p) for p in ps])
    plt.xlabel("processos"); plt.ylabel("speedup"); plt.grid(alpha=0.3); plt.legend()
    plt.title("Curva de speedup: pi_mpi")
    plt.tight_layout(); plt.savefig("speedup.png", dpi=150)
    print("grafico salvo em speedup.png")
except ImportError:
    print("(matplotlib ausente: cole a tabela numa planilha ou no Colab para plotar)")
