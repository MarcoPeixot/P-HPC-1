#!/usr/bin/env python3
"""Gera o gráfico comparativo usando os CSVs do repositório."""
import csv
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[2]


def load_speedup(path):
    with path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    times = {int(row["nprocs"]): float(row["t_total"]) for row in rows}
    baseline = times[1]
    return {workers: baseline / total for workers, total in times.items()}


def load_corpus_repetitions(workers):
    run_dir = ROOT / "resultados" / "pipeline" / "run-20260930-corpus02"
    measurements = sorted(run_dir.glob(f"w{workers:02d}-r*/measurement.json"))
    if len(measurements) != 3:
        raise ValueError(f"Esperava 3 medições para {workers} workers; encontrei {len(measurements)}")
    return [json.loads(path.read_text(encoding="utf-8"))["t_total"]
            for path in measurements]


corpus = load_speedup(ROOT / "resultados" / "speedup_corpus.csv")
pi_mpi = load_speedup(ROOT / "resultados" / "speedup.csv")
workers = sorted(set(corpus) & set(pi_mpi))
baseline = sorted(load_corpus_repetitions(1))[1]
corpus_ranges = {p: load_corpus_repetitions(p) for p in workers}
lower = [corpus[p] - baseline / max(corpus_ranges[p]) for p in workers]
upper = [baseline / min(corpus_ranges[p]) - corpus[p] for p in workers]

fig, ax = plt.subplots(figsize=(8.5, 5.2))
ax.plot(workers, [workers_n for workers_n in workers], "k--", label="Ideal")
ax.errorbar(workers, [corpus[p] for p in workers], yerr=[lower, upper],
            fmt="o-", linewidth=2, capsize=4,
            label="Pipeline do corpus (Dask; faixa de 3 execuções)")
ax.plot(workers, [pi_mpi[p] for p in workers], "s-", linewidth=2,
        label="pi_mpi (MPI)")
ax.set_xscale("log", base=2)
ax.set_xticks(workers, [str(p) for p in workers])
ax.set_xlabel("Workers Dask / processos MPI")
ax.set_ylabel("Speedup S(p) = T(1) / T(p)")
ax.set_title("Speedup do pipeline textual e do pi_mpi")
ax.grid(True, which="both", alpha=0.28)
ax.legend()
fig.tight_layout()
fig.savefig(Path(__file__).with_name("speedup-comparativo.png"), dpi=180)
