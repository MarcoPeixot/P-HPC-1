#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
source /opt/ohpc/pub/apps/miniforge3/bin/activate hpc

if [[ ${1:-} == --agregar ]]; then
  BATCH=${2:?diretório do lote}
  for workers in 1 2 4 8 16 32; do
    for repetition in 1 2 3; do
      printf -v label 'w%02d-r%d' "$workers" "$repetition"
      [[ -f "$BATCH/$label/measurement.json" ]] || {
        echo "Medição ausente: $label" >&2
        exit 1
      }
    done
  done
  exec python agregar.py "$BATCH" --output "$BATCH/speedup_corpus.csv"
fi

if [[ $(id -u) -eq 0 ]]; then
  echo 'Execute como usuário comum do cluster, por exemplo g02.' >&2
  exit 1
fi
BATCH=${1:-"$PWD/resultados/run-$(date +%Y%m%d-%H%M%S)"}
if [[ -e "$BATCH" ]]; then
  echo "Lote já existe: $BATCH" >&2
  exit 1
fi
python preparar_dataset.py
bash submeter.sh "$BATCH"
BATCH=$(cd "$BATCH" && pwd)
JOB_IDS=$(cut -d, -f3 "$BATCH/slurm_jobs.csv" | paste -sd: -)
printf -v COMMAND '%q ' bash "$PWD/executar_tudo.sh" --agregar "$BATCH"
AGGREGATION=$(sbatch --parsable --partition=normal --nodes=1 --ntasks=1 \
  --cpus-per-task=1 --mem=1G --time=00:10:00 \
  --job-name=agregar-corpus --dependency="afterok:$JOB_IDS" \
  --output="$BATCH/aggregation-%j.log" --wrap="$COMMAND")
printf 'Agregação: job %s\nCSV ao concluir: %s/speedup_corpus.csv\nAcompanhe com: squeue -u %s\n' \
  "$AGGREGATION" "$BATCH" "$(id -un)"
