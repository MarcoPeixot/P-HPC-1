#!/usr/bin/env bash
set -euo pipefail
if [[ $(id -u) -eq 0 ]]; then echo 'Submeta como g02.' >&2; exit 1; fi
cd "$(dirname "$0")"
BATCH=${1:-"$PWD/resultados/run-$(date +%Y%m%d-%H%M%S)"}
if [[ -e "$BATCH" ]]; then echo "Lote já existe: $BATCH" >&2; exit 1; fi
mkdir -p "$BATCH" logs
BATCH=$(cd "$BATCH" && pwd)
source /opt/ohpc/pub/apps/miniforge3/bin/activate hpc
python - <<'PY'
from pathlib import Path
import hashlib,json
base=Path('/opt/ohpc/pub/datasets/b2w-reviews01')
m=json.loads((base/'manifest.json').read_text())
assert m['documents'] >= 50000
for item in m['files']:
    assert hashlib.sha256((base/item['path']).read_bytes()).hexdigest()==item['sha256'],item['path']
print('Dataset verificado:',m['documents'],'documentos')
PY
previous=''
for workers in 1 2 4 8 16 32; do
  case "$workers" in
    1|2|4) nodes=1;;
    8) nodes=2;;
    16|32) nodes=4;;
  esac
  options=(--parsable --nodes="$nodes")
  [[ -z "$previous" ]] || options+=(--dependency="afterany:$previous")
  job=$(sbatch "${options[@]}" job_corpus.sbatch "$workers" "$BATCH")
  printf '%s,%s,%s\n' "$workers" "$nodes" "$job" | tee -a "$BATCH/slurm_jobs.csv"
  previous=$job
done
printf 'Lote: %s\nAgregue ao terminar: python agregar.py %q --output ../resultados/speedup_corpus.csv\n' "$BATCH" "$BATCH"
