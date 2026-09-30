#!/usr/bin/env bash
#SBATCH --job-name=corpus
#SBATCH --partition=normal
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --exclusive
#SBATCH --time=00:30:00
#SBATCH --output=logs/corpus-%j.out
set -euo pipefail
WORKERS=${1:?workers}
BATCH=${2:?batch directory}
PER_NODE=$((WORKERS / SLURM_NNODES))
if ((WORKERS % SLURM_NNODES)); then exit 2; fi
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1
source /opt/ohpc/pub/apps/miniforge3/bin/activate hpc
cd "$SLURM_SUBMIT_DIR"
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
SCHED="$BATCH/scheduler-$SLURM_JOB_ID.json"
mkdir -p "$BATCH"
SCHED_PID=''
WORKER_PID=''
cleanup() {
  if [[ -n "$WORKER_PID" ]]; then kill "$WORKER_PID" 2>/dev/null || true; fi
  if [[ -n "$SCHED_PID" ]]; then kill "$SCHED_PID" 2>/dev/null || true; fi
  rm -f "$SCHED"
}
trap cleanup EXIT
dask scheduler --scheduler-file "$SCHED" --port 8786 --dashboard-address :8787 > "$BATCH/scheduler-$SLURM_JOB_ID.log" 2>&1 &
SCHED_PID=$!
for _ in $(seq 1 60); do
  [[ -s "$SCHED" ]] && break
  kill -0 "$SCHED_PID"
  sleep 1
done
[[ -s "$SCHED" ]]
srun --ntasks="$SLURM_NNODES" --ntasks-per-node=1 --cpu-bind=none \
  dask worker --scheduler-file "$SCHED" --nworkers "$PER_NODE" --nthreads 1 \
  --memory-limit 0 --local-directory "/tmp/corpus-$SLURM_JOB_ID" \
  > "$BATCH/workers-$SLURM_JOB_ID.log" 2>&1 &
WORKER_PID=$!
for repetition in 1 2 3; do
  printf -v label 'w%02d-r%d' "$WORKERS" "$repetition"
  python pipeline.py --scheduler-file "$SCHED" --workers "$WORKERS" --nodes "$SLURM_NNODES" \
    --repetition "$repetition" --output "$BATCH/$label"
done
