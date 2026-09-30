#!/bin/bash
# speedup.sh: submete a serie de seis jobs (1, 2, 4, 8, 16, 32 processos)
# em cadeia, um depois do outro, para que nenhum compartilhe CPU com outro.
#
# Uso: ./speedup.sh                  (4 bilhoes de pontos)
#      PONTOS=1000000000 ./speedup.sh
#
# Por que cada linha tem -N e --ntasks-per-node? Porque queremos controlar
# ONDE os processos caem. Ate 16 processos, um por core fisico
# (--hint=nomultithread); em 32, dois por core (SMT). Sem isso o SLURM
# poderia colocar 16 processos em 8 cores de 2 nos e a curva mentiria.
set -e
mkdir -p resultados
export PONTOS
[ -f resultados/speedup.csv ] || echo "nprocs,nnodes,t_total,t_serial,t_calc" > resultados/speedup.csv

dep=""
submete() {   # submete <n> <opcoes slurm...>
    local n=$1; shift
    local id
    id=$(sbatch --parsable $dep -n "$n" "$@" job_pi.sbatch)
    echo "n=$n  job $id  ($*)"
    dep="--dependency=afterany:$id"
}

submete  1 -N 1 --hint=nomultithread
submete  2 -N 1 --hint=nomultithread
submete  4 -N 1 --hint=nomultithread
submete  8 -N 2 --ntasks-per-node=4 --hint=nomultithread
submete 16 -N 4 --ntasks-per-node=4 --hint=nomultithread
submete 32 -N 4 --ntasks-per-node=8 --ntasks-per-core=2

echo
echo "Acompanhe com: squeue   |   quando terminar: python3 analisa_speedup.py"
