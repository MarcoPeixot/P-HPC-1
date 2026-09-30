#!/bin/bash
#SBATCH -J hello            # nome do job (aparece no squeue)
#SBATCH -o hello-%j.out     # saida: %j vira o numero do job
#SBATCH -N 4                # 4 nos
#SBATCH -n 8                # 8 processos no total (2 por no)
#SBATCH -t 00:02:00         # tempo maximo

module load gnu15 openmpi5   # os nos rodam da RAM: o job precisa carregar os modulos

echo "job $SLURM_JOB_ID em $SLURM_JOB_NODELIST"
prun ./hello_mpi
