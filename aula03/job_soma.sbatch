#!/bin/bash
#SBATCH -J soma
#SBATCH -o soma-%j.out
#SBATCH -n 8
#SBATCH -t 00:05:00
# Troque o -n na linha de comando: sbatch -n 1 job_soma.sbatch ... sbatch -n 32 job_soma.sbatch

module load gnu15 openmpi5
echo "job $SLURM_JOB_ID em $SLURM_JOB_NODELIST"
prun ./soma_reduce 2000000000
