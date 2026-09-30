#!/bin/bash
#SBATCH -J pingpong
#SBATCH -o pingpong-%j.out
#SBATCH -n 2
#SBATCH -t 00:05:00
# O -N (numero de nos) fica de fora de proposito: voce escolhe na linha de comando.
#   mesmo no:        sbatch -N 1 job_pingpong.sbatch
#   nos diferentes:  sbatch -N 2 --ntasks-per-node=1 job_pingpong.sbatch

module load gnu15 openmpi5
echo "job $SLURM_JOB_ID em $SLURM_JOB_NODELIST"
prun ./pingpong
