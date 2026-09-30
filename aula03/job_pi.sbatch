#!/bin/bash
#SBATCH -J pi
#SBATCH -o resultados/pi-%j.out
#SBATCH -t 00:10:00
#SBATCH --exclusive
# -n, -N e afins vem da linha de comando (veja speedup.sh).
# Uso manual: sbatch -n 8 -N 4 job_pi.sbatch

module load gnu15 openmpi5
PONTOS=${PONTOS:-4000000000}

echo "job $SLURM_JOB_ID: $SLURM_NTASKS processos em $SLURM_JOB_NUM_NODES no(s): $SLURM_JOB_NODELIST"
prun ./pi_mpi "$PONTOS" > resultados/ultimo-$SLURM_JOB_ID.txt
cat resultados/ultimo-$SLURM_JOB_ID.txt
grep "^CSV" resultados/ultimo-$SLURM_JOB_ID.txt | cut -d, -f2- >> resultados/speedup.csv
rm -f resultados/ultimo-$SLURM_JOB_ID.txt
