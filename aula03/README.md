# aula03-codigo: MPI e SLURM (HPC Aula 3, Inteli 2026.2)

Siga o roteiro-lab-aula03-mpi-slurm.md. Resumo:

    module load gnu15 openmpi5
    make                              # hello_mpi, pingpong, soma_reduce, pi_mpi
    sbatch job_hello.sbatch           # Bloco 1
    sbatch -N 1 job_pingpong.sbatch   # Bloco 2 (e -N 2 --ntasks-per-node=1)
    sbatch -n 8 job_soma.sbatch       # Bloco 3
    sbatch -n 4 -N 4 job_pi.sbatch    # Bloco 4: um job solto para ler a saida
    ./speedup.sh                      # seis jobs encadeados: 1, 2, 4, 8, 16, 32
    python3 analisa_speedup.py        # tabela, f de Amdahl, speedup.png (se houver matplotlib)

Arquivos:
- hello_mpi.c, pingpong.c, soma_reduce.c, pi_mpi.c, Makefile
- job_hello.sbatch, job_pingpong.sbatch, job_soma.sbatch, job_pi.sbatch
- speedup.sh, analisa_speedup.py
