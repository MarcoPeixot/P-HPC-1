/* pingpong.c
 * Exercicio 1: comunicacao ponto a ponto com MPI_Send / MPI_Recv.
 * O rank 0 manda uma mensagem ao rank 1, que devolve. Repetimos muitas
 * vezes e medimos o tempo de ida e volta para tres tamanhos de mensagem.
 * Precisa de exatamente 2 processos.
 *
 * Compilar:  mpicc -O2 -o pingpong pingpong.c
 * Mesmo no:       sbatch -n 2 -N 1 job_pingpong.sbatch
 * Nos diferentes: sbatch -n 2 -N 2 --ntasks-per-node=1 job_pingpong.sbatch
 */
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main(int argc, char **argv) {
    int rank, size;
    char host[64];
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    gethostname(host, sizeof(host));

    if (size != 2) {
        if (rank == 0) fprintf(stderr, "pingpong precisa de exatamente 2 processos (recebeu %d)\n", size);
        MPI_Finalize();
        return 1;
    }

    /* Cada processo diz onde esta: assim voces enxergam se o teste foi
       dentro de um no ou atravessando o switch. */
    printf("rank %d em %s\n", rank, host);
    MPI_Barrier(MPI_COMM_WORLD);

    long tamanhos[]   = {1, 1024, 1024 * 1024};   /* 1 B, 1 KiB, 1 MiB */
    int  repeticoes[] = {10000, 10000, 200};
    char *buf = malloc(1024 * 1024);

    for (int t = 0; t < 3; t++) {
        long n = tamanhos[t];
        int  rep = repeticoes[t];
        MPI_Barrier(MPI_COMM_WORLD);
        double t0 = MPI_Wtime();
        for (int i = 0; i < rep; i++) {
            if (rank == 0) {
                MPI_Send(buf, n, MPI_CHAR, 1, 0, MPI_COMM_WORLD);
                MPI_Recv(buf, n, MPI_CHAR, 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            } else {
                MPI_Recv(buf, n, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                MPI_Send(buf, n, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
            }
        }
        double dt = MPI_Wtime() - t0;
        if (rank == 0) {
            double ida_volta_us = dt / rep * 1e6;          /* microssegundos por ida e volta */
            double mb_s = (2.0 * n * rep) / dt / 1e6;      /* MB/s (ida + volta)             */
            printf("mensagem de %8ld bytes: ida e volta = %9.2f us   vazao = %8.1f MB/s\n",
                   n, ida_volta_us, mb_s);
        }
    }

    free(buf);
    MPI_Finalize();
    return 0;
}
