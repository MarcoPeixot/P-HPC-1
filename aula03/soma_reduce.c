/* soma_reduce.c
 * Exercicio 2: coletivas. O rank 0 decide o tamanho N do problema e avisa
 * todo mundo com MPI_Bcast. Cada rank soma a sua fatia de 1..N e o
 * MPI_Reduce junta tudo no rank 0. Conferimos contra a formula N(N+1)/2.
 *
 * Compilar:  mpicc -O2 -o soma_reduce soma_reduce.c
 * Rodar:     sbatch -n 8 job_soma.sbatch      (troque o -n e observe)
 */
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv) {
    int rank, size;
    long long N = 0;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (rank == 0) {
        N = (argc > 1) ? atoll(argv[1]) : 2000000000LL;   /* 2 bilhoes por padrao */
        printf("N = %lld, %d processos\n", N, size);
    }

    /* Somente o rank 0 conhece N. Bcast: um fala, todos ouvem. */
    MPI_Bcast(&N, 1, MPI_LONG_LONG, 0, MPI_COMM_WORLD);

    /* Cada rank pega uma fatia contigua de 1..N */
    long long fatia = N / size;
    long long ini = rank * fatia + 1;
    long long fim = (rank == size - 1) ? N : ini + fatia - 1;

    double t0 = MPI_Wtime();
    volatile long long parcial = 0;      /* volatile: impede o compilador de trocar o laco pela formula */
    for (long long i = ini; i <= fim; i++) parcial += i;
    double dt = MPI_Wtime() - t0;

    printf("rank %2d somou de %11lld a %11lld: parcial = %lld (%.3f s)\n",
           rank, ini, fim, (long long)parcial, dt);

    /* Reduce: todos falam, um junta (aqui com soma). */
    long long total = 0, p = parcial;
    MPI_Reduce(&p, &total, 1, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        long long esperado = N * (N + 1) / 2;
        printf("total    = %lld\n", total);
        printf("esperado = %lld  -> %s\n", esperado, (total == esperado) ? "OK" : "ERRADO");
    }

    MPI_Finalize();
    return 0;
}
