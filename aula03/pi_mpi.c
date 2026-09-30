/* pi_mpi.c
 * Experimento central do modulo: estimar pi por Monte Carlo com MPI.
 *
 * Sorteamos pontos (x, y) no quadrado [0,1) x [0,1). A fracao que cai dentro
 * do quarto de circulo de raio 1 e pi/4. Cada rank sorteia a sua parte dos
 * pontos com uma semente propria, conta os que cairam dentro, e MPI_Reduce
 * soma tudo no rank 0, que calcula pi e imprime os tempos.
 *
 * Nao le arquivo, nao troca dados durante o calculo: quase tudo e paralelo.
 * A parte serial que sobra e o arranque (MPI_Init, Bcast) e o Reduce final.
 *
 * Compilar: mpicc -O2 -o pi_mpi pi_mpi.c
 * Uso:      pi_mpi [pontos]     (padrao 4 bilhoes; use 1e9 em maquina fraca)
 */
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>

/* xorshift64: gerador rapido e simples, um estado por rank */
static inline uint64_t xorshift64(uint64_t *s) {
    uint64_t x = *s;
    x ^= x << 13; x ^= x >> 7; x ^= x << 17;
    return *s = x;
}

int main(int argc, char **argv) {
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    MPI_Barrier(MPI_COMM_WORLD);
    double t_ini = MPI_Wtime();

    /* ---------- 1. Arranque: rank 0 decide N e avisa todo mundo ---------- */
    long long N = 0;
    if (rank == 0) N = (argc > 1) ? atoll(argv[1]) : 4000000000LL;
    MPI_Bcast(&N, 1, MPI_LONG_LONG, 0, MPI_COMM_WORLD);

    long long meus = N / size + (rank < N % size ? 1 : 0);   /* fatia de cada rank */
    uint64_t semente = 0x9E3779B97F4A7C15ULL * (uint64_t)(rank + 1);  /* diferente por rank */

    double t_calc0 = MPI_Wtime();

    /* ---------- 2. Calculo: sortear e contar (100% paralelo) ---------- */
    long long dentro = 0;
    const double escala = 1.0 / 18446744073709551616.0;   /* 2^-64: uint64 -> [0,1) */
    for (long long i = 0; i < meus; i++) {
        double x = xorshift64(&semente) * escala;
        double y = xorshift64(&semente) * escala;
        if (x * x + y * y < 1.0) dentro++;
    }
    double t_calc = MPI_Wtime() - t_calc0;

    /* ---------- 3. Reducao: todos falam, o rank 0 junta ---------- */
    long long dentro_tot = 0;
    double t_calc_max;
    MPI_Reduce(&dentro, &dentro_tot, 1, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&t_calc, &t_calc_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    double t_total = MPI_Wtime() - t_ini;

    if (rank == 0) {
        const char *nnodes = getenv("SLURM_JOB_NUM_NODES");
        double pi = 4.0 * (double)dentro_tot / (double)N;
        printf("%lld pontos, %d processos em %s no(s)\n", N, size, nnodes ? nnodes : "1");
        printf("pi estimado = %.8f   erro = %.2e\n", pi, fabs(pi - M_PI));
        printf("tempo: total %.3f s | calculo (max) %.3f s | arranque+reducao %.3f s\n",
               t_total, t_calc_max, t_total - t_calc_max);
        /* Linha que o speedup.sh coleta. Nao mude o formato. */
        printf("CSV,%d,%s,%.4f,%.4f,%.4f\n", size, nnodes ? nnodes : "1",
               t_total, t_total - t_calc_max, t_calc_max);
    }

    MPI_Finalize();
    return 0;
}
