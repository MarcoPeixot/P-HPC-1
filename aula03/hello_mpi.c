/* hello_mpi.c
 * Cada processo se apresenta: rank, tamanho do comunicador e o no onde roda.
 * Compilar:  mpicc -O2 -o hello_mpi hello_mpi.c
 * Rodar:     prun ./hello_mpi     (dentro de um job SLURM)
 */
#include <mpi.h>
#include <stdio.h>
#include <unistd.h>

int main(int argc, char **argv) {
    int rank, size;
    char host[64];

    MPI_Init(&argc, &argv);                     /* todo processo comeca aqui    */
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);       /* quem sou eu (0..size-1)      */
    MPI_Comm_size(MPI_COMM_WORLD, &size);       /* quantos somos                */
    gethostname(host, sizeof(host));

    printf("ola do rank %d de %d, rodando em %s\n", rank, size, host);

    MPI_Finalize();                             /* e todo processo termina aqui */
    return 0;
}
