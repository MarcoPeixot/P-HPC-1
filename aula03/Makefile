# make        -> compila os quatro programas (carregue os modulos antes: module load gnu15 openmpi5)
# make clean  -> apaga binarios e saidas de jobs
CC     = mpicc
CFLAGS = -O2 -Wall
PROGS  = hello_mpi pingpong soma_reduce pi_mpi

all: $(PROGS)

$(PROGS): %: %.c
	$(CC) $(CFLAGS) -o $@ $<

clean:
	rm -f $(PROGS) *.out

.PHONY: all clean
