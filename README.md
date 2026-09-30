# P-HPC-1: MPI e PLN distribuído

Entrega das Partes 1 e 2 da ponderada de HPC: experimentos MPI da Aula 3 e pipeline de PLN distribuído com Dask.

## Estrutura do repositório

```
P-HPC-1/
├── aula03/               # Parte 1 — Aula 3: códigos e logs MPI
├── pipeline/             # Parte 2 — esta aula: pipeline TF-IDF com Dask
├── resultados/           # CSVs de resultado de ambas as partes
│   ├── bloco2-pingpong.csv      # Aula 3: latência e largura de banda MPI
│   ├── bloco3-soma.csv          # Aula 3: soma paralela com MPI_Reduce
│   ├── speedup.csv              # Aula 3: curva de speedup do pi_mpi
│   ├── speedup_corpus.csv       # Esta aula: curva de speedup do pipeline NLP
│   ├── sha256-originais.txt     # Aula 3: hashes dos arquivos MPI originais
│   └── pipeline/                # Esta aula: medições individuais por rodada
├── analise/              # Anotações de análise
├── RELATORIO_AULA03.md   # Relatório completo da Parte 1
├── ROTEIRO_PARTE02.md    # Passo a passo de execução da Parte 2
└── speedup.png           # Aula 3: gráfico ideal × Amdahl × medido
```

---

## Parte 1 — Aula 3: experimentos MPI

Experimentos de comunicação e paralelismo com MPI em cluster SLURM. Conferido com o roteiro da Aula 3, Prof. João Luisi, Inteli 2026.2.

### Códigos (`aula03/`)

| Arquivo | O que faz |
|---|---|
| `pingpong.c` | Comunicação ponto a ponto: mede latência e vazão entre dois processos para 1 B, 1 KiB e 1 MiB |
| `soma_reduce.c` | Soma paralela de 1..2×10⁹ com `MPI_Bcast` + `MPI_Reduce`; valida contra N(N+1)/2 |
| `pi_mpi.c` | Estimativa de π por Monte Carlo; decompõe tempo em parte serial e paralela |
| `hello_mpi.c` | Hello world MPI para verificar ambiente |
| `analisa_speedup.py` | Lê `resultados/speedup.csv` e imprime speedup, eficiência e fração serial (Amdahl) |
| `speedup.sh` | Submete a série de 6 jobs de pi (1, 2, 4, 8, 16, 32 processos) em cadeia no SLURM |

### Resultados da Aula 3 (`resultados/`)

| Arquivo | Conteúdo |
|---|---|
| `bloco2-pingpong.csv` | Latência (µs) e vazão (MB/s) para mesmo nó e nós diferentes, nos três tamanhos de mensagem |
| `bloco3-soma.csv` | Tempo do rank 0 e tempo máximo de cálculo para 1, 2, 4, 8 e 32 processos; coluna de validação |
| `speedup.csv` | Tempo total, serial e de cálculo do pi_mpi para 6 configurações; mediana dos menores tempos entre as duas séries |
| `speedup.png` | Curva de speedup: ideal, Amdahl (f estimado) e medido, eixo x em log₂ |
| `sha256-originais.txt` | SHA-256 dos arquivos `.c` e logs usados na análise |

O relatório completo (tabelas dos Blocos 2 e 3, cinco respostas do experimento de π e análise de Amdahl) está em [RELATORIO_AULA03.md](RELATORIO_AULA03.md).

Para recalcular speedup e eficiência localmente:

```bash
python3 aula03/analisa_speedup.py resultados/speedup.csv
```

---

## Parte 2 — Pipeline de PLN distribuído

Pipeline TF-IDF global sobre o corpus [B2W-Reviews01](https://github.com/americanas-tech/b2w-reviews01) (avaliações de produtos em português), executado com Dask em cluster SLURM do grupo do laboratório do Inteli.

O corpus tem 129.098 documentos com `review_text` não vazio, preparados em 128 arquivos JSONL a partir de 132.373 registros originais (47,16 MiB). Fica em `/opt/ohpc/pub/datasets/b2w-reviews01` no NFS, somente leitura nos workers. Licença CC BY-NC-SA 4.0, atribuição à B2W Digital.

### Códigos (`pipeline/`)

| Arquivo | O que faz |
|---|---|
| `pipeline.py` | Tokenização Unicode, remoção de stopwords, TF-IDF com IDF global, normalização L2; salva matrizes esparsas CSR/NPZ |
| `preparar_dataset.py` | Converte o CSV original em shards JSONL e gera o manifest com hashes |
| `agregar.py` | Lê os `measurement.json` de cada rodada e produz `speedup_corpus.csv` com as medianas |
| `job_corpus.sbatch` | Job SLURM: sobe scheduler e workers Dask, roda 3 repetições por configuração |
| `submeter.sh` | Submete as 6 configurações (1, 2, 4, 8, 16, 32 workers) em sequência |
| `executar_tudo.sh` | Submissão + agregação em um único comando |
| `test_pipeline.py` | Testes de correção: tokenização, IDF, normalização L2 |
| `experimento_pln.ipynb` | Notebook com preparação, execução, análise, tabelas e gráficos |
| `dataset-manifest.json` | Fonte, contagens, tamanhos e SHA-256 de cada shard |

### Resultados dessa aula (HPC-5) (`resultados/`)

| Arquivo | Conteúdo |
|---|---|
| `speedup_corpus.csv` | Mediana de t_total, t_serial e t_calc para 6 configurações de workers (mesmo cabeçalho do speedup MPI) |
| `pipeline/` | Medições individuais (`measurement.json`) e logs dos jobs por lote |

O relatório de resultados, gargalos e análise da curva de speedup está em [pipeline/RELATORIO.md](pipeline/RELATORIO.md). O passo a passo de reprodução está em [ROTEIRO_PARTE02.md](ROTEIRO_PARTE02.md).

Para rodar do zero no cluster:

```bash
bash pipeline/executar_tudo.sh
```
