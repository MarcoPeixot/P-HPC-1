# Roteiro — Parte 2: pipeline de PLN distribuído

Roteiro de execução da implementação deste repositório, organizado a partir dos requisitos da atividade. Os resultados já obtidos estão no [relatório da Parte 2](pipeline/RELATORIO.md); os detalhes do protocolo estão em [pipeline/README.md](pipeline/README.md).

## 1. Objetivo e entregáveis

Usar um dataset textual público com pelo menos 50.000 documentos no NFS, executar tokenização, remoção de stopwords, TF-IDF e estatísticas do corpus, e medir seis configurações de workers com três repetições cada.

Ao concluir, a entrega deve conter:

- Fonte, quantidade de documentos, tamanho em disco, formato e idioma do dataset.
- Código das quatro etapas do pipeline.
- Scripts de submissão via Slurm e evidências das 18 execuções.
- `resultados/speedup_corpus.csv` com seis linhas de medianas e o cabeçalho `nprocs,nnodes,t_total,t_serial,t_calc`.
- Estatísticas: tamanho do vocabulário, distribuição de tamanho dos documentos e termos de maior TF-IDF.

## 2. Conferir o cluster e o ambiente

No master, como usuário `g02`:

```bash
cd /home/g02/corpus
source /opt/ohpc/pub/apps/miniforge3/bin/activate hpc
sinfo -Nel
which python
python --version
python -c 'import dask, distributed, numpy; print(dask.__version__, distributed.__version__, numpy.__version__)'
```

Os quatro nós `c1`, `c2`, `c3` e `c4` precisam estar disponíveis para as configurações de 16 e 32 workers. O ambiente validado usa Python 3.11.16, Dask/Distributed 2026.8.0 e NumPy 2.4.6.

| Conteúdo | Caminho no cluster |
| --- | --- |
| Dataset e stopwords, leitura pelos workers | `/opt/ohpc/pub/datasets/b2w-reviews01` |
| Código e scripts | `/home/g02/corpus` |
| Resultados, matrizes e logs | `/home/g02/corpus/resultados` |

O ambiente e o dataset em `/opt/ohpc/pub` são compartilhados por NFS e montados somente para leitura nos workers. Código de execução e saídas ficam em `/home/g02/corpus`, compartilhado e gravável.

## 3. Preparar e descrever o dataset

A implementação usa [B2W-Reviews01, da B2W Digital](https://github.com/americanas-tech/b2w-reviews01), com avaliações de produtos em português brasileiro, sob licença CC BY-NC-SA 4.0.

| Medida | Valor da preparação validada |
| --- | --- |
| Registros no original | 132.373 |
| Documentos utilizados | 129.098 campos `review_text` não vazios |
| Registros sem texto excluídos | 3.275 |
| Tamanho do CSV original | 49.453.175 bytes, aproximadamente 47,16 MiB |
| Formato original | CSV UTF-8, separado por vírgulas, com aspas e campos multilinha |
| Entrada do pipeline | 128 shards JSONL UTF-8 com ID e texto |
| Tamanho de shards e stopwords | 21.430.941 bytes, aproximadamente 20,44 MiB |
| Idioma | Português brasileiro |

Para preparar o dataset, executar **no master com permissão de escrita no diretório do dataset**:

```bash
python3.11 /home/g02/corpus/preparar_dataset.py
```

O [preparador](pipeline/preparar_dataset.py) baixa snapshots fixados, confere hashes, exclui textos vazios, prepara as partições e salva `manifest.json`. Quando a preparação já existe, confere os arquivos e reutiliza o dataset. O original completo permanece no NFS; o repositório contém [os metadados](pipeline/dataset-manifest.json), o código e os resultados.

## 4. Conferir as quatro etapas

O executor está em [pipeline/pipeline.py](pipeline/pipeline.py):

1. **Tokenização:** normaliza Unicode, converte para minúsculas com `casefold` e extrai sequências de letras, preservando acentos.
2. **Stopwords:** remove a lista portuguesa fixada de 207 termos. Documentos que ficam sem tokens permanecem com vetor zero.
3. **TF-IDF:** calcula frequência documental global, IDF suavizado e vetores normalizados em L2. Todas as partições usam o mesmo vocabulário e IDF; as matrizes são CSR em NPZ.
4. **Estatísticas:** reúne vocabulário, histogramas e medidas de comprimento antes/depois das stopwords, e os 30 termos com maior soma de TF-IDF normalizado no corpus.

Antes do benchmark, no master com o ambiente `hpc` ativado:

```bash
cd /home/g02/corpus
python test_pipeline.py
bash -n job_corpus.sbatch submeter.sh
```

Os testes conferem IDF global, normalização L2, vetores vazios, invariância entre workers, integridade do dataset e agregação das medianas.

## 5. Submeter as seis configurações

| Workers | Nós | Workers por nó | Repetições |
| ---: | ---: | ---: | ---: |
| 1 | 1 | 1 | 3 |
| 2 | 1 | 2 | 3 |
| 4 | 1 | 4 | 3 |
| 8 | 2 | 4 | 3 |
| 16 | 4 | 4 | 3 |
| 32 | 4 | 8 | 3 |

Cada worker usa uma thread. A configuração de um worker é single-node. Aos 32 workers, são usadas as CPUs lógicas com SMT. As alocações são exclusivas e os jobs são encadeados para executar um após o outro.

Como `g02`, definir um **novo diretório de lote** e submeter:

```bash
cd /home/g02/corpus
source /opt/ohpc/pub/apps/miniforge3/bin/activate hpc
LOTE="$PWD/resultados/run-$(date +%Y%m%d-%H%M%S)"
bash submeter.sh "$LOTE"
squeue -u g02
cat "$LOTE/slurm_jobs.csv"
```

O [submissor](pipeline/submeter.sh) valida o dataset e chama `sbatch` para cada configuração. O [job](pipeline/job_corpus.sbatch) inicia scheduler e workers, executa três repetições do pipeline e encerra os processos ao terminar. O script recusa um lote já existente, preservando as execuções anteriores.

## 6. Conferir as execuções e calcular medianas

Depois que os jobs terminarem, na mesma sessão em que `LOTE` foi definido:

```bash
cd /home/g02/corpus
python agregar.py "$LOTE" --output resultados/speedup_corpus.csv
cat resultados/speedup_corpus.csv
```

Para retomar em outra sessão, atribuir a `LOTE` o caminho completo do lote criado antes da agregação.

A [agregação](pipeline/agregar.py) exige três repetições completas por configuração, matrizes presentes, topologia correta, pelo menos 50.000 documentos e identidade de dataset, protocolo e código. A saída deve indicar as seis configurações completas e `missing: []`. Se houver configuração ausente, conferir as medições e os logs antes de considerar a entrega concluída.

O CSV final usa:

```csv
nprocs,nnodes,t_total,t_serial,t_calc
```

`nprocs` representa workers Dask. Cada coluna de tempo é a mediana independente das três repetições:

- `t_total`: leitura, quatro etapas, comunicação e escrita das matrizes.
- `t_serial`: coordenação medida no cliente, combinação de DF/vocabulário, distribuição de IDF e agregação final.
- `t_calc`: restante do tempo de parede, incluindo fases distribuídas, espera, comunicação e I/O.

Startup, validação de hashes e exportação final de JSON/vocabulário ficam fora da medição. As medianas das parcelas podem não somar exatamente à mediana total. As repetições não limpam caches e seguem ordem fixa; esse protocolo precisa ser mantido ao comparar os resultados.

## 7. Organizar a entrega no repositório

Preservar os seguintes arquivos:

| Entrega | Local no repositório |
| --- | --- |
| Este roteiro | [ROTEIRO_PARTE02.md](ROTEIRO_PARTE02.md) |
| Descrição e protocolo | [pipeline/README.md](pipeline/README.md) |
| Relatório e análise de resultados | [pipeline/RELATORIO.md](pipeline/RELATORIO.md) |
| Metadados do dataset | [pipeline/dataset-manifest.json](pipeline/dataset-manifest.json) |
| Pipeline, preparador, submissão e agregação | [pipeline/](pipeline/) |
| CSV final de medianas | [resultados/speedup_corpus.csv](resultados/speedup_corpus.csv) |
| Medições, estatísticas e logs do lote validado | [resultados/pipeline/run-20260930-corpus02/](resultados/pipeline/run-20260930-corpus02/) |

As matrizes completas do lote validado permanecem em `/home/g02/corpus/resultados/run-20260930-corpus02`. Para um novo lote, copiar seu CSV, medições, estatísticas e logs para o repositório e atualizar o relatório com sua identificação; preservar a procedência das evidências.

## 8. Resultado já validado

O lote `run-20260930-corpus02` concluiu os jobs 122–127: seis configurações e 18 repetições. Foram conferidos 129.098 documentos, 47.801 termos de vocabulário, 2.304 matrizes e igualdade dos resultados entre configurações. As seis medianas foram recalculadas contra as medições individuais.

O melhor tempo foi 4,735173 s com dois workers, contra 6,353340 s com um. As configurações maiores foram mais lentas; o [relatório](pipeline/RELATORIO.md) discute coordenação, comunicação e I/O. O objetivo é registrar o resultado observado, mesmo quando adicionar workers não acelera o pipeline.

Este roteiro documenta a reprodução. Sua criação não submeteu novos jobs nem realizou staging, commit ou push.
