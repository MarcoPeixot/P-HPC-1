# Parte 2 — PLN distribuído com B2W-Reviews01

Esta parte implementa e mede tokenização, remoção de stopwords, TF-IDF e estatísticas do corpus com Dask sobre Slurm. A Parte 1 contém os experimentos MPI da Aula 3.

## Dataset e rastreabilidade

- Fonte: [B2W Digital / Americanas — B2W-Reviews01](https://github.com/americanas-tech/b2w-reviews01).
- Snapshot: commit `4639429ec698d7821fc99a0bc665fa213d9fcd5a`.
- Original: **132.373 registros**, **49.453.175 bytes** (47,16 MiB).
- Corpus utilizado: **129.098 documentos**, cada um correspondente a `review_text` não vazio. Foram excluídos 3.275 registros sem texto. Duplicatas são preservadas; títulos não são concatenados.
- Idioma: português brasileiro. Avaliações de produtos da Americanas, coletadas entre janeiro e maio de 2018 segundo a fonte.
- Formato original verificado: CSV UTF-8, **vírgula** como delimitador, aspas duplas e campos multilinha. A orientação de ponto e vírgula no README da fonte não corresponde ao snapshot atual.
- SHA-256 do CSV: `821fb0bf9f7230b0fba4e4f9fadd75a66d1a9ff0b1657810791d33007eb2ab38`.
- Local no NFS: `/opt/ohpc/pub/datasets/b2w-reviews01/`.
- Entrada de execução: 128 arquivos JSONL UTF-8, distribuição round-robin determinística, contendo somente ID de linha e texto. O manifest registra contagem, tamanho em bytes e SHA-256 de cada arquivo.
- Licença do dataset: **CC BY-NC-SA 4.0**. Atribuição: **B2W Digital — B2W-Reviews01**. As restrições de atribuição, uso não comercial e compartilhamento pela mesma licença se aplicam ao dataset e a seus derivados.
- Stopwords: 207 termos portugueses do NLTK Data, snapshot `550b6625bcef1f2abff2ff770a5a0d272c9c6b2a`, com SHA-256 do arquivo-fonte registrado no manifest. Não é necessário instalar NLTK nos workers.

O CSV completo fica no NFS, não no repositório público. O repositório contém o preparador reproduzível, metadados e resultados.

## Quatro etapas do pipeline

1. **Tokenização:** normalização Unicode NFC e `casefold`; expressão regular de sequências de letras Unicode. Números, pontuação e underscore são separadores. Acentos são preservados.
2. **Stopwords:** remoção dos termos da lista portuguesa fixada. Documentos que ficam vazios continuam no corpus e recebem vetor zero.
3. **TF-IDF:** cada partição calcula contagens por documento e frequência documental. O cliente combina os DFs de todas as partições; todos os workers recebem o mesmo vocabulário e IDF. Para termo `t`, `idf(t) = log((1 + N)/(1 + df(t))) + 1`; TF é a contagem bruta e cada vetor recebe normalização L2. Não há corte por frequência, limite de vocabulário ou hash de termos.
4. **Estatísticas:** tamanho do vocabulário, comprimentos antes/depois das stopwords (histograma completo, média, desvio, mínimo, máximo e percentis) e 30 termos com maior **soma de TF-IDF normalizado sobre todos os documentos**. Empates são ordenados pelo termo. Também são registrados IDF, DF e média por documento.

A matriz de cada partição é gravada como CSR em NPZ (`indptr`, `indices`, `data`, `document_ids`, `shape`). O vocabulário ordenado fica em `vocabulary.jsonl`. Os vetores não são densificados e as estatísticas locais são agregadas globalmente.

## Configurações e protocolo de medição

| Workers | Nós | Workers por nó | Threads por worker |
| ---: | ---: | ---: | ---: |
| 1 | 1 | 1 | 1 |
| 2 | 1 | 2 | 1 |
| 4 | 1 | 4 | 1 |
| 8 | 2 | 4 | 1 |
| 16 | 4 | 4 | 1 |
| 32 | 4 | 8 | 1 |

A configuração de 1 worker é single-node. Até 16 workers, a distribuição acompanha os 16 cores físicos do cluster; 32 utiliza os 32 processadores lógicos (SMT). Os jobs usam nós exclusivos, são encadeados e cada alocação executa o pipeline completo três vezes. Cada repetição tem seus próprios arquivos e medição. O ambiente é `hpc`: Python 3.11.16, Dask/Distributed 2026.8.0 e NumPy 2.4.6.

O corpus e as 128 partições são idênticos em todas as configurações. O Active Memory Manager do scheduler fica desativado em todos os jobs, pois a política padrão `ReduceReplicas` é incompatível com `scatter(broadcast=True)` do vocabulário/IDF. Essa configuração se limita ao scheduler temporário de cada job. O pipeline verifica os hashes antes de medir e confirma quantidade de workers, uma thread e distribuição equilibrada entre os nós.

- `t_total`: do início da leitura distribuída até a conclusão da agregação das estatísticas; inclui escrita das matrizes NPZ e comunicação.
- `t_serial`: tempo de combinação local de DF/vocabulário, distribuição do IDF e agregação final no cliente.
- `t_calc`: `t_total - t_serial`, incluindo fases distribuídas, espera, comunicação e escrita. Não representa exclusivamente tempo de CPU.
- Fora da medição: início do job, startup de scheduler/workers, validação de hashes e exportação final de JSON/vocabulário.
- CSV final: `nprocs,nnodes,t_total,t_serial,t_calc`; `nprocs` representa **workers Dask**. Cada coluna de tempo é a mediana independente de três repetições, por isso a identidade aditiva pode não se manter exatamente nas medianas.

A ordem de configurações é fixa. As repetições não limpam cache de filesystem; as medianas representam o protocolo sequencial real. A taxa de aceleração compara o pipeline completo, incluindo coordenação e I/O. Com um corpus deste tamanho, esses custos podem dominar e não há expectativa de speedup linear.

## Execução no cluster

Preparar somente no master, com permissão de escrita em `/opt/ohpc/pub`:

```bash
python3.11 /home/g02/corpus/preparar_dataset.py
```

Os nós leem `/opt/ohpc/pub` por NFS somente leitura; saídas e logs ficam em `/home/g02/corpus`, que permite escrita compartilhada. Como `g02`:

```bash
cd /home/g02/corpus
bash submeter.sh
squeue -u g02
```

Ao terminar, agregar o lote informado pelo script:

```bash
source /opt/ohpc/pub/apps/miniforge3/bin/activate hpc
python agregar.py resultados/run-AAAAMMDD-HHMMSS --output resultados/speedup_corpus.csv
```

A agregação exige três repetições completas para incluir cada configuração e recusa mistura de corpus, protocolo ou código. Não gera medições fictícias para jobs pendentes ou falhos.

## Verificação de correção

```bash
source /opt/ohpc/pub/apps/miniforge3/bin/activate hpc
python test_pipeline.py
bash -n job_corpus.sbatch submeter.sh
```

O teste usa três documentos com DF/IDF conhecidos, verifica os valores TF-IDF e vetor vazio, compara matrizes e estatísticas com um e dois workers e confirma rejeição de dados alterados. O benchmark real usa o corpus integral, não a fixture.

## Resultado realizado em 30/09/2026

As 18 repetições do lote `run-20260930-corpus02` foram concluídas e validadas. Consulte o [relatório com resultados](RELATORIO.md), o [CSV de medianas](../resultados/speedup_corpus.csv) e as [evidências individuais](../resultados/pipeline/run-20260930-corpus02). As matrizes ficam no NFS em `/home/g02/corpus/resultados/run-20260930-corpus02`.
