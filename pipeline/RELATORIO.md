# Relatório da Parte 2: PLN distribuído com Dask

30/09/2026. Cluster OpenHPC/Slurm, nós `c1`, `c2`, `c3`, `c4`, usuário `g02`.

## Dataset

O [B2W-Reviews01, da B2W Digital](https://github.com/americanas-tech/b2w-reviews01) contém avaliações de produtos em português brasileiro. O snapshot utilizado é o commit `4639429ec698d7821fc99a0bc665fa213d9fcd5a`, sob licença CC BY-NC-SA 4.0. O arquivo original tem 132.373 registros e 49.453.175 bytes (47,16 MiB), em CSV UTF-8 separado por vírgulas. A leitura considera aspas e campos multilinha.

Foi usado apenas `review_text`: 3.275 registros sem texto foram excluídos, resultando em 129.098 documentos. Duplicatas foram preservadas. A preparação gerou 128 shards JSONL e uma lista fixa de 207 stopwords, com 21.430.941 bytes (20,44 MiB) no total. Os hashes e metadados estão em [dataset-manifest.json](dataset-manifest.json).

No NFS, o original e os arquivos preparados estão em `/opt/ohpc/pub/datasets/b2w-reviews01`. As saídas ficam em `/home/g02/corpus/resultados/run-20260930-corpus02`, pois os nós montam `/opt/ohpc/pub` somente para leitura.

## Pipeline implementado

1. Tokenização Unicode NFC, casefold e sequências de letras, preservando acentos.
2. Remoção de stopwords portuguesas do snapshot fixado do NLTK Data.
3. TF-IDF com frequência documental global, IDF suavizado e normalização L2; vetores em matrizes CSR por partição.
4. Agregação das estatísticas de vocabulário, comprimentos e termos com maior soma de TF-IDF.

O vocabulário e o IDF são calculados para todo o corpus e usados por todos os workers. Os detalhes matemáticos, o intervalo medido e os comandos de reprodução estão no [README](README.md).

## Execuções via Slurm

Os jobs 122 a 127 concluíram com código de saída zero, somando 18 repetições completas. Cada configuração usou uma thread por worker e três repetições do corpus integral. As alocações foram exclusivas e sequenciais. Até 16 workers há quatro por nó; 32 workers utiliza oito por nó, incluindo SMT. A configuração de um worker rodou em um único nó.

| Workers | Nós | Repetições | Mediana total (s) | Mediana serial (s) | Mediana t_calc (s) | Speedup | Eficiência |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 3 | 6.353340 | 1.018419 | 5.334921 | 1.000 | 100.0% |
| 2 | 1 | 3 | 4.735173 | 1.733567 | 3.047155 | 1.342 | 67.1% |
| 4 | 1 | 3 | 5.073977 | 2.986841 | 2.063768 | 1.252 | 31.3% |
| 8 | 2 | 3 | 8.046799 | 5.427586 | 2.619214 | 0.790 | 9.9% |
| 16 | 4 | 3 | 9.216111 | 7.797210 | 1.418902 | 0.689 | 4.3% |
| 32 | 4 | 3 | 11.172532 | 9.915839 | 1.225851 | 0.569 | 1.8% |

O arquivo [speedup_corpus.csv](../resultados/speedup_corpus.csv) usa o mesmo cabeçalho da Aula 3: `nprocs,nnodes,t_total,t_serial,t_calc`. Aqui, `nprocs` indica workers Dask. Cada tempo é a mediana independente de três execuções; as medianas das parcelas podem não somar exatamente à mediana total.

### Interpretação

O menor tempo foi com 2 workers, no mesmo nó: 4.735 s contra 6.353 s com um worker, speedup de 1.342. Mais workers reduziram a parcela distribuída em vários casos, mas elevaram a coordenação e a distribuição do vocabulário/IDF; isso fez o tempo total piorar nas configurações com vários nós.

O benchmark mede o pipeline completo, incluindo comunicação e escrita das matrizes, não só o cálculo local de tokens. O corpus tem textos curtos e quantidade fixa de dados; aumentar workers não garante acelerar essa carga. `t_serial` inclui a coordenação medida no cliente e o broadcast do vocabulário/IDF; `t_calc` é o restante do tempo de parede, incluindo comunicação, espera e I/O. As três repetições não limpam caches e a ordem é fixa. Esses resultados descrevem este corpus, esta implementação e este cluster; não permitem concluir que Dask tenha pior desempenho em geral.

## Estatísticas do corpus

- Documentos processados: 129.098
- Vocabulário após stopwords: 47.801 termos
- Valores não nulos nas matrizes: 1.607.559
- Documentos com vetor zero após a limpeza: 100, mantidos no corpus

| Medida de tamanho em tokens | Antes de stopwords | Depois de stopwords |
| --- | ---: | ---: |
| Mínimo | 0.000 | 0.000 |
| Máximo | 795.000 | 475.000 |
| Média | 22.855 | 13.330 |
| Desvio padrão | 21.891 | 12.074 |
| p25 | 11.000 | 7.000 |
| p50 | 16.000 | 10.000 |
| p75 | 26.000 | 15.000 |
| p90 | 45.000 | 25.000 |
| p95 | 61.000 | 34.000 |
| p99 | 111.000 | 61.000 |

Os histogramas completos e os 30 termos de maior TF-IDF estão em [statistics.json](../resultados/pipeline/run-20260930-corpus02/w01-r1/statistics.json). Comprimentos mínimos iguais a zero correspondem a textos sem tokens de letras, mesmo que o campo original não esteja vazio.

### Dez termos com maior soma de TF-IDF normalizado

| Termo | Soma TF-IDF | Frequência documental | IDF |
| --- | ---: | ---: | ---: |
| produto | 7209.918 | 59191 | 1.779793 |
| recomendo | 4297.025 | 24199 | 2.674227 |
| bom | 4212.728 | 22871 | 2.730666 |
| entrega | 3952.492 | 21508 | 2.792108 |
| prazo | 3237.764 | 15434 | 3.123942 |
| qualidade | 3206.270 | 15973 | 3.089617 |
| chegou | 3074.684 | 15314 | 3.131747 |
| excelente | 3039.266 | 13931 | 3.226391 |
| antes | 2688.986 | 12237 | 3.356034 |
| bem | 2637.416 | 15049 | 3.149202 |

O ranking soma o peso TF-IDF normalizado de cada termo sobre todos os documentos. Ele não é um ranking de IDF isolado nem de frequência bruta.

## Validação e evidências

- Dois testes de correção passaram localmente e no ambiente `hpc` do master: IDF global/L2, vetores vazios, invariância entre workers, hashes e agregação de medianas.
- Comparamos as 128 partições das 18 repetições (2.176 pares entre execuções distintas); os arrays CSR foram idênticos.
- Vocabulários e estatísticas foram idênticos em todas as repetições. Confirmamos IDs únicos, 129.098 documentos e norma L2 unitária em todas as linhas não vazias.
- As seis configurações têm três medições reais, mesmo protocolo `b2w-global-tfidf-v2`, mesmo manifest e mesmo SHA-256 do executor.
- Medições individuais, logs dos jobs, vocabulário e [validation.json](../resultados/pipeline/run-20260930-corpus02/validation.json) estão em [resultados/pipeline/run-20260930-corpus02](../resultados/pipeline/run-20260930-corpus02). As 2.304 matrizes NPZ continuam no NFS, sem incluí-las no Git.

### Ajustes no cluster

O nó `c2` estava acessível, mas marcado como sem resposta no controlador; a solicitação de retorno ao serviço regularizou seu registro, sem reiniciar os serviços. Os quatro nós participaram dos jobs com 16 e 32 workers e terminaram disponíveis.

A tentativa inicial revelou incompatibilidade entre broadcast de dados globais e a política padrão `ReduceReplicas` do Active Memory Manager. O executor agora desativa o AMM no scheduler temporário de cada job antes de medir. Após a correção, todas as configurações foram repetidas no lote `corpus02`; medições da tentativa anterior não entram no CSV final.
