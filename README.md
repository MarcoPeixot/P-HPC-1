# P-HPC-1 — MPI e PLN distribuído

Entrega das Partes 1 e 2: experimentos MPI da Aula 3 e pipeline de PLN distribuído com dataset público. Os códigos, relatórios e evidências históricos estão publicados em `main`. As alterações de reprodução desta revisão permanecem locais até um novo commit e push.

## Parte 1 — Aula 3

Conferida com `Roteiro Aula 3 - MPI e SLURM.pdf`, Inteli 2026.2, Prof. João Luisi. O [relatório](RELATORIO_AULA03.md) contém as tabelas dos Blocos 2 e 3, as três respostas de cada bloco, a tabela de speedup e eficiência e as cinco respostas do experimento.

| Item | Arquivo |
| --- | --- |
| Relatório completo da Aula 3 | [RELATORIO_AULA03.md](RELATORIO_AULA03.md) |
| Tabela do Bloco 2: tempos e razão entre nós | [resultados/bloco2-pingpong.csv](resultados/bloco2-pingpong.csv) |
| Tabela do Bloco 3: tempos do rank 0 e validação | [resultados/bloco3-soma.csv](resultados/bloco3-soma.csv) |
| Seis pontos de π: 1, 2, 4, 8, 16 e 32 processos | [resultados/speedup.csv](resultados/speedup.csv) |
| Gráfico: ideal, Amdahl e medido, eixo x em log base 2 | [speedup.png](speedup.png) |
| Códigos e logs originais | [aula03/](aula03/) |
| CSV original com todas as medições | [aula03/resultados/speedup-original.csv](aula03/resultados/speedup-original.csv) |
| Integridade dos arquivos originais MPI | [resultados/sha256-originais.txt](resultados/sha256-originais.txt) |

Os resultados MPI foram recuperados de `/home/test/aula03-codigo`, com registros de 09/09/2026. O CSV de entrega seleciona o menor tempo de cada ponto entre as duas séries completas, jobs 27–32 e 35–40. Cada linha preserva todas as colunas do job escolhido. O gráfico usa esses mesmos seis pontos; as configurações adicionais não foram misturadas às séries.

A comparação numérica com PCIe, citada em uma pergunta adicional do Bloco 2, depende do dado da Aula 1, ainda não fornecido. Essa ausência está declarada no relatório e não afeta os cinco itens solicitados da Parte 1.

## Parte 2 — Pipeline de PLN no cluster

Dataset público: [B2W-Reviews01, da B2W Digital](https://github.com/americanas-tech/b2w-reviews01), com avaliações de produtos em português brasileiro. O original possui 132.373 registros, em CSV UTF-8, e 49.453.175 bytes (47,16 MiB). Foram utilizados **129.098 documentos** com `review_text` não vazio, preparados em 128 arquivos JSONL; shards e stopwords somam 21.430.941 bytes (20,44 MiB).

O original e os arquivos preparados estão no NFS em `/opt/ohpc/pub/datasets/b2w-reviews01`. O [manifest](pipeline/dataset-manifest.json) fixa fonte, snapshot, contagens, tamanhos e hashes. O dataset usa licença CC BY-NC-SA 4.0, com atribuição à B2W Digital, conforme a fonte. O dataset integral permanece no NFS.

O [pipeline](pipeline/pipeline.py) implementa tokenização Unicode, remoção de stopwords portuguesas, TF-IDF com IDF global e normalização L2, e estatísticas de vocabulário, distribuição de comprimentos e termos com maior soma de TF-IDF. As matrizes são esparsas, em CSR/NPZ.

| Item | Arquivo |
| --- | --- |
| Roteiro de execução da Parte 2 | [ROTEIRO_PARTE02.md](ROTEIRO_PARTE02.md) |
| Descrição do dataset, protocolo e reprodução | [pipeline/README.md](pipeline/README.md) |
| Relatório de resultados e gargalos | [pipeline/RELATORIO.md](pipeline/RELATORIO.md) |
| Preparação reproduzível do dataset | [pipeline/preparar_dataset.py](pipeline/preparar_dataset.py) |
| Quatro etapas do pipeline | [pipeline/pipeline.py](pipeline/pipeline.py) |
| Dependências Python | [pipeline/requirements.txt](pipeline/requirements.txt) |
| Execução completa e agregação automática | [pipeline/executar_tudo.sh](pipeline/executar_tudo.sh) |
| Submissão das seis configurações | [pipeline/submeter.sh](pipeline/submeter.sh) |
| Job Slurm com três repetições por configuração | [pipeline/job_corpus.sbatch](pipeline/job_corpus.sbatch) |
| Agregação das medianas | [pipeline/agregar.py](pipeline/agregar.py) |
| CSV final com o mesmo cabeçalho da Aula 3 | [resultados/speedup_corpus.csv](resultados/speedup_corpus.csv) |
| Medições individuais e logs dos jobs 122–127 | [resultados/pipeline/run-20260930-corpus02/](resultados/pipeline/run-20260930-corpus02/) |

## Validação dos requisitos — 30/09/2026

| Requisito solicitado | Resultado da conferência |
| --- | --- |
| Tabelas dos Blocos 2 e 3 | Presentes; razão do ping-pong e tempos do rank 0 conferidos nos logs. |
| Seis pontos de `pi_mpi` | CSV com 1, 2, 4, 8, 16 e 32 processos, selecionando os menores tempos das duas séries. |
| Gráfico e cinco respostas do experimento | Presentes no relatório, alinhados aos cinco enunciados do PDF. |
| Dataset público com pelo menos 50.000 documentos no NFS | 129.098 documentos; fonte, idioma, formatos e tamanhos documentados; hashes e contagens conferidos no servidor. |
| Tokenização, stopwords, TF-IDF e estatísticas | Implementados; dois testes de correção passaram no ambiente `hpc` do master. |
| Slurm com 1, 2, 4, 8, 16 e 32 workers, três vezes cada | 18 medições completas dos jobs 122–127; um worker em um nó; demais topologias registradas. |
| Medianas em `resultados/speedup_corpus.csv` | Seis linhas recalculadas contra as medições individuais; cabeçalho `nprocs,nnodes,t_total,t_serial,t_calc`. |

O código local e o executado no cluster têm o mesmo SHA-256; o manifest local corresponde ao manifest do NFS. Foram conferidas as 2.304 matrizes das 18 repetições, com 2.176 comparações entre partições de execuções distintas: arrays CSR, vocabulário e estatísticas idênticos. Foram confirmados 129.098 IDs únicos e normalização L2 das linhas não vazias. Os logs do controlador Slurm registram código de saída zero para os seis jobs 122–127. As saídas completas permanecem em `/home/g02/corpus/resultados/run-20260930-corpus02`, compartilhado e gravável; o dataset em `/opt/ohpc/pub` é somente leitura nos workers.

O melhor tempo mediano do corpus foi com dois workers (4,735173 s), contra 6,353340 s com um. As configurações maiores foram mais lentas; o relatório registra esse resultado e os custos de coordenação, comunicação e I/O. `t_serial` e `t_calc` têm as definições documentadas no protocolo. As medianas são calculadas por coluna e não precisam manter a soma exata entre parcelas.

## Reprodução

Para recalcular o speedup MPI, na raiz local:

```bash
python3 aula03/analisa_speedup.py resultados/speedup.csv
```

Para gerar novamente o gráfico, o ambiente Python precisa ter Matplotlib. Para os jobs MPI, use um diretório compartilhado e gravável em `/home`, carregue `gnu15` e `openmpi5`, compile com `make` e use os scripts de `aula03/`.

Para o corpus, siga o passo a passo de [ROTEIRO_PARTE02.md](ROTEIRO_PARTE02.md) e os detalhes de [pipeline/README.md](pipeline/README.md). O README do pipeline inclui clone, instalação do ambiente `hpc` e dos requirements, permissões do dataset, testes e execução com `bash pipeline/executar_tudo.sh`. As execuções históricas ficam em `/home/g02/corpus`; uma nova reprodução usa o clone compartilhado em `/home/g02/P-HPC-1`.

## Estado do Git

Repositório público: [MarcoPeixot/P-HPC-1](https://github.com/MarcoPeixot/P-HPC-1). Branch local e padrão do remoto: `main`. O conteúdo histórico está publicado. Esta revisão de reprodução não foi commitada nem enviada ao remoto.
