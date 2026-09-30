# Parte 2 — PLN distribuído com B2W-Reviews01

O ponto de entrada desta parte é [experimento_pln.ipynb](experimento_pln.ipynb), com o código completo e a análise. O notebook implementa e demonstra tokenização, remoção de stopwords, TF-IDF e estatísticas, e permite submeter o experimento Dask/Slurm. Os scripts continuam como entradas dos jobs. A Parte 1 contém os experimentos MPI da Aula 3.

## Abrir e executar o notebook

Após instalar [requirements.txt](requirements.txt), no diretório `pipeline`:

```bash
jupyter lab experimento_pln.ipynb
```

Selecione o kernel do ambiente em que os requirements foram instalados e execute as células do início ao fim. O notebook contém preparação, as quatro etapas, agregação, dois testes executáveis e análise das 18 medições reais com tabelas e gráficos. A configuração padrão usa uma fixture pequena para testar o código e lê os resultados publicados; não baixa o dataset nem submete novos jobs.

Para outra execução real, abra no master como `g02` e altere `EXECUTAR_CLUSTER = True`, após preparar ambiente, NFS e permissões conforme os passos abaixo. A célula de submissão chama `executar_tudo.sh`; a análise padrão continua apontando para o lote histórico até que `EVIDENCIAS` e `CSV_MEDIANAS` sejam ajustados para outro lote.

Para abrir JupyterLab no master e acessar do notebook pessoal:

```bash
source /opt/ohpc/pub/apps/miniforge3/bin/activate hpc
cd /home/g02/P-HPC-1/pipeline
jupyter lab --no-browser --ip=127.0.0.1 --port=8888 experimento_pln.ipynb
```

Em outro terminal no computador pessoal, use `ssh -L 8888:127.0.0.1:8888 g02@10.128.0.4` e abra a URL local com o token exibido pelo Jupyter. O arquivo inclui código completo; `pipeline.py`, `preparar_dataset.py` e `agregar.py` continuam disponíveis para execução batch. As medições históricas foram geradas por esses scripts, não pela execução atual do notebook.

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

## Reprodução do zero no cluster

O cluster deve ter OpenHPC/Slurm e NFS configurados, os quatro nós disponíveis e um usuário comum como `g02`, com `/home` compartilhado. A instalação de Slurm, Warewulf e NFS é infraestrutura prévia; os passos abaixo preparam o ambiente e o experimento sobre esse cluster.

### 1. Clonar o código

No master, como `g02`:

```bash
cd /home/g02
git clone https://github.com/MarcoPeixot/P-HPC-1.git
cd /home/g02/P-HPC-1
```

O diretório do clone deve ser compartilhado e gravável pelos nós. O clone da branch `main` contém os arquivos e comandos documentados nesta página.

### 2. Instalar Python e dependências no NFS

Os jobs esperam o ambiente `hpc` em `/opt/ohpc/pub/apps/miniforge3`. Um administrador executa no master os passos de instalação. Se Miniforge e o ambiente `hpc` já existem, reutilize-os e instale somente os requirements.

Se Miniforge não estiver instalado, para Linux x86_64:

```bash
sudo mkdir -p /opt/ohpc/pub/apps
curl -fL https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh -o /tmp/Miniforge3-Linux-x86_64.sh
sudo bash /tmp/Miniforge3-Linux-x86_64.sh -b -p /opt/ohpc/pub/apps/miniforge3
```

Criar o ambiente, se ainda não existir, e instalar as versões usadas pelo pipeline:

```bash
sudo /opt/ohpc/pub/apps/miniforge3/bin/conda create -y -n hpc python=3.11 pip
sudo /opt/ohpc/pub/apps/miniforge3/envs/hpc/bin/python -m pip install -r /home/g02/P-HPC-1/pipeline/requirements.txt
```

As dependências diretas estão em [requirements.txt](requirements.txt): NumPy 2.4.6, Dask/Distributed 2026.8.0, JupyterLab 4.6.4, ipykernel 7.4.0 e Matplotlib 3.11.2. O preparador usa a biblioteca padrão; stopwords são baixadas de um snapshot fixo, sem exigir instalação do NLTK. O benchmark histórico usou Python 3.11.16.

A instalação não interativa segue a [documentação oficial do Miniforge](https://github.com/conda-forge/miniforge#install). `/opt/ohpc/pub` precisa estar montado nos workers, com permissão de leitura dos arquivos e execução dos binários.

### 3. Permitir a preparação do dataset

O administrador cria o diretório do dataset no master e dá acesso ao usuário que prepara os dados:

```bash
sudo mkdir -p /opt/ohpc/pub/datasets/b2w-reviews01
sudo chown g02:g02 /opt/ohpc/pub/datasets/b2w-reviews01
```

Os workers leem esse diretório por NFS. As saídas são gravadas no clone em `/home/g02/P-HPC-1/pipeline/resultados`.

### 4. Ativar o ambiente e testar

Como `g02`, no master:

```bash
cd /home/g02/P-HPC-1/pipeline
source /opt/ohpc/pub/apps/miniforge3/bin/activate hpc
python --version
python -m pip check
python -c 'import numpy, dask, distributed; print(numpy.__version__, dask.__version__, distributed.__version__)'
sinfo -Nel
python test_pipeline.py
bash -n executar_tudo.sh submeter.sh job_corpus.sbatch
```

O teste usa três documentos com DF/IDF conhecidos e verifica TF-IDF, vetor vazio, invariância entre workers, hashes e medianas. Ele não é o benchmark do dataset completo.

### 5. Executar o experimento com um comando

Com ambiente e permissões preparados, como `g02`:

```bash
cd /home/g02/P-HPC-1/pipeline
bash executar_tudo.sh
```

O [executar_tudo.sh](executar_tudo.sh) ativa `hpc`, baixa ou verifica o dataset, chama [submeter.sh](submeter.sh) para as seis configurações e submete um job de agregação dependente do sucesso dos seis jobs. Cada configuração executa três repetições. O comando retorna os jobs submetidos; a execução continua pelo Slurm.

Para escolher o diretório do lote:

```bash
bash executar_tudo.sh "$PWD/resultados/minha-execucao"
```

O lote deve ser novo. Acompanhe com `squeue -u g02`; as saídas Slurm ficam em `<lote>/logs/corpus-<job>.out`, junto dos logs de scheduler/workers. Se algum job falhar, a dependência `afterok` impede a agregação automática; confira os logs antes de executar novamente.

Ao concluir, o CSV fica em `<lote>/speedup_corpus.csv`, junto das 18 medições e matrizes. A agregação recusa medições ausentes e preserva o CSV histórico da raiz do repositório. Para entregar um novo lote, copie o CSV validado para o nome exigido:

```bash
cp "$LOTE/speedup_corpus.csv" /home/g02/P-HPC-1/resultados/speedup_corpus.csv
```

Antes desse comando, atribua a `LOTE` o caminho completo impresso pelo script. Atualize também o relatório e copie as medições/estatísticas/logs relevantes para a entrega, preservando a identificação do lote.

### Execução por etapas

O `submeter.sh` existente submete as seis configurações, mas não prepara o dataset nem agrega resultados. A alternativa manual continua disponível:

```bash
cd /home/g02/P-HPC-1/pipeline
source /opt/ohpc/pub/apps/miniforge3/bin/activate hpc
python preparar_dataset.py
LOTE="$PWD/resultados/run-$(date +%Y%m%d-%H%M%S)"
bash submeter.sh "$LOTE"
# Depois de os seis jobs concluírem com sucesso:
bash executar_tudo.sh --agregar "$LOTE"
```

O agregador exige três repetições completas, matrizes presentes e mesmo dataset, protocolo e código. O CSV final mantém o cabeçalho da Aula 3 e contém seis linhas de medianas.

## Resultado realizado em 30/09/2026

As 18 repetições do lote `run-20260930-corpus02` foram concluídas e validadas. Consulte o [relatório com resultados](RELATORIO.md), o [CSV de medianas](../resultados/speedup_corpus.csv) e as [evidências individuais](../resultados/pipeline/run-20260930-corpus02). As matrizes ficam no NFS em `/home/g02/corpus/resultados/run-20260930-corpus02`.
