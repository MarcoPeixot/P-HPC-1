"""Mede, fora do cluster, o volume serializado e o custo de CPU de cada fase do pipeline.

Reutiliza as funções de pipeline/pipeline.py sobre o mesmo dataset preparado
(hashes conferidos contra pipeline/dataset-manifest.json) e executa as fases em
sequência, num único processo, sem Dask. Os bytes são tamanhos de pickle
(protocolo 5, sem compressão), independentes do hardware; os tempos de CPU
dependem da máquina e servem apenas para a proporção entre as fases.

Uso: python analise/wildisley-filho/medir_volumes.py <diretório do dataset preparado>
"""
import hashlib
import json
import math
import pickle
import platform
import statistics
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path

import numpy as np

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'pipeline'))
import pipeline as P  # noqa: E402

REPETICOES = 3


def tamanho(obj):
    return len(pickle.dumps(obj, protocol=5))


def cronometrar(funcao):
    inicio = time.perf_counter()
    resultado = funcao()
    return resultado, time.perf_counter() - inicio


def executar(paths, stopwords):
    tempos = {}
    states, tempos['preprocess'] = cronometrar(lambda: [P.preprocess(str(p), stopwords) for p in paths])
    metadata, tempos['partition_metadata'] = cronometrar(lambda: [P.partition_metadata(s) for s in states])

    def combinar():
        frequency, raw, filtered = Counter(), [], []
        for df, r, f in metadata:
            frequency.update(df)
            raw.extend(r)
            filtered.extend(f)
        terms = sorted(frequency)
        vocabulary = {term: index for index, term in enumerate(terms)}
        n = len(raw)
        idf = np.asarray([math.log((1 + n) / (1 + frequency[term])) + 1 for term in terms])
        return terms, vocabulary, idf, raw, filtered
    (terms, vocabulary, idf, raw, filtered), tempos['serial_combinacao'] = cronometrar(combinar)

    with tempfile.TemporaryDirectory() as saida:
        results, tempos['vectorize_npz'] = cronometrar(
            lambda: [P.vectorize(s, i, vocabulary, idf, saida) for i, s in enumerate(states)])
        npz = sum(p.stat().st_size for p in Path(saida).glob('*.npz'))

    def agregar():
        totals = Counter()
        for partial, _, _ in results:
            totals.update(partial)
        sorted(totals.items(), key=lambda item: (-item[1], terms[item[0]]))[:30]
        P.length_stats(raw)
        P.length_stats(filtered)
    _, tempos['serial_agregacao'] = cronometrar(agregar)
    return tempos, states, metadata, vocabulary, idf, results, npz


def main():
    dataset = Path(sys.argv[1])
    manifest = json.loads((REPO / 'pipeline' / 'dataset-manifest.json').read_text())
    for item in manifest['files']:
        if hashlib.sha256((dataset / item['path']).read_bytes()).hexdigest() != item['sha256']:
            raise ValueError(f"Dataset difere do manifest do cluster: {item['path']}")
    paths = [dataset / item['path'] for item in manifest['files'] if item['path'].endswith('.jsonl')]
    stopwords = frozenset((dataset / 'stopwords-pt.txt').read_text().splitlines())

    rodadas = []
    for _ in range(REPETICOES):
        tempos, states, metadata, vocabulary, idf, results, npz = executar(paths, stopwords)
        rodadas.append(tempos)
    cpu = {fase: statistics.median(r[fase] for r in rodadas) for fase in rodadas[0]}

    blob = pickle.dumps(vocabulary, protocol=5)
    _, serializar = cronometrar(lambda: pickle.dumps(vocabulary, protocol=5))
    _, desserializar = cronometrar(lambda: pickle.loads(blob))

    saida = dict(
        maquina=f'{platform.machine()} {platform.processor()}'.strip(),
        python=platform.python_version(),
        repeticoes=REPETICOES,
        cpu_s_mediana=cpu,
        pickle_vocabulario_s=dict(serializar=serializar, desserializar=desserializar),
        bytes=dict(
            shards_jsonl=sum(p.stat().st_size for p in paths),
            estado_particoes=sum(tamanho(s) for s in states),
            metadados_gather=sum(tamanho(m) for m in metadata),
            vocabulario=tamanho(vocabulary),
            idf=tamanho(idf),
            resultados_gather=sum(tamanho(r) for r in results),
            npz_escrita=npz,
            stopwords_por_tarefa=tamanho(stopwords),
        ),
        entradas=dict(vocabulario=len(vocabulary), df_parciais=sum(len(m[0]) for m in metadata),
                      totais_parciais=sum(len(r[0]) for r in results), particoes=len(paths)),
    )
    destino = Path(__file__).with_name('volumes_locais.json')
    destino.write_text(json.dumps(saida, indent=2, ensure_ascii=False) + '\n')
    print(json.dumps(saida, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    main()
