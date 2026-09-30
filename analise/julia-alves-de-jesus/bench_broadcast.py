"""Mede o custo do broadcast do vocabulário no Dask, fora do cluster.

Reproduz a chamada de pipeline/pipeline.py:114,
client.scatter([vocabulary], broadcast=True, hash=False), com o vocabulário
real do lote run-20260930-corpus02, e compara com o mesmo conteúdo enviado
como um único bloco de bytes. Também mede a serialização de uma cópia com a
função que o Dask usa para montar mensagens (distributed.protocol.dumps).

Uso, na raiz do repositório, com pipeline/requirements.txt instalado:
    python analise/julia-alves-de-jesus/bench_broadcast.py
"""
import csv
import json
from pathlib import Path
import pickle
import platform
import statistics
import time

from distributed import Client, LocalCluster
from distributed.protocol import dumps, loads, to_serialize

AQUI = Path(__file__).resolve().parent
VOCAB = AQUI.parents[1] / 'resultados/pipeline/run-20260930-corpus02/vocabulary.jsonl'
REPETICOES = 5


def mediana_do_tempo(funcao, repeticoes=REPETICOES):
    tempos = []
    for _ in range(repeticoes):
        inicio = time.perf_counter()
        funcao()
        tempos.append(time.perf_counter() - inicio)
    return statistics.median(tempos)


if __name__ == '__main__':
    vocabulary = {json.loads(linha)['term']: i for i, linha in enumerate(VOCAB.open(encoding='utf-8'))}
    blob = pickle.dumps(vocabulary)

    # Uma cópia, serializada como o Dask serializa uma mensagem de dados.
    mensagem = {'op': 'update-data', 'data': {'vocabulario': to_serialize(vocabulary)}}
    frames = dumps(mensagem)
    serializacao = {
        'termos': len(vocabulary),
        'pickle_mb': len(blob) / 1e6,
        'dask_frames': len(frames),
        'dask_mb': sum(len(f) for f in frames) / 1e6,
        'dask_dumps_ms': 1000 * mediana_do_tempo(lambda: dumps(mensagem)),
        'dask_loads_ms': 1000 * mediana_do_tempo(lambda: loads(frames)),
        'pickle_ida_volta_ms': 1000 * mediana_do_tempo(lambda: pickle.loads(pickle.dumps(vocabulary))),
    }
    with (AQUI / 'serializacao_local.csv').open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(serializacao))
        writer.writeheader()
        writer.writerow({k: round(v, 4) for k, v in serializacao.items()})
    print(serializacao)

    linhas = []
    for workers in (1, 2, 4, 8):
        with LocalCluster(n_workers=workers, threads_per_worker=1, dashboard_address=None) as cluster, \
                Client(cluster) as client:
            t_dict = mediana_do_tempo(lambda: client.scatter([vocabulary], broadcast=True, hash=False))
            t_bytes = mediana_do_tempo(lambda: client.scatter([blob], broadcast=True, hash=False))
        linhas.append(dict(workers=workers, dict_s=round(t_dict, 4), bytes_s=round(t_bytes, 5)))
        print(linhas[-1])
    with (AQUI / 'broadcast_local.csv').open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['workers', 'dict_s', 'bytes_s'])
        writer.writeheader()
        writer.writerows(linhas)
    print('Máquina:', platform.machine(), platform.processor() or '', platform.python_version())
