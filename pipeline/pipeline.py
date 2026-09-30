"""TF-IDF global em partições Dask, sem matriz documento-vocabulário densa."""
import argparse
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import re
import socket
import time
import unicodedata

import numpy as np
from distributed import Client

TOKEN = re.compile(r'[^\W\d_]+', re.UNICODE)
PROTOCOL = 'b2w-global-tfidf-v2'


def tokenize(text):
    return TOKEN.findall(unicodedata.normalize('NFC', text).casefold())


def preprocess(path, stopwords):
    documents, frequency = [], Counter()
    with open(path, encoding='utf-8') as stream:
        for line in stream:
            item = json.loads(line)
            tokens = tokenize(item['text'])
            counts = Counter(token for token in tokens if token not in stopwords)
            frequency.update(counts.keys())
            documents.append((item['id'], counts, len(tokens), sum(counts.values())))
    return documents, frequency


def partition_metadata(state):
    documents, frequency = state
    return frequency, [d[2] for d in documents], [d[3] for d in documents]


def vectorize(state, partition, vocabulary, idf, output):
    documents, _ = state
    indices, values, indptr, ids = [], [], [0], []
    totals = Counter()
    for doc_id, counts, _, _ in documents:
        pairs = sorted((vocabulary[term], count * idf[vocabulary[term]]) for term, count in counts.items())
        norm = math.sqrt(sum(value * value for _, value in pairs))
        for index, value in pairs:
            weight = value / norm
            indices.append(index)
            values.append(weight)
            totals[index] += weight
        indptr.append(len(indices))
        ids.append(doc_id)
    np.savez_compressed(Path(output) / f'part-{partition:03d}.npz',
                        indptr=np.asarray(indptr, dtype=np.int64),
                        indices=np.asarray(indices, dtype=np.int32),
                        data=np.asarray(values, dtype=np.float64),
                        document_ids=np.asarray(ids, dtype=np.int64),
                        shape=np.asarray([len(documents), len(vocabulary)], dtype=np.int64))
    return totals, len(values), socket.gethostname()


def length_stats(lengths):
    array = np.asarray(lengths, dtype=np.int64)
    histogram = Counter(int(n) for n in lengths)
    return dict(count=len(lengths), min=int(array.min()), max=int(array.max()),
                mean=float(array.mean()), std=float(array.std()),
                quantiles=dict(zip(['p25', 'p50', 'p75', 'p90', 'p95', 'p99'],
                                   np.quantile(array, [.25, .5, .75, .9, .95, .99]).tolist())),
                histogram={str(k): histogram[k] for k in sorted(histogram)})


def execute(client, directory, output, workers, repetition, expected_nodes):
    directory, output = Path(directory).resolve(), Path(output).resolve()
    if output.exists():
        raise FileExistsError(f'Repetição já existe: {output}')
    manifest_bytes = (directory / 'manifest.json').read_bytes()
    manifest = json.loads(manifest_bytes)
    for item in manifest['files']:
        if hashlib.sha256((directory / item['path']).read_bytes()).hexdigest() != item['sha256']:
            raise ValueError(f"Dataset alterado: {item['path']}")
    paths = [directory / item['path'] for item in manifest['files'] if item['path'].endswith('.jsonl')]
    stopwords = frozenset((directory / 'stopwords-pt.txt').read_text().splitlines())
    client.wait_for_workers(workers, timeout=180)
    client.amm.stop()
    if client.amm.running():
        raise RuntimeError('Broadcast exige AMM ReduceReplicas desativado')
    info = client.scheduler_info()['workers']
    hosts = client.run(socket.gethostname)
    distribution = Counter(hosts.values())
    if len(info) != workers or len(distribution) != expected_nodes:
        raise ValueError(f'Alocação inesperada: {workers=} {expected_nodes=} {distribution=}')
    if len(set(distribution.values())) != 1 or any(w['nthreads'] != 1 for w in info.values()):
        raise ValueError('Exige workers igualmente distribuídos, uma thread por worker')
    output.mkdir(parents=True)
    start = time.perf_counter()
    serial = 0.0
    states = client.map(preprocess, [str(p) for p in paths], stopwords=stopwords, pure=False)
    metadata = client.gather(client.map(partition_metadata, states))
    mark = time.perf_counter()
    frequency, raw_lengths, filtered_lengths = Counter(), [], []
    for df, raw, filtered in metadata:
        frequency.update(df)
        raw_lengths.extend(raw)
        filtered_lengths.extend(filtered)
    documents = len(raw_lengths)
    if documents != manifest['documents']:
        raise ValueError('Contagem de documentos diverge do manifest')
    terms = sorted(frequency)
    vocabulary = {term: index for index, term in enumerate(terms)}
    idf = np.asarray([math.log((1 + documents) / (1 + frequency[term])) + 1 for term in terms])
    vocabulary_future = client.scatter([vocabulary], broadcast=True, hash=False, timeout=120)[0]
    idf_future = client.scatter([idf], broadcast=True, hash=False, timeout=120)[0]
    serial += time.perf_counter() - mark
    parts = client.map(vectorize, states, list(range(len(paths))), vocabulary=vocabulary_future,
                       idf=idf_future, output=str(output), pure=False)
    results = client.gather(parts)
    mark = time.perf_counter()
    totals, nonzero, execution_hosts = Counter(), 0, Counter()
    for partial, nnz, host in results:
        totals.update(partial)
        nonzero += nnz
        execution_hosts[host] += 1
    ranked = sorted(totals.items(), key=lambda item: (-item[1], terms[item[0]]))[:30]
    stats = dict(protocol=PROTOCOL, documents=documents, vocabulary_size=len(terms),
                 empty_after_stopwords=sum(n == 0 for n in filtered_lengths), nonzero=nonzero,
                 lengths_before_stopwords=length_stats(raw_lengths),
                 lengths_after_stopwords=length_stats(filtered_lengths),
                 top_tfidf=[dict(term=terms[index], sum_tfidf=value,
                                mean_tfidf=value / documents, document_frequency=frequency[terms[index]],
                                idf=float(idf[index])) for index, value in ranked])
    serial += time.perf_counter() - mark
    elapsed = time.perf_counter() - start
    (output / 'statistics.json').write_text(json.dumps(stats, ensure_ascii=False, indent=2) + '\n')
    with (output / 'vocabulary.jsonl').open('w', encoding='utf-8') as f:
        for term, index in vocabulary.items():
            f.write(json.dumps(dict(index=index, term=term, df=frequency[term], idf=float(idf[index])), ensure_ascii=False) + '\n')
    measurement = dict(status='complete', protocol=PROTOCOL, amm_enabled=False, workers=workers, nnodes=expected_nodes,
                       repetition=repetition, t_total=elapsed, t_serial=serial, t_calc=elapsed - serial,
                       documents=documents, partitions=len(paths), worker_hosts=dict(distribution),
                       partitions_by_host=dict(execution_hosts),
                       dataset_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
                       code_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                       slurm_job_id=os.getenv('SLURM_JOB_ID'),
                       matrix_format='CSR em NPZ: indptr, indices, data, document_ids, shape',
                       timed='leitura, quatro etapas, coordenação e escrita NPZ; exclui startup, verificação e export final JSON')
    (output / 'measurement.json').write_text(json.dumps(measurement, indent=2) + '\n')
    client.cancel(states)
    print(json.dumps(measurement))
    return stats, measurement


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--scheduler-file', required=True)
    parser.add_argument('--dataset', default='/opt/ohpc/pub/datasets/b2w-reviews01')
    parser.add_argument('--output', required=True)
    parser.add_argument('--workers', required=True, type=int)
    parser.add_argument('--nodes', required=True, type=int)
    parser.add_argument('--repetition', required=True, type=int)
    args = parser.parse_args()
    with Client(scheduler_file=args.scheduler_file) as client:
        execute(client, args.dataset, args.output, args.workers, args.repetition, args.nodes)
