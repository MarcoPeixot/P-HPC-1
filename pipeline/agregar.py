"""Agrega somente três repetições reais, preservando o formato da Aula 3."""
import argparse
import csv
import json
import math
from pathlib import Path
import statistics

LAYOUT = {1: 1, 2: 1, 4: 1, 8: 2, 16: 4, 32: 4}
HEADER = ['nprocs', 'nnodes', 't_total', 't_serial', 't_calc']


def aggregate(batch, output):
    batch, output = Path(batch), Path(output)
    rows, identities = [], set()
    missing = []
    for workers, nodes in LAYOUT.items():
        measurements = []
        for repetition in (1, 2, 3):
            directory = batch / f'w{workers:02d}-r{repetition}'
            path = directory / 'measurement.json'
            if not path.exists():
                continue
            item = json.loads(path.read_text())
            if item['status'] != 'complete' or item['workers'] != workers or item['nnodes'] != nodes or item['repetition'] != repetition:
                raise ValueError(f'Medição incompatível: {path}')
            if item['documents'] < 50000 or sum(item['worker_hosts'].values()) != workers or len(item['worker_hosts']) != nodes:
                raise ValueError(f'Corpus ou topologia inválidos: {path}')
            if len(list(directory.glob('part-*.npz'))) != item['partitions']:
                raise ValueError(f'Matrizes incompletas: {path}')
            for key in HEADER[2:]:
                if not math.isfinite(item[key]) or item[key] < 0:
                    raise ValueError(f'Tempo inválido: {path}')
            if not math.isclose(item['t_total'], item['t_serial'] + item['t_calc'], abs_tol=1e-8):
                raise ValueError(f'Decomposição inválida: {path}')
            identities.add((item['protocol'], item['dataset_manifest_sha256'], item['code_sha256'], item['documents'], item['partitions']))
            measurements.append(item)
        if len(measurements) != 3:
            missing.append(workers)
            continue
        rows.append(dict(nprocs=workers, nnodes=nodes,
                         **{key: f'{statistics.median(m[key] for m in measurements):.6f}' for key in HEADER[2:]}))
    if len(identities) > 1:
        raise ValueError('Repetições usam dataset, executor ou protocolo diferentes')
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        writer.writeheader()
        writer.writerows(rows)
    print(json.dumps(dict(configurations_complete=[r['nprocs'] for r in rows], missing=missing, output=str(output))))
    return rows, missing


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('batch')
    parser.add_argument('--output', default='resultados/speedup_corpus.csv')
    args = parser.parse_args()
    aggregate(args.batch, args.output)
