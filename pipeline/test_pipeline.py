"""Fixture de correção: DF global, L2, partições e invariância entre workers."""
from pathlib import Path
import hashlib
import json
import math
import tempfile
import unittest

import numpy as np
from distributed import Client, LocalCluster
from pipeline import execute, tokenize
from agregar import aggregate, LAYOUT


class PipelineTest(unittest.TestCase):
    def test_global_idf_and_worker_invariance(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            dataset = base / 'dataset'
            dataset.mkdir()
            files = []
            records = [(1, 'Gato gato e cão'), (2, 'Cão peixe'), (3, 'e de')]
            for i, record in enumerate(records):
                path = dataset / f'part-{i:03d}.jsonl'
                path.write_text(json.dumps(dict(id=record[0], text=record[1])) + '\n')
                files.append(dict(path=path.name, sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
            stop = dataset / 'stopwords-pt.txt'
            stop.write_text('e\nde\n')
            files.append(dict(path=stop.name, sha256=hashlib.sha256(stop.read_bytes()).hexdigest()))
            (dataset / 'manifest.json').write_text(json.dumps(dict(documents=3, files=files)))
            stats = []
            matrices = []
            for workers in (1, 2):
                with LocalCluster(n_workers=workers, threads_per_worker=1, processes=False, dashboard_address=None) as cluster:
                    with Client(cluster) as client:
                        out = base / f'w{workers}'
                        result, timing = execute(client, dataset, out, workers, 1, 1)
                        stats.append(result)
                        self.assertGreater(timing['t_total'], 0)
                        vocab = [json.loads(x) for x in (out / 'vocabulary.jsonl').read_text().splitlines()]
                        self.assertEqual([v['term'] for v in vocab], ['cão', 'gato', 'peixe'])
                        self.assertAlmostEqual(vocab[0]['idf'], math.log(4 / 3) + 1)
                        self.assertAlmostEqual(vocab[1]['idf'], math.log(2) + 1)
                        dense = np.zeros((3, 3))
                        for p in out.glob('part-*.npz'):
                            m = np.load(p)
                            for row, doc in enumerate(m['document_ids']):
                                start, end = m['indptr'][row:row + 2]
                                dense[doc - 1, m['indices'][start:end]] = m['data'][start:end]
                        matrices.append(dense)
            a, b = math.log(4 / 3) + 1, math.log(2) + 1
            expected = np.array([[a / math.sqrt(a*a + 4*b*b), 2*b / math.sqrt(a*a + 4*b*b), 0],
                                 [a / math.sqrt(a*a + b*b), 0, b / math.sqrt(a*a + b*b)], [0, 0, 0]])
            np.testing.assert_allclose(matrices[0], expected)
            np.testing.assert_array_equal(matrices[0], matrices[1])
            self.assertEqual(stats[0], stats[1])
            self.assertEqual(stats[0]['empty_after_stopwords'], 1)
            self.assertEqual(stats[0]['lengths_after_stopwords']['histogram'], {'0': 1, '2': 1, '3': 1})
            self.assertEqual(tokenize('AÇÃO ac\u0327a\u0303o 123 teste_ABC'), ['ação', 'ação', 'teste', 'abc'])
            (dataset / 'part-000.jsonl').write_text('{}\n')
            with LocalCluster(n_workers=1, threads_per_worker=1, processes=False, dashboard_address=None) as cluster:
                with Client(cluster) as client:
                    with self.assertRaisesRegex(ValueError, 'Dataset alterado'):
                        execute(client, dataset, base / 'tampered', 1, 1, 1)


    def test_medians_require_three_matching_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            for workers, nodes in LAYOUT.items():
                for repetition, elapsed in enumerate((5.0, 1.0, 3.0), 1):
                    directory = base / f'w{workers:02d}-r{repetition}'
                    directory.mkdir()
                    (directory / 'part-000.npz').write_bytes(b'fixture')
                    item = dict(status='complete', protocol='fixture', workers=workers, nnodes=nodes,
                                repetition=repetition, documents=50000, partitions=1,
                                worker_hosts={f'n{i}': workers // nodes for i in range(nodes)},
                                dataset_manifest_sha256='dataset', code_sha256='code',
                                t_total=elapsed, t_serial=.2 * elapsed, t_calc=.8 * elapsed)
                    (directory / 'measurement.json').write_text(json.dumps(item))
            rows, missing = aggregate(base, base / 'speedup.csv')
            self.assertEqual(len(rows), 6)
            self.assertFalse(missing)
            self.assertEqual(rows[0]['t_total'], '3.000000')
            (base / 'w32-r3/measurement.json').unlink()
            rows, missing = aggregate(base, base / 'partial.csv')
            self.assertEqual(len(rows), 5)
            self.assertEqual(missing, [32])
            path = base / 'w02-r1/measurement.json'
            item = json.loads(path.read_text())
            item['dataset_manifest_sha256'] = 'other-dataset'
            path.write_text(json.dumps(item))
            with self.assertRaisesRegex(ValueError, 'diferentes'):
                aggregate(base, base / 'invalid.csv')


if __name__ == '__main__':
    unittest.main()
