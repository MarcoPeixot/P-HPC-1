"""Baixa snapshots fixos e prepara shards JSONL no master do cluster."""
import argparse
import csv
import hashlib
import io
import json
from pathlib import Path
import unicodedata
import urllib.request
import zipfile

COMMIT = '4639429ec698d7821fc99a0bc665fa213d9fcd5a'
CSV_URL = f'https://raw.githubusercontent.com/americanas-tech/b2w-reviews01/{COMMIT}/B2W-Reviews01.csv'
CSV_SHA = '821fb0bf9f7230b0fba4e4f9fadd75a66d1a9ff0b1657810791d33007eb2ab38'
STOP_COMMIT = '550b6625bcef1f2abff2ff770a5a0d272c9c6b2a'
STOP_URL = f'https://raw.githubusercontent.com/nltk/nltk_data/{STOP_COMMIT}/packages/corpora/stopwords.zip'
STOP_SHA = '48c0e52d8b52546e827f53761fb30300c0ab94f70660d28bd65ba0a86270946b'


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def download(url, target, expected):
    if not target.exists():
        with urllib.request.urlopen(url, timeout=120) as response:
            data = response.read()
        if hashlib.sha256(data).hexdigest() != expected:
            raise ValueError(f'SHA-256 inesperado: {url}')
        target.write_bytes(data)
    if sha256(target) != expected:
        raise ValueError(f'Snapshot local alterado: {target}')


def prepare(directory, shards=128):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    raw = directory / 'B2W-Reviews01.csv'
    download(CSV_URL, raw, CSV_SHA)
    if (directory / 'manifest.json').exists():
        manifest = json.loads((directory / 'manifest.json').read_text())
        if manifest['partitions'] != shards or manifest['raw_sha256'] != CSV_SHA:
            raise ValueError('Preparação existente usa outro protocolo')
        for item in manifest['files']:
            if sha256(directory / item['path']) != item['sha256']:
                raise ValueError(f"Arquivo preparado alterado: {item['path']}")
        return manifest
    with urllib.request.urlopen(STOP_URL, timeout=120) as response:
        archive = response.read()
    if hashlib.sha256(archive).hexdigest() != STOP_SHA:
        raise ValueError('Snapshot de stopwords inesperado')
    words = zipfile.ZipFile(io.BytesIO(archive)).read('stopwords/portuguese').decode('utf-8').splitlines()
    stopwords = sorted({unicodedata.normalize('NFC', word).casefold() for word in words})
    stop = directory / 'stopwords-pt.txt'
    stop.write_text('\n'.join(stopwords) + '\n', encoding='utf-8')
    paths = [directory / f'part-{i:03d}.jsonl' for i in range(shards)]
    streams = [p.open('w', encoding='utf-8') for p in paths]
    rows = empty = documents = 0
    try:
        with raw.open(encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f, delimiter=',')
            if 'review_text' not in reader.fieldnames:
                raise ValueError('Coluna review_text ausente')
            for rows, row in enumerate(reader, 1):
                if None in row or row['review_text'] is None:
                    raise ValueError(f'Registro CSV inválido: {rows}')
                text = row['review_text'].strip()
                if not text:
                    empty += 1
                    continue
                record = {'id': rows, 'text': text}
                streams[documents % shards].write(json.dumps(record, ensure_ascii=False) + '\n')
                documents += 1
    finally:
        for stream in streams:
            stream.close()
    if documents < 50000:
        raise ValueError(f'Corpus insuficiente: {documents} documentos')
    files = [{'path': p.name, 'bytes': p.stat().st_size, 'sha256': sha256(p)} for p in [*paths, stop]]
    manifest = dict(source='https://github.com/americanas-tech/b2w-reviews01', commit=COMMIT,
                    raw_url=CSV_URL, raw_sha256=CSV_SHA, raw_bytes=raw.stat().st_size,
                    records=rows, excluded_empty=empty, documents=documents,
                    language='pt-BR', raw_format='CSV UTF-8, delimitador vírgula, aspas duplas',
                    document_field='review_text', partitions=shards, files=files,
                    prepared_bytes=sum(x['bytes'] for x in files), stopwords_count=len(stopwords),
                    stopwords_source=STOP_URL, stopwords_archive_sha256=STOP_SHA,
                    license='CC BY-NC-SA 4.0', attribution='B2W Digital — B2W-Reviews01')
    (directory / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n')
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--directory', default='/opt/ohpc/pub/datasets/b2w-reviews01')
    parser.add_argument('--shards', type=int, default=128)
    args = parser.parse_args()
    print(json.dumps(prepare(args.directory, args.shards), ensure_ascii=False, indent=2))
