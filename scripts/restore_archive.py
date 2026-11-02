#!/usr/bin/env python3
"""Verify and export an archived record without replacing production files."""
import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from sovereign_hall.services.storage_archive import ArchiveStore, canonical_bytes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive-path', type=Path, required=True)
    parser.add_argument('--kind', required=True)
    parser.add_argument('--key', required=True)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    locator = {'archive_path': str(args.archive_path), 'kind': args.kind,
               'key': args.key, 'revision': args.revision}
    value = ArchiveStore.read(locator)
    payload = value['content'] if args.kind in {'wiki_raw', 'wiki_page', 'wiki_log', 'wiki_log_gzip'} else canonical_bytes(value)
    if isinstance(payload, str):
        payload = payload.encode('utf-8')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('xb') as out:
        out.write(payload)
    print(json.dumps({'verified': True, 'output': str(args.output.resolve()), 'bytes': len(payload)}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
