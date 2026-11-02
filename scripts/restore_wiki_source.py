#!/usr/bin/env python3
"""Export a verified source page, including pages kept in the dense Wiki archive."""
import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
from sovereign_hall.services.wiki_source_archive import read_source, SourcePageArchive


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=PROJECT_ROOT / 'data' / 'knowledge')
    parser.add_argument('--path', required=True, help='Original wiki/sources/name.md path')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--revision', help='Optional SHA-256 of a preserved historical revision')
    args = parser.parse_args()
    raw = (SourcePageArchive(args.root).read(args.path, revision=args.revision) if args.revision
           else read_source(args.root, args.path))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('xb') as output:
        output.write(raw)
    print(f'Verified source exported: {args.output.resolve()} ({len(raw)} bytes)')


if __name__ == '__main__':
    main()
