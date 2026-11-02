"""Atomic, lossless storage for derived JSON caches with legacy read support."""
from __future__ import annotations

import gzip
import hashlib
import json
import os
from pathlib import Path


def compressed_path(path: Path) -> Path:
    return path.with_name(path.name + '.gz')


def read_json(path: Path):
    packed = compressed_path(path)
    if packed.exists() and (not path.exists() or packed.stat().st_mtime_ns >= path.stat().st_mtime_ns):
        return json.loads(gzip.decompress(packed.read_bytes()))
    return json.loads(path.read_bytes())


def write_bytes(path: Path, raw: bytes, *, max_output_bytes: int | None = None):
    packed_path = compressed_path(path)
    packed = gzip.compress(raw, compresslevel=6, mtime=0)
    if max_output_bytes is not None and len(packed) > max_output_bytes:
        raise RuntimeError('No capacity for compressed cache; original retained')
    packed_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = packed_path.with_name(f'.{packed_path.name}.{os.getpid()}.tmp')
    try:
        with temporary.open('wb') as output:
            output.write(packed)
            output.flush()
            os.fsync(output.fileno())
        if hashlib.sha256(gzip.decompress(temporary.read_bytes())).digest() != hashlib.sha256(raw).digest():
            raise RuntimeError('Compressed cache verification failed; original retained')
        temporary.replace(packed_path)
        handle = os.open(packed_path.parent, os.O_RDONLY)
        try:
            os.fsync(handle)
        finally:
            os.close(handle)
    finally:
        temporary.unlink(missing_ok=True)
    return packed_path


def write_json(path: Path, value, *, max_output_bytes: int | None = None):
    packed = write_bytes(path, json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode(),
                         max_output_bytes=max_output_bytes)
    path.unlink(missing_ok=True)
    return packed


def compress_existing(path: Path, *, max_output_bytes: int | None = None) -> dict:
    if not path.is_file():
        return {'compressed': False}
    original = path.read_bytes()
    json.loads(original)  # Only valid derived JSON caches are eligible.
    packed = write_bytes(path, original, max_output_bytes=max_output_bytes)
    if path.read_bytes() != original:
        return {'compressed': False, 'retained_changed': True}
    path.unlink()
    return {'compressed': True, 'original_bytes': len(original),
            'packed_bytes': packed.stat().st_size,
            'original_hash': hashlib.sha256(original).hexdigest()}
