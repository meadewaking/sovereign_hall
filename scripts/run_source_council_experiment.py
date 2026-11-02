"""Freeze and run two isolated council replicas to verify experiment accounting.

Replicas share input/code but are NOT an investment treatment/control pair.
API calls are explicit; completed output directories cannot be reused.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def worker(run, name):
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(run / 'runtime'))
    from sovereign_hall.services.experiment_runtime import isolate_process, RecordedLLM, write_json
    protocol = json.loads((run / 'protocol.json').read_text())
    for relative, expected in protocol['code_sha256'].items():
        if digest(run / 'runtime' / 'sovereign_hall' / relative) != expected:
            raise RuntimeError('code changed after freeze: ' + relative)
    if digest(run / 'packet.json') != protocol['packet_sha256']:
        raise RuntimeError('input changed after freeze')
    if digest(Path(__file__)) != protocol['worker_sha256']:
        raise RuntimeError('worker changed after freeze')
    arm = run / name
    isolate_process(arm)
    from sovereign_hall.services.llm_client import LLMClient
    from sovereign_hall.services.source_council import run_source_council
    client = LLMClient(max_concurrent=2, timeout=180, max_retries=0)
    if client.provider == 'local' or not client.client:
        raise RuntimeError('actual model unavailable')
    recorded = RecordedLLM(client, arm, max_calls=3)
    packet = json.loads((run / 'packet.json').read_text())
    events = []

    async def record(role, kind, payload):
        events.append(dict(role=role, kind=kind, payload=payload))
        write_json(arm / 'events.json', events)

    result = {'orders': 0, 'scope': 'engineering replica, not investment quality comparison'}
    try:
        result.update(await asyncio.wait_for(run_source_council(
            recorded, packet, topic=protocol['topic'], record=record), timeout=400))
    except BaseException as exc:
        result.update(status='failed', error_type=type(exc).__name__)
    finally:
        await client.close()
        result['accounting'] = recorded.summary()
        result['finished_at'] = datetime.now().astimezone().isoformat()
        write_json(arm / 'result.json', result)


def main():
    if len(sys.argv) == 4 and sys.argv[1] == '--worker':
        asyncio.run(worker(Path(sys.argv[2]).resolve(), sys.argv[3]))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--topic', required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root.parent))
    from sovereign_hall.services.experiment_runtime import snapshot_code, write_json
    run = args.output.resolve()
    run.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(args.packet, run / 'packet.json')
    shutil.copyfile(__file__, run / 'worker.py')
    manifest = snapshot_code(root, run / 'runtime' / 'sovereign_hall')
    protocol = dict(version='isolated_council_accounting_v1',
                    frozen_at=datetime.now().astimezone().isoformat(),
                    code_sha256=manifest, packet_sha256=digest(run / 'packet.json'),
                    worker_sha256=digest(run / 'worker.py'), topic=args.topic,
                    endpoint='zero startup; per-transport usage reconciles; two independent stats paths',
                    budget='two replicas, max three outer calls each; zero retries; 400 seconds each',
                    return_improvement=None, input_status='known regression; not holdout')
    write_json(run / 'protocol.json', protocol)
    for name in ('replica_a', 'replica_b'):
        with (run / (name + '.log')).open('w') as log:
            result = subprocess.run([sys.executable, '-B', str(run / 'worker.py'),
                                     '--worker', str(run), name], stdout=log, stderr=log,
                                    timeout=430)
        if result.returncode:
            raise RuntimeError(name + ' failed; inspect arm log')
    results = {name: json.loads((run / name / 'result.json').read_text())
               for name in ('replica_a', 'replica_b')}
    write_json(run / 'result.json', results)
    print(json.dumps({n: {'status': r['status'], 'accounting': r['accounting']}
                      for n, r in results.items()}, ensure_ascii=False))


if __name__ == '__main__':
    main()
