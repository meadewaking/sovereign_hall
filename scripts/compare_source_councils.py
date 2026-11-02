"""Explicit, nonexecuting paired source-council experiment on frozen packets.

Baseline: proposal then two reviewers. Candidate: blind readers then synthesis.
No investment promotion is implied. Each case is copied once, code/config frozen,
and every arm runs in a fresh process with isolated persistence and call records.
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


async def worker(run, case, side):
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(run / 'runtime'))
    from sovereign_hall.services.experiment_runtime import isolate_process, RecordedLLM, write_json
    protocol = json.loads((run / 'protocol.json').read_text())
    if side not in ('baseline', 'candidate') or case not in protocol['packets']:
        raise ValueError('unregistered arm')
    for relative, expected in protocol['code_sha256'].items():
        if digest(run / 'runtime' / 'sovereign_hall' / relative) != expected:
            raise RuntimeError('code changed after freeze: ' + relative)
    if digest(run / (case + '.json')) != protocol['packets'][case]:
        raise RuntimeError('input changed after freeze')
    if digest(Path(__file__)) != protocol['worker_sha256']:
        raise RuntimeError('worker changed after freeze')
    arm = run / (case + '_' + side)
    isolate_process(arm)
    from sovereign_hall.services.llm_client import LLMClient
    from sovereign_hall.services.source_council import run_source_council
    from sovereign_hall.services.blind_source_council import run_blind_source_council
    client = LLMClient(max_concurrent=2, timeout=180, max_retries=0)
    if client.provider == 'local' or not client.client:
        raise RuntimeError('actual model unavailable')
    recorded = RecordedLLM(client, arm, max_calls=3)
    packet = json.loads((run / (case + '.json')).read_text())
    events = []

    async def record(role, kind, payload):
        events.append(dict(role=role, kind=kind, payload=payload))
        write_json(arm / 'events.json', events)

    result = {'orders': 0, 'scope': 'source judgment; no investment approval or return evidence'}
    try:
        council = run_source_council if side == 'baseline' else run_blind_source_council
        result.update(await asyncio.wait_for(council(
            recorded, packet, topic=protocol['topic'], record=record), timeout=400))
    except Exception as exc:
        result.update(status='failed', error_type=type(exc).__name__)
    finally:
        await client.close()
        result['accounting'] = recorded.summary()
        result['finished_at'] = datetime.now().astimezone().isoformat()
        write_json(arm / 'result.json', result)


def main():
    if len(sys.argv) == 5 and sys.argv[1] == '--worker':
        asyncio.run(worker(Path(sys.argv[2]).resolve(), sys.argv[3], sys.argv[4]))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--topic', required=True)
    parser.add_argument('--criteria', type=Path, required=True,
                        help='Precommitted facts, rejection criteria, and input provenance')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root.parent))
    from sovereign_hall.services.experiment_runtime import snapshot_code, write_json
    run = args.output.resolve()
    run.mkdir(parents=True, exist_ok=False)
    packets = {}
    for i, path in enumerate(args.packet, 1):
        case = 'case_' + str(i)
        shutil.copyfile(path, run / (case + '.json'))
        packets[case] = digest(run / (case + '.json'))
    shutil.copyfile(args.criteria, run / 'criteria.json')
    shutil.copyfile(__file__, run / 'worker.py')
    manifest = snapshot_code(root, run / 'runtime' / 'sovereign_hall')
    protocol = dict(version='blind_source_council_comparison_v1',
                    frozen_at=datetime.now().astimezone().isoformat(),
                    code_sha256=manifest, packets=packets,
                    criteria_sha256=digest(run / 'criteria.json'),
                    worker_sha256=digest(run / 'worker.py'), topic=args.topic,
                    budget='3 calls, 6000 output tokens per call, no retries, 400 seconds per arm',
                    execution=False, return_improvement=None)
    write_json(run / 'protocol.json', protocol)
    results = {}
    for i, case in enumerate(packets):
        # Counterbalance order across cases, never retry selectively.
        for side in (('baseline', 'candidate') if i % 2 == 0 else ('candidate', 'baseline')):
            name = case + '_' + side
            with (run / (name + '.log')).open('w') as log:
                try:
                    outcome = subprocess.run([sys.executable, '-B', str(run / 'worker.py'),
                                              '--worker', str(run), case, side],
                                             stdout=log, stderr=log, timeout=430)
                    process_status = 'returned' if outcome.returncode == 0 else 'process_failed'
                except subprocess.TimeoutExpired:
                    process_status = 'process_timeout'
            path = run / name / 'result.json'
            results[name] = json.loads(path.read_text()) if path.exists() else {'status': process_status}
            write_json(run / 'result.json', results)
            print(name, results[name]['status'], flush=True)


if __name__ == '__main__':
    main()
