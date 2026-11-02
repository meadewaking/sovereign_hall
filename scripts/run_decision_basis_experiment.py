"""Freeze and compare reviewed decision programs with the existing raw council.

Both arms receive the same original documents, reviewed bindings, numerical
derivations, account, cutoff and policies. Only the candidate's typed program
compiler can admit premises into its decision. No production DB or orders.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def packet_for(data, case):
    from sovereign_hall.services.action_research import build_packet
    from sovereign_hall.services.decision_basis import compile_basis, common_document
    original = build_packet(data['state'], data['documents'], data['universe'], objective=case['objective'])
    initial = compile_basis(original, data['certificate'])
    shared = common_document(initial, data['state']['cutoff'])
    packet = build_packet(data['state'], [*data['documents'], shared], data['universe'], objective=case['objective'])
    return packet, compile_basis(packet, data['certificate'])


async def worker(run, name, side):
    if side not in {'baseline', 'candidate'} or not re.fullmatch(r'[a-z0-9_]{1,40}', name):
        raise ValueError('invalid arm identity')
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(run / 'runtime'))
    from sovereign_hall.services.experiment_runtime import isolate_process, RecordedLLM, write_json
    protocol = json.loads((run / 'protocol.json').read_text())
    if digest(run / 'input.json') != protocol['input_sha256'] or digest(Path(__file__)) != protocol['worker_sha256']:
        raise RuntimeError('changed input/worker')
    for relative, expected in protocol['code_sha256'].items():
        if digest(run / 'runtime' / 'sovereign_hall' / relative) != expected:
            raise RuntimeError('changed frozen code: ' + relative)
    data = json.loads((run / 'input.json').read_text())
    case = next(c for c in data['cases'] if c['id'] == name)
    directory = run / (name + '_' + side)
    isolate_process(directory)
    from sovereign_hall.core.config import get_config
    from sovereign_hall.services.llm_client import LLMClient
    from sovereign_hall.services.action_research import run_action_research
    from sovereign_hall.services.decision_basis import run_decision_basis
    if get_config().get('simulation', {}) != data['state']['policy']:
        raise RuntimeError('frozen account policy differs from source config')
    packet, basis = packet_for(data, case)
    write_json(directory / 'packet.json', packet)
    write_json(directory / 'basis.json', basis)
    client = LLMClient(max_concurrent=2, timeout=150, max_retries=0)
    if client.provider == 'local' or not client.client:
        raise RuntimeError('actual model unavailable')
    recorded = RecordedLLM(client, directory, max_calls=protocol['budget']['max_calls_per_arm'])

    class LoggedClient:
        async def chat(self, **request):
            if client.get_stats()['total_cost'] >= protocol['budget']['estimated_usd_per_arm']:
                raise RuntimeError('frozen estimated cost budget exhausted')
            return await recorded.chat(**{**request, 'use_cache': False})

    events = []
    async def record(role, kind, payload):
        events.append(dict(role=role, kind=kind, payload=payload))
        write_json(directory / 'events.json', events)
    start = time.monotonic()
    result = dict(case=name, side=side, status='started', orders=0,
                  historical_predictions_written=0, return_improvement=None)
    try:
        route = (run_decision_basis(LoggedClient(), packet, data['certificate'], record=record)
                 if side == 'candidate' else run_action_research(LoggedClient(), packet, record=record))
        output = await asyncio.wait_for(route, protocol['budget']['wall_seconds_per_arm'])
        write_json(directory / 'research_queue.json', output['research_queue'])
        result.update(status='completed', committee_ran=True, decision=output)
    except BaseException as exc:
        result.update(status='failed', error_type=type(exc).__name__, error=str(exc)[:300])
        if isinstance(exc, asyncio.CancelledError):
            raise
    finally:
        await client.close()
        result.update(accounting=recorded.summary(), elapsed_seconds=time.monotonic()-start,
                      finished_at=datetime.now().astimezone().isoformat())
        write_json(directory / 'result.json', result)
        print(json.dumps(dict(case=name, side=side, status=result['status'], calls=len(recorded.calls))), flush=True)


def main():
    if len(sys.argv) == 6 and sys.argv[1] == '--worker':
        asyncio.run(worker(Path(sys.argv[2]).resolve(), sys.argv[3], sys.argv[4])); return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--freeze-only', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root.parent))
    from sovereign_hall.services.experiment_runtime import snapshot_code, write_json
    data = json.loads(args.input.read_text())
    names = [case['id'] for case in data['cases']]
    if not names or len(names) != len(set(names)) or any(not re.fullmatch(r'[a-z0-9_]{1,40}', n) for n in names):
        raise ValueError('unique local case identifiers required')
    for case in data['cases']:
        packet_for(data, case)
    run = args.output.resolve()
    run.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(args.input, run / 'input.json')
    shutil.copyfile(__file__, run / 'worker.py')
    manifest = snapshot_code(root, run / 'runtime' / 'sovereign_hall')
    for source in (root / 'services' / 'calendar_data').glob('*'):
        if source.is_file():
            target = run / 'runtime' / 'sovereign_hall' / 'services' / 'calendar_data' / source.name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
            manifest[str(source.relative_to(root))] = digest(target)
    protocol = dict(experiment_id=data['experiment_id'],
        frozen_at=datetime.now().astimezone().isoformat(), cutoff=data['state']['cutoff'],
        input_sha256=digest(run/'input.json'), worker_sha256=digest(run/'worker.py'), code_sha256=manifest,
        control='portfolio_action_research_v1, same original sources plus common reviewed numerical supplement',
        candidate='reviewed source bindings -> deterministic calculation graph -> typed proposal/review programs -> compiled decision',
        considered_architectures=data['considered_architectures'],
        budget=dict(max_calls_per_arm=6, estimated_usd_per_arm=.15, wall_seconds_per_arm=600),
        downstream_endpoint=data['acceptance'], input_status='seen issuer regression, current quotes; not holdout',
        return_improvement=None, production_promotion=False)
    write_json(run / 'protocol.json', protocol)
    if args.freeze_only:
        print(str(run)); return
    for case in data['cases']:
        children = []
        for side in ('baseline', 'candidate'):
            log = (run / (case['id'] + '_' + side + '.log')).open('w')
            child = subprocess.Popen([sys.executable, '-B', str(run/'worker.py'), '--worker',
                str(run), case['id'], side, 'frozen'], stdout=log, stderr=log)
            children.append((child, log))
        for child, log in children:
            try:
                child.wait(timeout=640)
            except subprocess.TimeoutExpired:
                child.terminate()
                child.wait(timeout=20)
                raise RuntimeError('bounded arm timed out; retained failure log')
            finally:
                log.close()
            if child.returncode:
                raise RuntimeError('arm subprocess failed; inspect retained log')
    outputs = {p.parent.name: json.loads(p.read_text()) for p in run.glob('*/result.json')}
    write_json(run / 'result.json', outputs)
    print(json.dumps({k: dict(status=v['status'], calls=v['accounting']['outer_calls']) for k,v in outputs.items()}))


if __name__ == '__main__':
    main()
