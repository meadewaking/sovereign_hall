"""Verify sealed inputs and replay recorded candidate judgments, without a model."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    args = parser.parse_args()
    run = args.run.resolve()
    protocol = json.loads((run / 'protocol.json').read_text())
    if digest(run / 'input.json') != protocol['input_sha256'] or digest(run / 'worker.py') != protocol['worker_sha256']:
        raise RuntimeError('frozen inputs/worker changed')
    for name, expected in protocol['code_sha256'].items():
        if digest(run / 'runtime' / 'sovereign_hall' / name) != expected:
            raise RuntimeError('frozen code changed: ' + name)
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(run / 'runtime'))
    from sovereign_hall.services.action_research import compile_result, validate_choice, verify_packet
    from sovereign_hall.services.experiment_runtime import write_json
    outputs = {}
    for arm in sorted(run.glob('*_candidate')):
        result = json.loads((arm / 'result.json').read_text())
        packet = json.loads((arm / 'packet.json').read_text())
        verify_packet(packet)
        calls = [json.loads(path.read_text()) for path in sorted(arm.glob('model_call_*.json'))]
        if len(calls) != 3 or any(c['status'] != 'success' for c in calls):
            raise RuntimeError('incomplete candidate calls')
        choices = []
        for call in calls:
            request_packet = json.loads(call['request']['user'].split('\n待审提案=', 1)[0])
            if request_packet != packet or call['request']['use_cache']:
                raise RuntimeError('model received different input')
            choices.append(validate_choice(call['response'], packet)[0])
        replayed = compile_result(packet, choices[0], choices[1:])
        if replayed != result['decision']:
            raise RuntimeError('recorded decision differs from replay')
        if (result['orders'] or result['historical_predictions_written'] or
                not result['accounting']['usage_matches_client'] or result['accounting']['startup']['total_requests']):
            raise RuntimeError('isolation/accounting regression')
        outputs[arm.name] = dict(action=replayed['action'], quorum_met=replayed['quorum_met'],
                                 requests=len(replayed['research_queue']), replay_matches=True)
    recovery = run / 'control_harness_recovery'
    if recovery.exists():
        amendment = json.loads((recovery / 'harness_amendment.json').read_text())
        if amendment['original_protocol_sha256'] != digest(run / 'protocol.json'):
            raise RuntimeError('harness amendment applies to different protocol')
        for name in ('input.json', 'worker.py', 'protocol.json'):
            if digest(recovery / name) != digest(run / name):
                raise RuntimeError('recovered control changed experiment inputs')
        for name, expected in protocol['code_sha256'].items():
            if digest(recovery / 'runtime' / 'sovereign_hall' / name) != expected:
                raise RuntimeError('recovered control code changed')
        for arm in sorted(recovery.glob('*_baseline')):
            if not (arm / 'result.json').exists():
                raise RuntimeError('control is still running')
            result = json.loads((arm / 'result.json').read_text())
            outputs[arm.name] = dict(status=result['status'], committee_ran=result.get('committee_ran'),
                decisions=[dict(ticker=d['ticker'], direction=d['direction'], quorum_met=d['vote_quorum_met'])
                           for d in result.get('decisions', [])],
                calls=result['accounting']['outer_calls'], usage_matches_client=result['accounting']['usage_matches_client'])
    receipt = dict(sealed_source_count=len(protocol['code_sha256']), frozen_inputs_verified=True,
                   actual_model_calls=0, orders=0, return_improvement=None, outputs=outputs)
    write_json(run / 'replay_result.json', receipt)
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == '__main__':
    main()
