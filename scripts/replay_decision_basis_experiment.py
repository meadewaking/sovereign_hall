"""Verify frozen code/common inputs and recompile recorded real model programs.

Offline only: no clients, account DB, price retrieval, predictions, or orders.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import sys


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',type=Path,required=True)
    args=parser.parse_args()
    run=args.run.resolve()
    sys.dont_write_bytecode=True
    sys.path.insert(0,str(run/'runtime'))
    protocol=json.loads((run/'protocol.json').read_text())
    if digest(run/'input.json')!=protocol['input_sha256'] or digest(run/'worker.py')!=protocol['worker_sha256']:
        raise ValueError('changed frozen input/worker')
    for name,expected in protocol['code_sha256'].items():
        if digest(run/'runtime'/'sovereign_hall'/name)!=expected:
            raise ValueError('changed frozen code: '+name)
    spec=importlib.util.spec_from_file_location('frozen_worker',run/'worker.py')
    worker=importlib.util.module_from_spec(spec);spec.loader.exec_module(worker)
    from sovereign_hall.services.action_research import validate_choice,compile_result
    from sovereign_hall.services.decision_basis import validate_program,compile_decision
    data=json.loads((run/'input.json').read_text())
    results={}
    for case in data['cases']:
        packet,basis=worker.packet_for(data,case)
        for side in ('baseline','candidate'):
            directory=run/(case['id']+'_'+side)
            original=json.loads((directory/'result.json').read_text())
            if original['status']!='completed':
                results[directory.name]=dict(reproduced=False,status=original['status']);continue
            if json.loads((directory/'packet.json').read_text())!=packet or json.loads((directory/'basis.json').read_text())!=basis:
                raise ValueError('changed stored common packet/basis')
            events=json.loads((directory/'events.json').read_text())
            roles=[e['role'] for e in events if e['kind']=='request']
            responses={e['role']:e['payload'] for e in events if e['kind']=='response'}
            if len(roles)!=3 or len(set(roles))!=3 or set(roles)!=set(responses):
                raise ValueError('incomplete/duplicate real seats')
            programs=[]
            for role in roles:
                row=responses[role]
                if side=='candidate':
                    compiled,errors=validate_program(row['raw'],packet,basis)
                    if compiled!=row['compiled'] or errors!=row['errors']:
                        raise ValueError('candidate validation replay mismatch')
                    programs.append(compiled['program'] if compiled else None)
                else:
                    parsed,errors=validate_choice(row['raw'],packet)
                    if parsed!=row['validated'] or errors!=row['errors']:
                        raise ValueError('control validation replay mismatch')
                    programs.append(parsed)
            output=(compile_decision(packet,basis,programs[0],programs[1:]) if side=='candidate'
                    else compile_result(packet,programs[0],programs[1:]))
            if output!=original['decision']:
                raise ValueError('committee decision replay mismatch: '+directory.name)
            results[directory.name]=dict(reproduced=True,action=output['action'],quorum_met=output['quorum_met'])
    print(json.dumps(dict(experiment_id=protocol['experiment_id'],verified_code_files=len(protocol['code_sha256']),
        results=results,new_model_calls=0,orders=0,return_improvement=None),ensure_ascii=False,indent=2))


if __name__=='__main__':main()
