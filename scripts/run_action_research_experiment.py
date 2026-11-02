"""Run a frozen, non-executing action-research candidate against canonical HS.

Each arm is a new isolated subprocess, with separate DB and model accounting.
Input JSON supplies documents, authoritative account state and case objectives.
Nothing in this command promotes a policy or writes to the production account.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
import hashlib
import json
import logging.handlers
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def worker(run, case_name, side):
    if side not in {'baseline', 'candidate'} or not re.fullmatch(r'[a-z0-9_]{1,40}', case_name):
        raise ValueError('invalid arm identity')
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(run / 'runtime'))
    from sovereign_hall.services.experiment_runtime import isolate_process, RecordedLLM, write_json
    protocol = json.loads((run / 'protocol.json').read_text())
    for name, expected in protocol['code_sha256'].items():
        if digest(run / 'runtime' / 'sovereign_hall' / name) != expected:
            raise RuntimeError('frozen source changed: ' + name)
    if digest(run / 'input.json') != protocol['input_sha256'] or digest(Path(__file__)) != protocol['worker_sha256']:
        raise RuntimeError('frozen input/worker changed')
    data = json.loads((run / 'input.json').read_text())
    case = next(c for c in data['cases'] if c['id'] == case_name)
    directory = run / (case_name + '_' + side)
    isolate_process(directory)
    from sovereign_hall.core.config import get_config
    from sovereign_hall.services.llm_client import LLMClient
    from sovereign_hall.services.action_research import build_packet, run_action_research
    from sovereign_hall.core import Document
    config = get_config()
    if config.get('simulation', {}) != data['state']['policy']:
        raise RuntimeError('account policy differs from frozen effective config')
    client = LLMClient(max_concurrent=2, timeout=150, max_retries=0)
    if client.provider == 'local' or not client.client:
        raise RuntimeError('actual model unavailable')
    recorded = RecordedLLM(client, directory, max_calls=protocol['budget']['max_calls_per_arm'])
    # Canonical agents access client attributes as well as .chat. Preserve the
    # real client but route every call through the independent transport ledger.
    class LoggedClient:
        def __getattr__(self, key):
            return getattr(client, key)
        async def chat(self, **kwargs):
            if client.get_stats()['total_cost'] >= protocol['budget']['estimated_usd_per_arm']:
                raise RuntimeError('frozen cost budget exhausted')
            return await recorded.chat(**{**kwargs, 'use_cache': False})
    llm = LoggedClient()
    events = []
    async def record(role, kind, payload):
        events.append(dict(role=role, kind=kind, payload=payload))
        write_json(directory / 'events.json', events)
    started = time.monotonic()
    result = dict(side=side, case=case_name, status='started', orders=0,
                  historical_predictions_written=0, investment_return_improvement=None)
    db = None
    try:
        packet = build_packet(data['state'], data['documents'], data['universe'], objective=case['objective'])
        write_json(directory / 'packet.json', packet)
        if side == 'candidate':
            output = await asyncio.wait_for(run_action_research(llm, packet, record=record),
                                             protocol['budget']['wall_seconds_per_arm'])
            result.update(status='completed', decision=output, committee_ran=True)
            write_json(directory / 'research_queue.json', output['research_queue'])
        else:
            # The canonical entry creates its logger relative to __file__, not
            # DATA_DIR. Scope only that import-time log sink to this arm.
            original_handler = logging.handlers.RotatingFileHandler
            def scoped_handler(filename, *args, **kwargs):
                target = directory / 'data' / 'logs' / Path(filename).name
                target.parent.mkdir(parents=True, exist_ok=True)
                return original_handler(target, *args, **kwargs)
            logging.handlers.RotatingFileHandler = scoped_handler
            try:
                from sovereign_hall import run_discussion as rd
            finally:
                logging.handlers.RotatingFileHandler = original_handler
            from sovereign_hall.services.database import DatabaseService
            from sovereign_hall.application.run_research_round import ResearchRoundCoordinator
            from sovereign_hall.services.committee_meeting_store import CommitteeMeetingStore
            from sovereign_hall.services.decision_tracker import DecisionRecorder
            cutoff = datetime.fromisoformat(data['state']['cutoff'])
            class FrozenDatetime(datetime):
                @classmethod
                def now(cls, tz=None):
                    return cutoff.astimezone(tz) if tz else cutoff.replace(tzinfo=None)
            rd.datetime = FrozenDatetime
            config._config['research']['committee_proposal_limit'] = 1  # one issuer, same universe
            # Replayed model judgments are observations at today's frozen input,
            # not historical price predictions. Suppress only downstream forecast
            # bookkeeping; proposal and all actual committee stages are unchanged.
            async def no_historical_prediction(self, **kwargs):
                await record('canonical_prediction_boundary', 'not_a_prediction', kwargs)
                return None
            DecisionRecorder.record_decision = no_historical_prediction
            db = DatabaseService(str(directory / 'data' / 'sovereign_hall.db'))
            await db._init_db()
            coordinator = ResearchRoundCoordinator(db)
            state = await coordinator.start(base_topic=case['objective'], research_objective=protocol['experiment_id'],
                                              input_hash=protocol['input_sha256'])
            docs = [Document.from_dict(d) for d in data['documents']]
            # Both routes have the identical complete documents, account, actual
            # policy and cutoff. How they allocate/use that information is the
            # treatment; the baseline retains its real excerpt and debate rules.
            lessons = '【权威共同账户与政策输入】\n' + json.dumps(data['state'], ensure_ascii=False)
            async def pipeline():
                proposals = await rd.stage2_deep_research(llm, docs, case['objective'], db_service=db,
                                                         lessons_prompt=lessons, round_id=state.id)
                result['proposals'] = proposals
                write_json(directory / 'proposals.json', proposals)
                scoped = [p for p in proposals if p['ticker'] in data['universe']]
                result['outside_universe'] = [p['ticker'] for p in proposals if p['ticker'] not in data['universe']]
                async def decision_callback(decision):
                    await coordinator.record_event(state.id, 'IsolatedCommitteeDecision', decision)
                for proposal in scoped:
                    await db.add_proposal(proposal, round_id=state.id)
                discussion, decisions = await rd.stage3_ic_discussion(llm, None, scoped, case['objective'],
                    lessons_prompt=lessons, round_id=state.id, decision_callback=decision_callback,
                    committee_store=CommitteeMeetingStore(db))
                (directory / 'discussion.txt').write_text(discussion)
                result.update(status='completed', decisions=decisions, committee_ran=bool(decisions), round_id=state.id)
            await asyncio.wait_for(pipeline(), protocol['budget']['wall_seconds_per_arm'])
    except BaseException as exc:
        result.update(status='failed', error_type=type(exc).__name__, error=str(exc)[:200])
        if isinstance(exc, asyncio.CancelledError):
            raise
    finally:
        if db:
            await db.close()
        await client.close()
        result.update(accounting=recorded.summary(), elapsed_seconds=time.monotonic()-started,
                      finished_at=datetime.now().astimezone().isoformat())
        write_json(directory / 'result.json', result)
        print(json.dumps({k: result[k] for k in ('case', 'side', 'status', 'elapsed_seconds')}, ensure_ascii=False), flush=True)


def main():
    if len(sys.argv) == 6 and sys.argv[1] == '--worker':
        asyncio.run(worker(Path(sys.argv[2]).resolve(), sys.argv[3], sys.argv[4]))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--freeze-only', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root.parent))
    from sovereign_hall.services.experiment_runtime import snapshot_code, write_json
    from sovereign_hall.services.action_research import build_packet
    data = json.loads(args.input.read_text())
    names = [case['id'] for case in data['cases']]
    if len(set(names)) != len(names) or any(not re.fullmatch(r'[a-z0-9_]{1,40}', name) for name in names):
        raise ValueError('unique, local case identifiers required')
    for case in data['cases']:
        build_packet(data['state'], data['documents'], data['universe'], objective=case['objective'])
    run = args.output.resolve()
    run.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(args.input, run / 'input.json')
    shutil.copyfile(__file__, run / 'worker.py')
    manifest = snapshot_code(root, run / 'runtime' / 'sovereign_hall')
    for path in (root / 'services' / 'calendar_data').glob('*'):
        if path.is_file():
            target = run / 'runtime' / 'sovereign_hall' / 'services' / 'calendar_data' / path.name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)
            manifest[str(path.relative_to(root))] = digest(target)
    protocol = dict(experiment_id='portfolio_action_research_20261013_v1',
        frozen_at=datetime.now().astimezone().isoformat(), cutoff=data['state']['cutoff'],
        input_sha256=digest(run/'input.json'), worker_sha256=digest(run/'worker.py'), code_sha256=manifest,
        control='current canonical stage2 and stage3; forecast persistence suppressed; all other stages unchanged',
        candidate='authoritative action frontier + cost scenarios + specific acquisition queue + three-seat evidence council',
        compared_designs=['append authoritative state to existing discussion', 'action-first research allocation'],
        budget=dict(max_calls_per_arm=32, estimated_usd_per_arm=.35, wall_seconds_per_arm=1200),
        downstream_endpoint=data['acceptance'], input_status='coding-agent-reviewed current regression; not holdout',
        return_improvement=None, production_promotion=False)
    write_json(run / 'protocol.json', protocol)
    if args.freeze_only:
        print(str(run)); return
    # Process concurrency, not agent delegation. Run both routes for one case at
    # a time; never sample a completed arm again for a more favourable response.
    for case in data['cases']:
        children=[]
        for side in ('baseline', 'candidate'):
            log=(run/(case['id']+'_'+side+'.log')).open('w')
            child=subprocess.Popen([sys.executable, '-B', str(run/'worker.py'), '--worker',
                str(run), case['id'], side, 'frozen'], stdout=log, stderr=log)
            children.append((child, log))
        for child, log in children:
            child.wait(timeout=1240); log.close()
            if child.returncode:
                raise RuntimeError('arm subprocess failed; inspect retained logs')
    outputs={p.parent.name:json.loads(p.read_text()) for p in run.glob('*/result.json')}
    write_json(run/'result.json',outputs)
    print(json.dumps({k:dict(status=v['status'], calls=v['accounting']['outer_calls']) for k,v in outputs.items()}))


if __name__ == '__main__':
    main()
