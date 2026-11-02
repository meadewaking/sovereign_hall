"""Freeze acquisition arms and run unchanged actual model proposal/committee.

Inputs are acquired before this command: one common account/cutoff and separate
document sets. This is an isolated research architecture experiment, never a
return-promotion comparator or an account execution entry point.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
import hashlib
import json
import logging.handlers
from pathlib import Path
import shutil
import subprocess
import sys
import time


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def worker(run, side):
    if side not in {'baseline', 'candidate'}:
        raise ValueError('unknown acquisition arm')
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(run / 'runtime'))
    from sovereign_hall.services.experiment_runtime import isolate_process, RecordedLLM, write_json
    protocol = json.loads((run / 'protocol.json').read_text())
    for relative, expected in protocol['code_sha256'].items():
        if digest(run / 'runtime' / 'sovereign_hall' / relative) != expected:
            raise RuntimeError('frozen source drift: ' + relative)
    if digest(run / 'input.json') != protocol['input_sha256'] or digest(Path(__file__)) != protocol['worker_sha256']:
        raise RuntimeError('frozen input/worker drift')
    data = json.loads((run / 'input.json').read_text())
    arm = run / side
    isolate_process(arm)
    from sovereign_hall.core.config import get_config
    from sovereign_hall.core import Document
    from sovereign_hall.services.llm_client import LLMClient
    from sovereign_hall.services.database import DatabaseService
    from sovereign_hall.application.run_research_round import ResearchRoundCoordinator
    from sovereign_hall.services.committee_meeting_store import CommitteeMeetingStore
    from sovereign_hall.services.decision_tracker import DecisionRecorder

    config = get_config()
    if config.get('simulation', {}) != data['state']['policy']:
        raise RuntimeError('frozen execution policy differs')
    original = logging.handlers.RotatingFileHandler
    def scoped_handler(filename, *args, **kwargs):
        target = arm / 'data' / 'logs' / Path(filename).name
        target.parent.mkdir(parents=True, exist_ok=True)
        return original(target, *args, **kwargs)
    logging.handlers.RotatingFileHandler = scoped_handler
    try:
        from sovereign_hall import run_discussion as rd
    finally:
        logging.handlers.RotatingFileHandler = original
    cutoff = datetime.fromisoformat(data['state']['cutoff'])
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cutoff.astimezone(tz) if tz else cutoff.replace(tzinfo=None)
    rd.datetime = FrozenDatetime
    boundaries = []
    async def no_prediction(self, **kwargs):
        boundaries.append(kwargs)
        write_json(arm / 'not_historical_predictions.json', boundaries)
        return None
    DecisionRecorder.record_decision = no_prediction
    client = LLMClient(max_concurrent=4, timeout=180, max_retries=0)
    if client.provider == 'local' or not client.client:
        raise RuntimeError('actual model unavailable')
    recorded = RecordedLLM(client, arm, max_calls=protocol['budget']['max_calls_per_arm'])
    class LoggedClient:
        def __getattr__(self, key):
            return getattr(client, key)
        async def chat(self, **kwargs):
            if client.get_stats()['total_cost'] >= protocol['budget']['estimated_usd_per_arm']:
                raise RuntimeError('frozen estimated cost budget exhausted')
            return await recorded.chat(**{**kwargs, 'use_cache': False})

    result = dict(side=side, status='started', orders=0, historical_predictions_written=0,
        source_arm=data['arms'][side]['description'], investment_return_improvement=None,
        promotion_status='isolated', committee_ran=False)
    started = time.monotonic()
    db = DatabaseService(str(arm / 'data' / 'sovereign_hall.db'))
    try:
        await db._init_db()
        coordinator = ResearchRoundCoordinator(db)
        round_state = await coordinator.start(base_topic=data['objective'],
            research_objective=protocol['experiment_id'], input_hash=protocol['input_sha256'])
        docs = [Document.from_dict(d) for d in data['arms'][side]['documents']]
        common = ('【双方共同的权威账户、当前受控研究报价和执行配置】\n' +
                  json.dumps(data['state'], ensure_ascii=False) +
                  '\n公告中的业绩预告是未审计预测，不是已实现业绩。原文不证明价格会重估。'
                  '可以合理HOLD；当前报价值仅供研究，未来执行必须重取受控新鲜行情。')

        async def pipeline():
            proposals = await rd.stage2_deep_research(LoggedClient(), docs, data['objective'],
                db_service=db, lessons_prompt=common, round_id=round_state.id)
            result['proposals'] = proposals
            write_json(arm / 'proposals.json', proposals)
            scoped = [p for p in proposals if p['ticker'] in data['universe']]
            result['outside_universe'] = [p['ticker'] for p in proposals if p['ticker'] not in data['universe']]
            for proposal in scoped:
                await db.add_proposal(proposal, round_id=round_state.id)
            async def callback(decision):
                await coordinator.record_event(round_state.id, 'IsolatedCommitteeDecision', decision)
            discussion, decisions = await rd.stage3_ic_discussion(LoggedClient(), None, scoped, data['objective'],
                lessons_prompt=common, round_id=round_state.id, decision_callback=callback,
                committee_store=CommitteeMeetingStore(db))
            result.update(status='completed', decisions=decisions, discussion=discussion,
                          committee_ran=bool(scoped), round_id=round_state.id)
        await asyncio.wait_for(pipeline(), protocol['budget']['wall_seconds_per_arm'])
    except asyncio.CancelledError:
        result.update(status='cancelled', reason='controller_interrupt; completed durable decisions preserved')
        raise
    except Exception as exc:
        result.update(status='failed', error_type=type(exc).__name__, error=str(exc))
    finally:
        await db.close()
        await client.close()
        result.update(accounting=recorded.summary(), elapsed_seconds=time.monotonic()-started,
                      finished_at=datetime.now().astimezone().isoformat())
        write_json(arm / 'result.json', result)
        print(json.dumps({k:result[k] for k in ('side','status','committee_ran','elapsed_seconds')}, ensure_ascii=False))


def main():
    if len(sys.argv) == 4 and sys.argv[1] == '--worker':
        asyncio.run(worker(Path(sys.argv[2]).resolve(), sys.argv[3]))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--freeze-only', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root.parent))
    from sovereign_hall.services.experiment_runtime import snapshot_code, write_json
    data = json.loads(args.input.read_text())
    if set(data['arms']) != {'baseline', 'candidate'} or not data['universe']:
        raise ValueError('two acquisition arms and common frozen universe required')
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
    protocol = dict(experiment_id=data['experiment_id'], frozen_at=datetime.now().astimezone().isoformat(),
        cutoff=data['state']['cutoff'], input_sha256=digest(run / 'input.json'), worker_sha256=digest(run / 'worker.py'),
        code_sha256=manifest, budget=data['model_budget'], acceptance=data['acceptance'],
        comparison='bounded Bing acquisition vs primary event agenda, same canonical downstream code/config/models',
        return_comparator_faithful=False, return_improvement=None, production_promotion=False)
    write_json(run / 'protocol.json', protocol)
    if args.freeze_only:
        print(str(run)); return
    children = []
    for side in ('baseline', 'candidate'):
        log = (run / (side + '.log')).open('w')
        process = subprocess.Popen([sys.executable, '-B', str(run / 'worker.py'), '--worker', str(run), side],
                                   stdout=log, stderr=log)
        children.append((side, process, log))
    for side, process, log in children:
        process.wait(timeout=data['model_budget']['wall_seconds_per_arm']+60)
        log.close()
        if process.returncode and not (run/side/'result.json').exists():
            raise RuntimeError('arm setup failed; retained logs must be inspected')
    results = {side:json.loads((run / side / 'result.json').read_text()) for side in ('baseline','candidate')}
    write_json(run / 'result.json', results)
    print(json.dumps({s:dict(status=r['status'],calls=r['accounting']['outer_calls']) for s,r in results.items()}))


if __name__ == '__main__':
    main()
