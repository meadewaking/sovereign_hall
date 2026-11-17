"""Run one frozen nonexecuting financial-state arm in a fresh subprocess.

The control is the native DS/committee/K3.1 route. The candidate's actual
route is DS -> deterministic financial/scenario compiler -> independent K3.1.
Both persist identical original sources privately. No historical forecasts,
execution intents, main-account writes or model-output resampling.
"""
import asyncio
from datetime import datetime
import hashlib
import json
import logging.handlers
from pathlib import Path
import sys
import time


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def main(run, side):
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(run / 'runtime'))
    from sovereign_hall.services.experiment_runtime import isolate_process, RecordedLLM, write_json
    protocol = json.loads((run / 'protocol.json').read_text())
    for relative, expected in protocol['code_sha256'].items():
        if sha(run / 'runtime/sovereign_hall' / relative) != expected:
            raise RuntimeError('frozen source changed: ' + relative)
    if sha(run / 'input.json') != protocol['input_sha256'] or sha(Path(__file__)) != protocol['worker_sha256']:
        raise RuntimeError('input or worker changed')
    data = json.loads((run / 'input.json').read_text())
    directory = run / side
    isolate_process(directory)
    from sovereign_hall.core.config import get_config
    config = get_config()
    if config.get('simulation') != data['state']['policy']:
        raise RuntimeError('effective policy differs')
    config._config['model_collaboration']['state_path'] = str(directory / 'data/model_collaboration.json')
    # An explicit common-model recovery is a separate frozen diagnostic, never
    # a silent production fallback or a retry selected by an investment answer.
    if protocol.get('common_research_profile_override'):
        config._config['model_collaboration']['research_profile'] = protocol['common_research_profile_override']
    from sovereign_hall.services.model_collaboration import create_llm_client
    client = create_llm_client()
    if not getattr(client, 'collaboration_enabled', False):
        raise RuntimeError('actual production factory required')
    ledgers = {}
    research = client.research
    reviewer = client.reviewer.client_impl
    for name, transport in [('research', research), ('review', reviewer)]:
        target = directory / name
        target.mkdir()
        ledgers[name] = RecordedLLM(transport, target, max_calls=protocol['budget']['max_calls_per_arm'])

    class LoggedClient:
        def __init__(self, name, transport):
            self.name, self.transport = name, transport
        def __getattr__(self, key):
            return getattr(self.transport, key)
        async def chat(self, **kwargs):
            if sum(len(v.calls) for v in ledgers.values()) >= protocol['budget']['max_calls_per_arm']:
                raise RuntimeError('arm call budget exhausted')
            if client.get_stats()['total_cost'] >= protocol['budget']['estimated_usd_per_arm']:
                raise RuntimeError('arm cost budget exhausted')
            return await ledgers[self.name].chat(**{**kwargs, 'use_cache': False})

    client.research = LoggedClient('research', research)
    client.reviewer.client_impl = LoggedClient('review', reviewer)
    events = []
    async def record(role, kind, payload):
        events.append(dict(role=role, kind=kind, payload=payload))
        write_json(directory / 'events.json', events)
    start = time.monotonic()
    result = dict(side=side, status='started', orders=0, execution_intents=0,
                  historical_predictions_written=0, investment_return_improvement=None)
    db = None
    try:
        from sovereign_hall.services.action_research import build_packet
        from sovereign_hall.services.financial_state import decision_packet, run_financial_research
        packet = build_packet(data['state'], data['documents'], data['universe'], objective=data['objective'])
        write_json(directory / 'packet.json', packet)
        original_handler = logging.handlers.RotatingFileHandler
        def scoped_handler(filename, *args, **kwargs):
            target = directory / 'data/logs' / Path(filename).name
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
        from sovereign_hall.core import Document
        cutoff = datetime.fromisoformat(data['state']['cutoff'])
        class FrozenDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cutoff.astimezone(tz) if tz else cutoff.replace(tzinfo=None)
        rd.datetime = FrozenDatetime
        async def no_historical_prediction(self, **kwargs):
            await record('forecast_boundary', 'not_a_historical_prediction', kwargs)
            return None
        DecisionRecorder.record_decision = no_historical_prediction
        db = DatabaseService(str(directory / 'data/sovereign_hall.db'))
        await db._init_db()
        coordinator = ResearchRoundCoordinator(db)
        state = await coordinator.start(base_topic=data['objective'], research_objective=protocol['experiment_id'],
            input_hash=protocol['input_sha256'])
        docs = [Document.from_dict(d) for d in data['documents']]
        await db.add_documents_batch(docs, round_id=state.id)
        lessons = '【共同账户、受控研究报价与可回链估值计算】\n' + json.dumps(data['state'], ensure_ascii=False)
        async def pipeline():
            if side == 'control':
                proposals = await rd.stage2_deep_research(client, docs, data['objective'], db_service=db,
                    lessons_prompt=lessons, round_id=state.id)
            else:
                typed = decision_packet(data)
                write_json(directory / 'typed_packet.json', typed)
                output = await run_financial_research(client, typed, record=record)
                result['candidate_research'] = output
                result.update(status='completed', decisions=[output], canonical_committee_ran=False,
                              redesigned_path_ran=True, round_id=state.id)
                return
            result['proposals'] = proposals
            write_json(directory / 'proposals.json', proposals)
            scoped = [p for p in proposals if p['ticker'] in data['universe']]
            result['outside_universe'] = [p['ticker'] for p in proposals if p['ticker'] not in data['universe']]
            for proposal in scoped:
                await db.add_proposal(proposal, round_id=state.id)
            async def callback(decision):
                await coordinator.record_event(state.id, 'IsolatedCommitteeDecision', decision)
            discussion, decisions = await rd.stage3_ic_discussion(client, None, scoped, data['objective'],
                lessons_prompt=lessons, round_id=state.id, decision_callback=callback,
                committee_store=CommitteeMeetingStore(db))
            (directory / 'discussion.txt').write_text(discussion)
            result.update(status='completed', decisions=decisions, canonical_committee_ran=bool(decisions),
                          round_id=state.id)
            conn = await db._get_connection()
            async with conn.execute('SELECT COUNT(*) FROM execution_intents') as cursor:
                result['execution_intents'] = (await cursor.fetchone())[0]
            if result['execution_intents']:
                raise RuntimeError('nonexecuting experiment created intent')
        await asyncio.wait_for(pipeline(), protocol['budget']['wall_seconds_per_arm'])
    except BaseException as exc:
        result.update(status='failed', error_type=type(exc).__name__, error=str(exc)[:300])
        if isinstance(exc, asyncio.CancelledError):
            raise
    finally:
        if db:
            await db.close()
        await client.close()
        attempts = [a for ledger in ledgers.values() for call in ledger.calls for a in call['attempts']]
        complete = all(a.get('status') == 'returned' and isinstance(a.get('usage'), dict) for a in attempts)
        totals = {k:sum(a['usage'][k] for a in attempts) if complete else None
                  for k in ('prompt_tokens','completion_tokens')}
        stats = client.get_stats()
        result.update(accounting=dict(outer_calls=sum(len(v.calls) for v in ledgers.values()),
            transport_attempts=len(attempts), provider_usage_complete=complete, provider_usage=totals,
            routes={name:dict(calls=len(ledger.calls), model=ledger.client.model) for name,ledger in ledgers.items()},
            aggregate_stats=stats, configured_cost_estimate_usd=stats['total_cost'], billed_cost_usd=None,
            usage_matches_client=complete and all(totals[k] == stats[k] for k in totals)),
            elapsed_seconds=time.monotonic()-start, finished_at=datetime.now().astimezone().isoformat())
        write_json(directory / 'result.json', result)
        print(json.dumps({k:result[k] for k in ('side','status','elapsed_seconds')}), flush=True)


if __name__ == '__main__':
    run, side = Path(sys.argv[1]).resolve(), sys.argv[2]
    if side not in {'control','candidate'}:
        raise ValueError('invalid arm')
    asyncio.run(main(run, side))
