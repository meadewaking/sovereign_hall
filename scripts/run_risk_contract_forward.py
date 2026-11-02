"""Observe new production decisions in separate durable risk/contract books.

Main database is opened read-only. Existing forward experiment selections are
not touched. Restart the same output to retain the registered window and books.
"""
import argparse
import asyncio
from datetime import datetime
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT.parent))


async def run(args):
    from sovereign_hall.core.config import get_config
    from sovereign_hall.services.database import DatabaseService
    from sovereign_hall.services.market_data import MarketDataService
    from sovereign_hall.services.risk_budget_forward import RiskBudgetForwardEvaluation
    args.output.mkdir(parents=True, exist_ok=True)
    import hashlib
    import json
    from sovereign_hall.services.policy_snapshot_service import runtime_code_artifacts
    manifest = runtime_code_artifacts(ROOT, 'run_discussion.py')
    pinned = args.output/'owner_manifest.json'
    if pinned.exists() and json.loads(pinned.read_text()) != manifest:
        raise RuntimeError('source changed: retain this window; use its frozen source to resume')
    if not pinned.exists():
        pinned.write_text(json.dumps(manifest, sort_keys=True))
    # A process lock protects all private books across independent invocations.
    import fcntl
    lock = (args.output/'owner.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    db = DatabaseService(str(args.output/'metadata.db'))
    market = MarketDataService()
    engine = None
    try:
        await db._init_db()
        engine = RiskBudgetForwardEvaluation(db, market, root=args.output,
                    policy=get_config().get('simulation', {}))
        await engine.initialize()
        while True:
            with sqlite3.connect(args.db.resolve().as_uri()+'?mode=ro', uri=True) as source:
                source.row_factory = sqlite3.Row
                rounds = [dict(r) for r in source.execute('''SELECT DISTINCT r.* FROM research_rounds r
                    JOIN simulation_committee_outcomes c ON c.round_id=r.id
                    WHERE datetime(c.created_at)>=datetime(?)
                    AND r.completed_at IS NOT NULL ORDER BY r.started_at''', (engine.started_at,))]
                packets = []
                for r in rounds:
                    committee = [dict(c) for c in source.execute(
                        'SELECT * FROM simulation_committee_outcomes WHERE round_id=? ORDER BY id',(r['id'],))]
                    from sovereign_hall.services.paired_allocation import instant
                    if any(instant(c['created_at']) < instant(engine.started_at) for c in committee):
                        continue
                    policies = [dict(p) for p in source.execute(
                        'SELECT * FROM policy_snapshots WHERE id=?',(r['policy_snapshot_id'],))]
                    decisions = []
                    for c in committee:
                        decision = dict(ticker=c['ticker'], direction=c['direction'], confidence=c['confidence'],
                            target_position=c['target_position'], decision_id=c['decision_id'],
                            vote_quorum_met=bool(c['quorum_met']))
                        prediction = source.execute('SELECT expected_days FROM price_predictions WHERE decision_id=? AND round_id=?',
                                                    (c['decision_id'],r['id'])).fetchone()
                        if prediction:
                            decision['expected_days'] = prediction[0]
                        decisions.append(decision)
                    packets.append((r,committee,policies,decisions))
            for r, committee, policies, decisions in packets:
                await engine.capture_round(r['id'],decisions,producer_records=dict(
                    round=r,committee=committee,policies=policies,captured_at=datetime.now().astimezone().isoformat()))
            await engine.tick()
            print(engine.experiment_id, engine.last_status.get('evaluation_status'), flush=True)
            if args.once:
                break
            await asyncio.sleep(60)
    finally:
        if engine:
            await engine.close()
        await db.close()
        await market.close()
        lock.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db',type=Path,default=ROOT/'data/sovereign_hall.db')
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--once',action='store_true')
    asyncio.run(run(parser.parse_args()))
