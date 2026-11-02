"""Capture, freeze and replay the opt-in market-state HS candidate.

No orders, prediction writes, production code switches or model calls. The
initial evidence endpoint is an integer-lot research plan, not a live fill.
Future genuine model/forward comparisons require their own frozen protocol.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_bytes())


def validate_receipts(receipts):
    from sovereign_hall.services.market_state_policy import UNIVERSE
    if (not isinstance(receipts, list) or len(receipts) != len(UNIVERSE)
            or {r.get('ticker') for r in receipts} != set(UNIVERSE)):
        raise ValueError('exact fixed universe capture required')


async def capture(output, *, existing_tape=None):
    """Actual read-only snapshot; existing originals are never recaptured/re-aged."""
    sys.path.insert(0, str(ROOT.parent))
    import httpx
    from sovereign_hall.services.exchange_calendar import load_exchange_calendar
    from sovereign_hall.services.market_data import MarketDataService
    from sovereign_hall.core.config import get_config
    from sovereign_hall.services.market_state_policy import UNIVERSE, completed_sessions
    from sovereign_hall.services.experiment_runtime import write_json
    output = output.resolve()
    # Explicit new artifact directory, never a production database path.
    output.mkdir(parents=True, exist_ok=False)
    raw = output / 'raw'
    raw.mkdir()
    zone = ZoneInfo('Asia/Shanghai')
    start = datetime.now(zone)
    cal = load_exchange_calendar(start.year)
    last = completed_sessions(cal.sessions, start.isoformat())[-1]
    first = (start.date() - timedelta(days=200)).isoformat()
    receipts = []
    if existing_tape is not None:
        previous = read_json(existing_tape / 'tape_acquisition.json')
        if previous['latest_completed_session'] != last or previous['calendar_identity'] != cal.source_identity:
            raise ValueError('existing capture calendar mismatch')
        receipts = previous['receipts']
        validate_receipts(receipts)
        for receipt in receipts:
            source = existing_tape / 'raw_tape' / (receipt['ticker'] + '.json')
            if sha(source) != receipt['sha256']:
                raise ValueError('existing original changed')
            shutil.copyfile(source, raw / (receipt['ticker'] + '.json'))
    else:
        async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
            for ticker in UNIVERSE:
                symbol = ('sh' if ticker.startswith('5') else 'sz') + ticker
                params = {'param': f'{symbol},day,{first},{last},200,qfq'}
                requested = datetime.now(zone).isoformat()
                response = await client.get('https://web.ifzq.gtimg.cn/appstock/app/fqkline/get', params=params)
                path = raw / (ticker + '.json')
                path.write_bytes(response.content)
                receipts.append(dict(ticker=ticker, url=str(response.url), status=response.status_code,
                    requested_at=requested, captured_at=datetime.now(zone).isoformat(), sha256=sha(path)))
                write_json(output / 'history_requests.json', receipts)
    validate_receipts(receipts)
    receipts = [{k: v for k, v in r.items() if k != 'path'} for r in receipts]
    dbpath = ROOT / 'data' / 'sovereign_hall.db'
    c = sqlite3.connect(dbpath.as_uri() + '?mode=ro', uri=True, timeout=5)
    c.row_factory = sqlite3.Row
    market = MarketDataService(timeout=4)
    try:
        c.execute('BEGIN')
        cash = float(c.execute("SELECT value FROM system_stats WHERE key='simulation_cash'").fetchone()[0])
        positions = [dict(r) for r in c.execute('SELECT * FROM simulation_positions ORDER BY ticker')]
        financial_counts = dict(c.execute('SELECT count(*) trades, sum(fee) fees, max(traded_at) latest FROM simulation_trades').fetchone())
        financial_counts['ledger'] = c.execute('SELECT count(*) FROM simulation_ledger_entries').fetchone()[0]
        trades_today = c.execute('SELECT count(*) FROM simulation_trades WHERE date(traded_at)=?', (start.date().isoformat(),)).fetchone()[0]
        # Startup snapshots are linked to actual rounds. The compatibility
        # active flag is not authoritative and may legitimately be zero.
        row = c.execute('''SELECT p.id,p.content_hash,p.effective_at
            FROM research_rounds r JOIN policy_snapshots p ON p.id=r.policy_snapshot_id
            ORDER BY r.started_at DESC LIMIT 1''').fetchone()
        policy_snapshot = dict(row) if row is not None else None
        tickers = sorted(set(UNIVERSE) | {p['ticker'] for p in positions})
        quotes = dict(zip(tickers, await asyncio.gather(*(market.get_current_quote(t) for t in tickers))))
        cutoff = datetime.now(zone).isoformat()
        state = dict(cutoff=cutoff, cash=cash, positions=positions, quotes=quotes,
            policy=get_config().get('simulation', {}), trades_today=trades_today,
            latest_completed_session=last)
        write_json(output / 'input.json', dict(state=state, receipts=receipts,
            calendar=dict(year=cal.year, source_identity=cal.source_identity, sessions=cal.sessions),
            observed_financial_counts=financial_counts, control_policy_snapshot=policy_snapshot,
            quote_attempts=market.last_quote_attempts, historical_returns_verified=False))
        write_json(output / 'capture_receipt.json', dict(cutoff=cutoff, production_writes=0,
            reused_history=existing_tape is not None, new_history_http_requests=0 if existing_tape else 3,
            history_capture_times={r['ticker']: r['captured_at'] for r in receipts}))
        print(json.dumps(dict(capture=str(output), cutoff=cutoff, production_writes=0)))
    finally:
        await market.close()
        c.close()


def evaluate(run, *, replay=False):
    protocol = read_json(run / 'protocol.json')
    for relative, expected in protocol['code_sha256'].items():
        if sha(run / 'runtime' / 'sovereign_hall' / relative) != expected:
            raise ValueError('frozen code drift: ' + relative)
    for relative, expected in protocol['input_sha256'].items():
        if sha(run / relative) != expected:
            raise ValueError('frozen input drift: ' + relative)
    if sha(Path(__file__)) != protocol['worker_sha256']:
        raise ValueError('frozen worker drift')
    sys.path.insert(0, str(run / 'runtime'))
    from sovereign_hall.services.market_state_policy import parse_tape, build_plan, diagnostic_prefixes
    from sovereign_hall.services.exchange_calendar import load_exchange_calendar
    from sovereign_hall.services.experiment_runtime import write_json
    data = read_json(run / 'input.json')
    validate_receipts(data['receipts'])
    state = data['state']
    cal = load_exchange_calendar(data['calendar']['year'])
    if cal.sessions != data['calendar']['sessions'] or cal.source_identity != data['calendar']['source_identity']:
        raise ValueError('frozen calendar certificate mismatch')
    tapes = {r['ticker']: parse_tape((run / 'raw' / (r['ticker'] + '.json')).read_bytes(), r,
        ticker=r['ticker'], cutoff=state['cutoff'], sessions=cal.sessions) for r in data['receipts']}
    plan = build_plan(tapes, state)
    prefixes = diagnostic_prefixes(tapes)
    if replay:
        if plan != read_json(run / 'candidate_plan.json') or prefixes != read_json(run / 'prefix_diagnostics.json'):
            raise ValueError('frozen result mismatch')
        write_json(run / 'replay_result.json', dict(status='matched', model_calls=0,
            production_writes=0, output_sha256=plan['output_sha256']))
    else:
        if (run / 'candidate_plan.json').exists():
            raise FileExistsError('candidate output already exists; use replay')
        write_json(run / 'parsed_tapes.json', tapes)
        write_json(run / 'candidate_plan.json', plan)
        write_json(run / 'prefix_diagnostics.json', prefixes)
    print(json.dumps(dict(status='matched' if replay else 'completed',
        actions=[(a['ticker'], a['action'], a['reason']) for a in plan['actions']],
        model_quality_test_eligible=plan['model_quality_test_eligible'],
        diagnostic_entry_sessions=prefixes['entry_branch_sessions'],
        diagnostic_hold_sessions=prefixes['abstention_branch_sessions'],
        actual_model_calls=0, return_improvement=None)))


def freeze(capture_path, output):
    sys.path.insert(0, str(ROOT.parent))
    from sovereign_hall.services.experiment_runtime import snapshot_code, write_json
    from sovereign_hall.services.market_state_policy import VERSION
    output.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(capture_path / 'input.json', output / 'input.json')
    shutil.copytree(capture_path / 'raw', output / 'raw')
    shutil.copyfile(__file__, output / 'worker.py')
    manifest = snapshot_code(ROOT, output / 'runtime' / 'sovereign_hall')
    for source in (ROOT / 'services' / 'calendar_data').glob('*'):
        if source.is_file():
            target = output / 'runtime' / 'sovereign_hall' / 'services' / 'calendar_data' / source.name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
            manifest[str(source.relative_to(ROOT))] = sha(target)
    data = read_json(output / 'input.json')
    inputs = {str(p.relative_to(output)): sha(p) for p in [output / 'input.json', *sorted((output / 'raw').glob('*.json'))]}
    write_json(output / 'protocol.json', dict(experiment_id='market_state_policy_20261016_v1',
        candidate_version=VERSION, frozen_at=datetime.now().astimezone().isoformat(),
        cutoff=data['state']['cutoff'], code_sha256=manifest, input_sha256=inputs,
        worker_sha256=sha(output / 'worker.py'), control=data['control_policy_snapshot'],
        comparator_status='not_run; evaluate input discrimination before any genuine model comparison',
        endpoint='observed market state -> research lots/HOLD; no actual approval or execution',
        historical_return=None, return_comparator_faithful=False,
        promotion='isolated_development_candidate', production_writes=0, model_calls=0))
    subprocess.run([sys.executable, '-B', str(output / 'worker.py'), '--worker', str(output)], check=True)


def main():
    if len(sys.argv) in (3, 4) and sys.argv[1] == '--worker':
        evaluate(Path(sys.argv[2]).resolve(), replay=len(sys.argv) == 4 and sys.argv[3] == '--replay')
        return
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    capture_args = sub.add_parser('capture')
    capture_args.add_argument('--output', type=Path, required=True)
    capture_args.add_argument('--existing-tape', type=Path)
    freeze_args = sub.add_parser('freeze')
    freeze_args.add_argument('--capture', type=Path, required=True)
    freeze_args.add_argument('--output', type=Path, required=True)
    replay_args = sub.add_parser('replay')
    replay_args.add_argument('--experiment', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'capture':
        asyncio.run(capture(args.output, existing_tape=args.existing_tape))
    elif args.command == 'freeze':
        freeze(args.capture.resolve(), args.output.resolve())
    else:
        run = args.experiment.resolve()
        subprocess.run([sys.executable, '-B', str(run / 'worker.py'), '--worker', str(run), '--replay'], check=True)


if __name__ == '__main__':
    main()
