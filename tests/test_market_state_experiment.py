"""Read-only capture and frozen CLI boundaries; not investment experiments."""
from datetime import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

import pytest


def cli():
    path = Path(__file__).resolve().parents[1] / 'scripts/run_market_state_experiment.py'
    spec = importlib.util.spec_from_file_location('market_state_cli', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def prepare_tape(path):
    from sovereign_hall.services.exchange_calendar import load_exchange_calendar
    from sovereign_hall.services.market_state_policy import UNIVERSE, completed_sessions
    now = datetime.now(ZoneInfo('Asia/Shanghai'))
    cal = load_exchange_calendar(now.year)
    path.mkdir()
    (path / 'raw_tape').mkdir()
    receipts = []
    for ticker in UNIVERSE:
        raw = path / 'raw_tape' / (ticker + '.json')
        raw.write_text('{}')  # Capture preserves originals; evaluator must reject this stub.
        receipts.append(dict(ticker=ticker, sha256=hashlib.sha256(raw.read_bytes()).hexdigest(),
            captured_at=now.isoformat()))
    (path / 'tape_acquisition.json').write_text(json.dumps(dict(receipts=receipts,
        latest_completed_session=completed_sessions(cal.sessions, now.isoformat())[-1],
        calendar_identity=cal.source_identity)))


@pytest.mark.asyncio
async def test_actual_capture_path_reads_database_without_mutation_or_active_flag(tmp_path, monkeypatch):
    from sovereign_hall.services import market_data
    from sovereign_hall.core import config
    module = cli()
    root = tmp_path / 'project'
    (root / 'data').mkdir(parents=True)
    db = root / 'data' / 'sovereign_hall.db'
    c = sqlite3.connect(db)
    c.executescript('''CREATE TABLE system_stats(key TEXT, value TEXT);
        INSERT INTO system_stats VALUES('simulation_cash','10000');
        CREATE TABLE simulation_positions(ticker TEXT,shares INTEGER,avg_cost REAL);
        CREATE TABLE simulation_trades(fee REAL,traded_at TEXT);
        CREATE TABLE simulation_ledger_entries(id TEXT);
        CREATE TABLE policy_snapshots(id TEXT, content_hash TEXT,effective_at TEXT,active INTEGER);
        INSERT INTO policy_snapshots VALUES('actual-startup','hash','2026-10-15',0);
        CREATE TABLE research_rounds(policy_snapshot_id TEXT,started_at TEXT);
        INSERT INTO research_rounds VALUES('actual-startup','2026-10-16');''')
    c.commit()
    c.close()
    before = db.read_bytes()
    source = tmp_path / 'original'
    prepare_tape(source)
    closed = []
    class ReadOnlyMarket:
        last_quote_attempts = {}
        def __init__(self, **kwargs):
            pass
        async def get_current_quote(self, ticker):
            return None
        async def close(self):
            closed.append(True)
    class Config:
        def get(self, *args):
            return dict(execution_mode='simulation')
    monkeypatch.setattr(module, 'ROOT', root)
    monkeypatch.setattr(market_data, 'MarketDataService', ReadOnlyMarket)
    monkeypatch.setattr(config, 'get_config', lambda: Config())
    output = tmp_path / 'capture'
    await module.capture(output, existing_tape=source)
    data = json.loads((output / 'input.json').read_text())
    assert data['control_policy_snapshot']['id'] == 'actual-startup'
    assert data['state']['cash'] == 10000 and closed == [True]
    assert all(q is None for q in data['state']['quotes'].values())
    assert db.read_bytes() == before
    with pytest.raises(FileExistsError):
        await module.capture(output, existing_tape=source)
    assert closed == [True]  # no quote lookup or other side effect on reuse


@pytest.mark.asyncio
async def test_path_like_ticker_rejected_before_copy_or_market_access(tmp_path, monkeypatch):
    module = cli()
    source = tmp_path / 'original'
    prepare_tape(source)
    p = source / 'tape_acquisition.json'
    data = json.loads(p.read_text())
    data['receipts'][0]['ticker'] = '../escape'
    p.write_text(json.dumps(data))
    with pytest.raises(ValueError, match='fixed universe'):
        await module.capture(tmp_path / 'output', existing_tape=source)
    assert not (tmp_path / 'output' / 'escape.json').exists()
    assert not (tmp_path / 'output' / 'input.json').exists()


def test_frozen_worker_checks_hashes_before_any_input_evaluation(tmp_path):
    module = cli()
    run = tmp_path / 'experiment'
    run.mkdir()
    (run / 'protocol.json').write_text(json.dumps(dict(code_sha256={},
        input_sha256={}, worker_sha256='tampered')))
    with pytest.raises(ValueError, match='worker drift'):
        module.evaluate(run)
    assert list(run.iterdir()) == [run / 'protocol.json']
