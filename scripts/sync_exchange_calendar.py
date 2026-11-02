"""Persist a validated annual session certificate; no quotes or account actions."""
from pathlib import Path
import argparse
from datetime import date
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT.parent))
from sovereign_hall.services.exchange_calendar import load_exchange_calendar
from sovereign_hall.scripts.backfill_daily_prices import persist_trading_calendar


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--year', type=int, required=True)
    parser.add_argument('--db', type=Path, default=ROOT / 'data/sovereign_hall.db')
    args = parser.parse_args()
    calendar = load_exchange_calendar(args.year)
    rows = calendar.rows(date(args.year, 1, 1), date(args.year, 12, 31))
    written = persist_trading_calendar(args.db, rows)
    print(json.dumps({'year': args.year, 'verified_days': len(rows),
                      'open_sessions': sum(r['is_open'] for r in rows),
                      'changed_rows': written, 'source_identity': calendar.source_identity}))


if __name__ == '__main__':
    main()
