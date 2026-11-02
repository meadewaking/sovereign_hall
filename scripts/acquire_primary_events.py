"""Acquire a bounded, outcome-independent primary disclosure agenda (opt-in).

Example: python scripts/acquire_primary_events.py --output runs/event_feed/new
Writes immutable raw responses, the observed index coverage and full PDF-page
Documents. No model, production DB, account, quote, order or broker access.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys

import httpx


async def acquire(output, *, limit=2, lookback_days=14):
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root.parent))
    from sovereign_hall.services.primary_event_research import (
        VERSION, discovery_request, select_events, event_documents)
    from sovereign_hall.services.issuer_filings import MAX_BYTES
    from sovereign_hall.services.experiment_runtime import write_json
    # Validate before creating the output directory or sending any HTTP.
    as_of = datetime.now().astimezone()
    discovery_request('szse', as_of=as_of, lookback_days=lookback_days)
    if type(limit) is not int or not 1 <= limit <= 10:
        raise ValueError('event count must be between one and ten')
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    (output / 'raw').mkdir()
    receipt = dict(version=VERSION, discovery_as_of=as_of.isoformat(),
        requested_events=limit, http_ceiling=2+limit, requests=[], failures=[],
        production_writes=0, model_calls=0, orders=0, status='started',
        universe_claim='matching first index pages only; not complete market coverage')
    scans, documents = [], []
    try:
        async with httpx.AsyncClient(timeout=20, trust_env=False, follow_redirects=False,
            headers={'User-Agent':'Mozilla/5.0', 'Referer':'https://www.cninfo.com.cn/'}) as client:
            async def fetch(method, url, filename, **kwargs):
                if len(receipt['requests']) >= receipt['http_ceiling']:
                    raise RuntimeError('HTTP budget exhausted')
                row = dict(method=method, url=url, request=kwargs.get('data'),
                           started_at=datetime.now().astimezone().isoformat())
                receipt['requests'].append(row)
                write_json(output / 'receipt.json', receipt)
                async with client.stream(method, url, **kwargs) as response:
                    response.raise_for_status()
                    if response.is_redirect:
                        raise ValueError('unverified disclosure redirect')
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        body.extend(chunk)
                        if len(body) > MAX_BYTES:
                            raise ValueError('source byte budget exceeded')
                captured = datetime.now().astimezone()
                path = output / 'raw' / filename
                path.write_bytes(body)
                row.update(captured_at=captured.isoformat(), status='returned',
                    bytes=len(body), sha256=hashlib.sha256(body).hexdigest(), raw_path=str(path))
                write_json(output / 'receipt.json', receipt)
                return bytes(body), captured

            for exchange in ('szse','sse'):
                url, params = discovery_request(exchange, as_of=as_of, lookback_days=lookback_days)
                raw, captured = await fetch('POST', url, exchange+'_index.json', data=params)
                scan = select_events(json.loads(raw), exchange=exchange, as_of=as_of,
                                     lookback_days=lookback_days, limit=limit)
                scan['captured_at'] = captured.isoformat()
                scans.append(scan)
                write_json(output / 'discovery.json', scans)
            selected = sorted([e for s in scans for e in s['events']],
                key=lambda e:(e['published_at'],int(e['announcement_id'])), reverse=True)[:limit]
            write_json(output / 'events.json', selected)
            if not selected:
                receipt['status'] = 'no_eligible_event'
            for event in selected:
                # A failed case is retained. Do not replace it after seeing its
                # content, quote, economics, forecast sign or eventual return.
                try:
                    raw, captured = await fetch('GET', event['url'],
                        event['ticker']+'_'+event['announcement_id']+'.pdf')
                    pages, source = event_documents(raw, event=event, captured_at=captured,
                                                   cutoff=datetime.now().astimezone())
                    documents.extend(d.to_dict() for d in pages)
                    write_json(output / (event['ticker']+'_source.json'), source)
                except Exception as exc:
                    receipt['failures'].append(dict(ticker=event['ticker'], error_type=type(exc).__name__, error=str(exc)))
            if selected:
                receipt['status'] = 'completed' if not receipt['failures'] else 'source_incomplete'
            receipt.update(selected_events=len(selected), completed_documents=len(documents))
    except Exception as exc:
        receipt.update(status='failed', error_type=type(exc).__name__, error=str(exc))
    finally:
        receipt['finished_at'] = datetime.now().astimezone().isoformat()
        write_json(output / 'documents.json', documents)
        write_json(output / 'receipt.json', receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--limit', type=int, default=2)
    parser.add_argument('--lookback-days', type=int, default=14)
    args = parser.parse_args()
    receipt = asyncio.run(acquire(args.output, limit=args.limit, lookback_days=args.lookback_days))
    print(json.dumps({k:receipt.get(k) for k in ('status','selected_events','completed_documents','model_calls','orders')}, ensure_ascii=False))
    if receipt['status'] in {'failed','source_incomplete'}:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
