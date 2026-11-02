"""Acquire current issuer evidence and freeze input for native research comparison."""
import argparse
import asyncio
from datetime import datetime
import json
from pathlib import Path
import sys
import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT.parent))


async def main(args):
    from sovereign_hall.core.config import get_config
    from sovereign_hall.services.issuer_acquisition import acquire_issuer
    from sovereign_hall.services.market_data import MarketDataService
    args.output.mkdir(parents=True,exist_ok=False)
    async with httpx.AsyncClient() as client:
        docs,audit = await acquire_issuer(args.ticker,client)
    market = MarketDataService()
    try:
        quote = await market.get_current_quote(args.ticker)
    finally:
        await market.close()
    (args.output/'acquisition.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2))
    cutoff = datetime.now().astimezone().isoformat()
    state = dict(cutoff=cutoff,cash=10000,total_assets=10000,trades_today=0,positions=[],
                 policy=get_config().get('simulation',{}),quotes={args.ticker:quote})
    documents = [d.to_dict() for d in docs]
    packet = dict(state=state,documents=documents,universe=[args.ticker],
        cases=[dict(id='issuer_current',objective=f'核验{args.ticker}当期经营与估值，选择有依据行动、继续补证或HOLD')],
        acceptance=dict(goal='actual proposal and decision pipeline',profit_proven=False,
                        limitation='missing facts may imply HOLD; no forced approvals'))
    (args.output/'input.json').write_text(json.dumps(packet,ensure_ascii=False,indent=2))
    print(json.dumps(dict(status=audit['status'],documents=len(documents),quote_available=bool(quote)),ensure_ascii=False))


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--ticker',required=True)
    p.add_argument('--output',type=Path,required=True)
    asyncio.run(main(p.parse_args()))
