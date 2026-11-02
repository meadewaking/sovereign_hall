"""Bounded issuer-first acquisition, retaining provider receipts and gaps."""
from datetime import datetime
from zoneinfo import ZoneInfo
from .issuer_dossier import income_request, build_income_dossier
from ..domain.portfolio.instruments import is_etf_ticker


async def acquire_issuer(ticker, client):
    """Fetch only explicit ticker disclosures; ETF facts are never synthesized."""
    if is_etf_ticker(ticker):
        return await acquire_fund(ticker, client)
    url, params = income_request(ticker)
    try:
        response = await client.get(url, params=params, timeout=20)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        return [], dict(ticker=ticker, status='provider_failure', error_type=type(exc).__name__)
    captured = datetime.now(ZoneInfo('Asia/Shanghai'))
    docs, audit = build_income_dossier(payload, ticker=ticker, captured_at=captured, cutoff=captured)
    audit['receipt'] = dict(url=url, params=params, captured_at=captured.isoformat(), payload=payload)
    return docs, audit


async def acquire_fund(ticker, client):
    """Fetch fund disclosure page; keep missing structured facts explicit.

    Capture time is not publication time. This document can guide follow-up
    research but never establishes current liquidity, valuation or holdings.
    """
    from ..core import Document
    import hashlib
    from bs4 import BeautifulSoup
    url = f'https://fundf10.eastmoney.com/jbgk_{ticker}.html'
    captured = datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()
    try:
        response = await client.get(url, timeout=20)
        response.raise_for_status()
        html = response.text
        text = BeautifulSoup(html, 'html.parser').get_text(' ', strip=True)
        if ticker not in text or '基金' not in text:
            raise ValueError('fund identity absent from response')
    except Exception as exc:
        return [], dict(ticker=ticker, status='provider_failure', error_type=type(exc).__name__)
    digest = hashlib.sha256(html.encode()).hexdigest()
    doc = Document(doc_id=f'fund_profile_{ticker}_{digest[:16]}',
        title=f'{ticker} 基金资料（发布时间未核验）', url=url,
        source='eastmoney_fund_profile', content=text[:16000],
        metadata=dict(crawled_at=captured, publication_unverified=True,
                      source_kind='secondary_distributor', ticker=ticker))
    return [doc], dict(ticker=ticker, status='captured_unverified_publication',
        facts=[], missing=['verified_publication_time','current_exposure','liquidity','valuation'],
        receipt=dict(url=url,captured_at=captured,sha256=digest,payload=html))


async def supplement_proposals(proposals, db, round_id):
    """Bounded production supplement; persist sources before committee use."""
    import httpx
    import json
    supplements = []
    async with httpx.AsyncClient() as client:
        for proposal in proposals[:2]:
            docs, audit = await acquire_issuer(proposal['ticker'], client)
            if docs:
                await db.add_documents_batch(docs, round_id=round_id)
            supplements.append(dict(ticker=proposal['ticker'], status=audit['status'],
                facts=audit.get('facts', []), missing=audit.get('missing', []),
                source_ids=[d.doc_id for d in docs],
                caveat='secondary disclosure data; not current valuation or investment approval'))
    return '【标的披露定向补证】\n' + json.dumps(supplements, ensure_ascii=False)
