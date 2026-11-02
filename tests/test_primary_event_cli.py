"""Acquisition boundaries only; HTTP fixtures do not test investment quality."""
from datetime import datetime
import importlib.util
import json
from pathlib import Path

import httpx
import pytest


def load_cli():
    path = Path(__file__).resolve().parents[1] / 'scripts/acquire_primary_events.py'
    spec = importlib.util.spec_from_file_location('event_acquisition_cli', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
async def test_invalid_budget_cannot_create_output_or_open_network(tmp_path, monkeypatch):
    cli = load_cli()
    monkeypatch.setattr(cli.httpx, 'AsyncClient', lambda **kw: pytest.fail('network opened'))
    output = tmp_path / 'events'
    with pytest.raises(ValueError):
        await cli.acquire(output, limit=11)
    assert not output.exists()


@pytest.mark.asyncio
async def test_failed_original_is_retained_and_no_issuer_substitution(tmp_path, monkeypatch):
    cli = load_cli()
    request_count = 0
    from sovereign_hall.services import primary_event_research as module
    from datetime import timedelta
    yesterday = (datetime.now(module.ZONE) - timedelta(days=1)).date().isoformat()
    def row(ticker, aid):
        return dict(secCode=ticker, secName='测试股份',
            announcementTitle=f'{datetime.now(module.ZONE).year}年前三季度业绩预告',
            announcementTime=datetime.fromisoformat(yesterday).replace(tzinfo=module.ZONE).timestamp()*1000,
            adjunctUrl=f'finalpage/{yesterday}/{aid}.PDF', announcementId=aid, adjunctType='PDF')
    def handle(request):
        nonlocal request_count
        request_count += 1
        if request.method == 'POST':
            if b'column=szse' in request.content:
                return httpx.Response(200, json=dict(announcements=[row('300434','2'),row('301282','1')],totalAnnouncement=2))
            return httpx.Response(200, json=dict(announcements=[],totalAnnouncement=0))
        return httpx.Response(200, content=b'not a PDF; never a valid original')
    real_client = httpx.AsyncClient
    monkeypatch.setattr(cli.httpx, 'AsyncClient', lambda **kw: real_client(transport=httpx.MockTransport(handle), **kw))
    output = tmp_path / 'events'
    receipt = await cli.acquire(output, limit=1)
    assert request_count == 3 and len(receipt['requests']) == 3
    assert receipt['status'] == 'source_incomplete'
    assert receipt['failures'][0]['ticker'] == '300434'
    assert json.loads((output/'events.json').read_text())[0]['ticker'] == '300434'
    assert json.loads((output/'documents.json').read_text()) == []
    assert receipt['orders'] == receipt['production_writes'] == 0
    with pytest.raises(FileExistsError):
        await cli.acquire(output, limit=1)
    assert request_count == 3
