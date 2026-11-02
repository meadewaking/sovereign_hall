from datetime import datetime, timezone
import copy
import pytest
from sovereign_hall.services import issuer_filings as f
from sovereign_hall.services.evidence_time import document_time_audit

NOW=datetime(2026,10,1,6,tzinfo=timezone.utc)

def payload():
    return {'totalAnnouncement':1,'announcements':[{'secCode':'000001','secName':'平安银行','announcementTitle':'2026年半年度报告摘要','announcementTime':1786723200000,'announcementId':'1225475343','adjunctUrl':'finalpage/2026-08-15/1225475343.PDF','adjunctType':'PDF'}]}

def select(p=None):
    return f.select_summary(p or payload(),ticker='000001',captured_at=NOW,cutoff=NOW)


def test_source_index_org_id_and_identity():
    _,p=f.index_request('000001',cutoff=NOW,org_id='gssz0000001')
    assert p['stock']=='000001,gssz0000001'
    assert select()['published_at']=='2026-08-15'
    with pytest.raises(ValueError): f.index_request('000001,evil',cutoff=NOW)
    with pytest.raises(ValueError): f.index_request('000001',cutoff=NOW,org_id='../evil')


@pytest.mark.parametrize('patch',[
    {'secCode':'600015'}, {'adjunctUrl':'https://evil.test/f.pdf'},
    {'adjunctUrl':'finalpage/2026-08-14/1225475343.PDF'},
    {'announcementId':'999'}, {'announcementTime':None},
])
def test_index_rejects_bad_lineage(patch):
    p=payload();p['announcements'][0].update(patch)
    with pytest.raises(ValueError):select(p)


def test_partial_index_duplicate_and_future_do_not_silently_choose():
    p=payload();p['totalAnnouncement']=2
    with pytest.raises(ValueError,match='incomplete'):select(p)
    p['announcements'].append(copy.deepcopy(p['announcements'][0]))
    with pytest.raises(ValueError,match='ambiguous'):select(p)
    with pytest.raises(ValueError,match='capture'):
        f.select_summary(payload(),ticker='000001',captured_at=NOW,cutoff=NOW.replace(day=1,month=9))
    p=payload();p['announcements'][0]['announcementTime']=int(NOW.timestamp()*1000)
    with pytest.raises(ValueError,match='no eligible'):select(p)


def test_whole_page_lineage_single_source_and_no_claim_promotion(monkeypatch):
    pages=['平安银行 000001 2026 年半年度报告摘要\n单位：百万元\n净利润 25696 24870','拨备覆盖率 219.58% 220.88%\n注：本行口径']
    monkeypatch.setattr(f,'extract_pages',lambda _:pages)
    docs,a=f.build_filing_documents(b'%PDF-fixture',filing=select(),captured_at=NOW,cutoff=NOW)
    assert a['independent_sources']==1 and not a['verified_numeric_facts']
    assert all(d.content.endswith(p) for d,p in zip(docs,pages))
    assert docs[1].url.endswith('#page=2')
    assert len({d.metadata['shared_source_group_id'] for d in docs})==1
    assert all(document_time_audit(d)['publication_time_known'] for d in docs)
    assert all('facts' not in d.metadata for d in docs)


def test_mismatched_pdf_identity_and_untrusted_url(monkeypatch):
    monkeypatch.setattr(f,'extract_pages',lambda _:['600015 华夏银行 2026年半年度报告摘要'])
    with pytest.raises(ValueError,match='does not match'):
        f.build_filing_documents(b'%PDF-fixture',filing=select(),captured_at=NOW,cutoff=NOW)
    bad=select();bad['url']='https://evil.test/a.pdf'
    with pytest.raises(ValueError,match='origin'):
        f.build_filing_documents(b'%PDF-fixture',filing=bad,captured_at=NOW,cutoff=NOW)


def test_invalid_pdf_transport_and_naive_or_late_capture():
    with pytest.raises(ValueError):f.extract_pages(b'<html>blocked</html>')
    with pytest.raises(ValueError):f.extract_pages(b'%PDF-'+b'0'*f.MAX_BYTES)
    for captured,cutoff in [(NOW,NOW.replace(tzinfo=None)),(NOW.replace(day=2),NOW)]:
        with pytest.raises(ValueError,match='capture'):
            f.build_filing_documents(b'',filing=select(),captured_at=captured,cutoff=cutoff)


@pytest.mark.asyncio
async def test_bundle_survives_actual_database_and_context(tmp_path,monkeypatch):
    from sovereign_hall.services.database import DatabaseService
    from sovereign_hall.core import Document
    from sovereign_hall.run_discussion import build_evidence_context
    texts=['平安银行 000001 2026年半年度报告摘要 单位：百万元 净利润 25696',
           '拨备覆盖率 219.58% 220.88% 注：本行口径']
    monkeypatch.setattr(f,'extract_pages',lambda _:texts)
    pages,_=f.build_filing_documents(b'%PDF-fixture',filing=select(),captured_at=NOW,cutoff=NOW)
    bundle=f.bundle_filing_documents(pages)
    with pytest.raises(ValueError):f.bundle_filing_documents(pages[::-1])
    db=DatabaseService(str(tmp_path/'isolated.db'))
    try:
        await db._init_db();await db.add_document(bundle)
        conn=await db._get_connection()
        async with conn.execute('select * from documents') as cur:
            stored=[Document.from_dict(dict(row)) for row in await cur.fetchall()]
        context,audit=build_evidence_context(stored,max_docs=30,doc_chars=16000,context_chars=60000)
        assert len(stored)==1
        assert all(' '.join(t.split()) in context for t in texts)
        assert 'PDF物理页=1/2' in context and 'PDF物理页=2/2' in context
    finally:await db.close()
