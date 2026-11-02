from datetime import datetime, timezone
import copy

import pytest

from sovereign_hall.services.issuer_dossier import build_income_dossier, income_request
from sovereign_hall.services.evidence_time import document_time_audit
from sovereign_hall.services.database import DatabaseService
from sovereign_hall.core import Document

NOW = datetime(2026, 9, 29, 6, tzinfo=timezone.utc)


def payload():
    return {"success": True, "result": {"data": [
        {"SECURITY_CODE": "000001", "SECURITY_NAME_ABBR": "测试银行",
         "REPORT_DATE": "2026-06-30 00:00:00", "NOTICE_DATE": "2026-08-20 00:00:00",
         "PARENT_NETPROFIT": 80, "TOTAL_OPERATE_INCOME": 200, "INTEREST_NI": None},
        {"SECURITY_CODE": "000001", "REPORT_DATE": "2025-06-30 00:00:00",
         "NOTICE_DATE": "2025-08-20 00:00:00", "PARENT_NETPROFIT": 100},
    ]}}


def build(p):
    return build_income_dossier(p, ticker="000001", captured_at=NOW, cutoff=NOW)


def test_negative_evidence_nulls_and_same_period_comparison():
    docs, audit = build(payload())
    profit = next(x for x in audit['facts'] if x['field'] == 'PARENT_NETPROFIT')
    assert profit['derived_yoy_pct'] == -20
    assert 'INTEREST_NI' not in {x['field'] for x in audit['facts']}
    assert '不是净息差' not in docs[0].content  # no fabricated interest number
    assert '净息差' in docs[0].content and 'HOLD' in docs[0].content
    assert document_time_audit(docs[0])['publication_time_known']


@pytest.mark.parametrize('change', [
    {'NOTICE_DATE': '2026-09-29'}, {'NOTICE_DATE': '2026-10-01'},
    {'NOTICE_DATE': None}, {'REPORT_DATE': '2027-01-01'},
    {'SECURITY_CODE': '600015'},
])
def test_unavailable_or_wrong_issuer_never_supplies_latest_fact(change):
    p=payload();p['result']['data'][0].update(change)
    docs, audit=build(p)
    assert len(audit['excluded']) == 1
    assert all(x['value'] != 80 for x in audit['facts'])
    assert all(x['report_period'] == '2025-06-30' for x in audit['facts'])


def test_historical_capture_not_backdated_and_naive_cutoff_rejected():
    with pytest.raises(ValueError, match='available'):
        build_income_dossier(payload(),ticker='000001',captured_at=NOW,
                             cutoff=NOW.replace(day=28))
    with pytest.raises(ValueError, match='timezone'):
        build_income_dossier(payload(),ticker='000001',captured_at=NOW,
                             cutoff=NOW.replace(tzinfo=None))


def test_conflicting_restatement_fails_closed():
    p=payload();row=copy.deepcopy(p['result']['data'][0]);row['PARENT_NETPROFIT']=99
    p['result']['data'].append(row)
    docs,audit=build(p)
    assert docs == [] and audit['status'] == 'ambiguous_latest_report'


@pytest.mark.parametrize('value',[float('nan'),float('inf'),True,'80',None])
def test_bad_numeric_values_are_unknown(value):
    p=payload();p['result']['data'][0]['PARENT_NETPROFIT']=value
    _,audit=build(p)
    assert not any(x['field']=='PARENT_NETPROFIT' for x in audit['facts'])


def test_non_positive_base_has_no_derived_growth():
    p=payload();p['result']['data'][1]['PARENT_NETPROFIT']=-100
    _,audit=build(p)
    assert 'derived_yoy_pct' not in next(x for x in audit['facts'] if x['field']=='PARENT_NETPROFIT')


def test_provider_failure_and_security_code_injection():
    assert build({'success':False})[0] == []
    with pytest.raises(ValueError): income_request('000001") OR 1=1')
    with pytest.raises(ValueError): income_request('159273')


@pytest.mark.asyncio
async def test_source_provenance_survives_actual_database(tmp_path):
    db=DatabaseService(str(tmp_path/'isolated.db'))
    try:
        await db._init_db()
        docs,_=build(payload());await db.add_document(docs[0])
        conn=await db._get_connection()
        async with conn.execute('select * from documents') as cur:
            stored=Document.from_dict(dict(await cur.fetchone()))
        assert document_time_audit(stored)['publish_time_source']=='structured_disclosure_notice'
        assert document_time_audit(stored)['publication_time_known']
        assert stored.content == ' '.join(docs[0].content.split())
    finally:
        await db.close()
