"""Opt-in issuer-first research input. No prices, orders, or production writes.

The provider is a secondary financial-data distributor, not the issuer. Raw
responses must be retained by the caller; notice dates never become crawl dates.
This current-observation adapter must not be used to manufacture historical PIT
data: a response captured after the decision cutoff is rejected in its entirety.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import math
import re
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from ..core import Document

VERSION = "issuer_dossier_v1"
ENDPOINT = "https://datacenter-web.eastmoney.com/api/data/v1/get"
ZONE = ZoneInfo("Asia/Shanghai")
FIELDS = {
    "TOTAL_OPERATE_INCOME": "营业总收入（元，报告期累计）",
    "PARENT_NETPROFIT": "归母净利润（元，报告期累计）",
    "INTEREST_NI": "利息净收入（元，报告期累计；不是净息差）",
    "FEE_COMMISSION_NI": "手续费及佣金净收入（元，报告期累计）",
}


def income_request(ticker: str) -> tuple[str, dict]:
    if not re.fullmatch(r"(?:0|3|6)\d{5}", ticker):
        raise ValueError("Only explicitly supplied A-share issuer codes are supported")
    return ENDPOINT, {
        "reportName": "RPT_DMSK_FN_INCOME", "columns": "ALL",
        "filter": f'(SECURITY_CODE="{ticker}")', "pageSize": "8",
        "pageNumber": "1", "sortColumns": "REPORT_DATE",
        "sortTypes": "-1", "source": "WEB", "client": "WEB",
    }


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("An explicit timezone is required")
    return value.astimezone(ZONE)


def _number(value):
    # Provider nulls are missing, never zero; booleans and numeric strings are
    # rejected so schema changes cannot silently change financial semantics.
    return (value if type(value) in (int, float) and math.isfinite(value) else None)


def build_income_dossier(payload: dict, *, ticker: str, captured_at: datetime,
                         cutoff: datetime) -> tuple[list[Document], dict]:
    url, params = income_request(ticker)
    captured_at, cutoff = _aware(captured_at), _aware(cutoff)
    audit = {"version": VERSION, "ticker": ticker, "cutoff": cutoff.isoformat(),
             "captured_at": captured_at.isoformat(), "excluded": [], "facts": [],
             "source_kind": "secondary_distributor", "historical_pit": False}
    if captured_at > cutoff:
        raise ValueError("Response was not available at the decision cutoff")
    if payload.get("success") is not True:
        audit["status"] = "provider_failure"
        return [], audit
    data = (payload.get("result") or {}).get("data")
    if not isinstance(data, list):
        audit["status"] = "invalid_schema"
        return [], audit
    eligible = []
    for index, row in enumerate(data):
        reason = ""
        if not isinstance(row, dict) or row.get("SECURITY_CODE") != ticker:
            reason = "issuer_mismatch"
        else:
            try:
                period = datetime.fromisoformat(row["REPORT_DATE"]).date()
                notice = datetime.fromisoformat(row["NOTICE_DATE"]).date()
                # Date-only availability: same-day records are excluded because
                # the provider does not report an intraday publication instant.
                if not period <= notice < cutoff.date():
                    reason = "unavailable_or_invalid_date"
            except (KeyError, TypeError, ValueError):
                reason = "missing_source_date"
        if reason:
            audit["excluded"].append({"row": index, "reason": reason})
        else:
            eligible.append((period, notice, row))
    if not eligible:
        audit["status"] = "no_eligible_disclosure"
        return [], audit
    latest_period = max(x[0] for x in eligible)
    latest = [x for x in eligible if x[0] == latest_period]
    # Conflicting restatements are a gap, not an opportunity to choose a value.
    if len({json.dumps(x[2], sort_keys=True) for x in latest}) != 1:
        audit["status"] = "ambiguous_latest_report"
        return [], audit
    period, notice, row = latest[0]
    prior = [x for x in eligible if (x[0].year, x[0].month, x[0].day) ==
             (period.year - 1, period.month, period.day)]
    prior_row = prior[0][2] if len(prior) == 1 else {}
    facts = []
    for key, label in FIELDS.items():
        value, previous = _number(row.get(key)), _number(prior_row.get(key))
        if value is None:
            continue
        fact = {"field": key, "label": label, "value": value,
                "report_period": period.isoformat(), "notice_date": notice.isoformat()}
        if previous is not None:
            fact.update(prior_value=previous, prior_period=prior[0][0].isoformat(),
                        prior_notice_date=prior[0][1].isoformat())
            if previous > 0:
                fact["derived_yoy_pct"] = round((value / previous - 1) * 100, 4)
        facts.append(fact)
    audit.update(status="usable" if facts else "no_numeric_facts", facts=facts)
    if not facts:
        return [], audit
    request_url = url + "?" + urlencode(params)
    body = (f"证券代码={ticker}；来源显示名称={row.get('SECURITY_NAME_ABBR', '')}。\n"
            "来源：东方财富财务数据转录，非发行人原公告；需独立核验。\n"
            "以下为已抓取响应中的累计财报数值；同比仅由同报告期上一年正基数计算，"
            "不能把半年报累计数当单季度数据，不能由利息收入推出净息差。\n"
            + json.dumps(facts, ensure_ascii=False) + "\n"
            "决定性缺口：本接口不提供净息差、不良率、资本充足率、分红方案、"
            "当前股息率、估值或可执行报价。旧报告不能证明当前经营改善，"
            "单一转载来源不能证明独立交叉验证；缺口未解决应允许HOLD。")
    digest = hashlib.sha256(body.encode()).hexdigest()
    doc = Document(doc_id=f"{VERSION}_{ticker}_{digest[:16]}", content=body,
                   title=f"{ticker} 财务披露事实与缺口 {period}", url=request_url,
                   source="eastmoney_financial_disclosure", metadata={
                       "publish_time": notice.isoformat(),
                       "publish_time_source": "structured_disclosure_notice",
                       "crawled_at": captured_at.isoformat(),
                       "dossier_version": VERSION, "facts": facts,
                       "source_kind": "secondary_distributor"})
    return [doc], audit
