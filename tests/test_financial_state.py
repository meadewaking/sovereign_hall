"""Financial semantics and economic rejection, independent of model wording."""
import json

import pytest

from sovereign_hall.services.financial_state import normalize_financials, compile_draft


def docs():
    def source(table, rows):
        rows = [{"SECURITY_CODE":"002415", "SECURITY_NAME_ABBR":"海康威视",
                 "NOTICE_DATE":"2026-10-17", **row} for row in rows]
        return dict(id=table, content=json.dumps(dict(table=table,ticker="002415",name="海康威视",rows=rows)),
            metadata=dict(crawled_at="2026-10-30T10:00:00+08:00", source_kind="secondary_distributor"))
    return [source("income",[
        dict(REPORT_DATE="2026-09-30",TOTAL_OPERATE_INCOME=740,PARENT_NETPROFIT=129),
        dict(REPORT_DATE="2026-06-30",TOTAL_OPERATE_INCOME=468,PARENT_NETPROFIT=79),
        dict(REPORT_DATE="2025-09-30",TOTAL_OPERATE_INCOME=658,PARENT_NETPROFIT=93)]),
        source("cashflow",[
            dict(REPORT_DATE="2026-09-30",NETCASH_OPERATE=72),
            dict(REPORT_DATE="2026-06-30",NETCASH_OPERATE=32),
            dict(REPORT_DATE="2025-09-30",NETCASH_OPERATE=137)]),
        source("earnings",[
            dict(REPORTDATE="2026-09-30",BASIC_EPS=1.409,BPS=9.19,XSMLL=49.63),
            dict(REPORTDATE="2026-06-30",BASIC_EPS=.862,BPS=9.20,XSMLL=49.97),
            dict(REPORTDATE="2025-09-30",BASIC_EPS=1.006,BPS=8.16,XSMLL=44.0)])]


def normalize(d=None):
    return normalize_financials(d or docs(),ticker="002415",name="海康威视",cutoff="2026-10-30T11:00:00+08:00")


def economic_packet():
    from sovereign_hall.services.action_research import build_packet
    cutoff="2026-10-30T14:08:51+08:00"
    def quote(ticker,price):
        return dict(ticker=ticker,price=price,quoted_at=cutoff,fetched_at=cutoff,
                    source="sina_realtime_quote")
    state=dict(cutoff=cutoff,cash=7077.61023,total_assets=8676.01023,trades_today=0,
        positions=[dict(ticker="512800",shares=1800)],quotes={"512800":quote("512800",.888),"002415":quote("002415",34.71)},
        policy=dict(execution_mode="simulation",realtime_quotes_required=True,trade_during_market_hours_only=True,
                    max_daily_trades=5,stop_loss_pct=-.08,max_single_position=1,max_gross=1,
                    max_realtime_quote_age_seconds=120,min_holding_days=10,max_holding_days=30,
                    min_committee_confidence=.55,min_risk_reward=.8))
    return dict(frontier=build_packet(state,[],["002415"],objective="cost regression"))


def test_period_matched_cash_conversion_and_quarter_amounts():
    state = normalize()
    assert state["derived"]["yoy:cfo:2026-09-30"]["value"] == pytest.approx(72/137-1)
    assert state["derived"]["cfo_to_profit:2026-09-30"]["value"] == pytest.approx(72/129)
    assert state["derived"]["quarter:revenue:2026-09-30"]["value"] == 272
    assert state["derived"]["quarter:cfo:2026-09-30"]["value"] == 40
    assert state["facts"]["cfo:2026-09-30"]["basis"] == "YTD"


def test_ratios_and_eps_not_added_as_flows():
    state = normalize()
    assert not any(k.startswith(("quarter:gross_margin", "quarter:basic_eps")) for k in state["derived"])
    assert state["derived"]["yoy_pp:gross_margin:2026-09-30"]["unit"] == "percentage_points"


@pytest.mark.parametrize("mutation", ["identity", "future_capture", "same_day_notice", "duplicate", "nan"])
def test_reject_untrustworthy_financial_inputs(mutation):
    documents = docs()
    data = json.loads(documents[0]["content"])
    if mutation == "identity": data["rows"][0]["SECURITY_CODE"] = "600000"
    elif mutation == "future_capture": documents[0]["metadata"]["crawled_at"] = "2026-10-31T10:00:00+08:00"
    elif mutation == "same_day_notice": data["rows"][0]["NOTICE_DATE"] = "2026-10-30"
    elif mutation == "duplicate": data["rows"].append(data["rows"][0])
    else: data["rows"][0]["PARENT_NETPROFIT"] = float("nan")
    documents[0]["content"] = json.dumps(data)
    with pytest.raises(ValueError): normalize(documents)


def test_zero_or_negative_prior_not_assigned_growth_rate():
    documents = docs()
    data = json.loads(documents[1]["content"])
    data["rows"][2]["NETCASH_OPERATE"] = 0
    documents[1]["content"] = json.dumps(data)
    assert "yoy:cfo:2026-09-30" not in normalize(documents)["derived"]


def test_compiler_unchanged_price_cost_is_not_a_universal_veto():
    packet = economic_packet()
    draft = dict(action="REFER",shares=100,holding_days=30,
                 valuation=dict(assumed_forward_eps=1.941,assumed_pe=34.71/1.941,
                                basis_fact_ids=[], assumptions=["test only"]))
    unchanged = compile_draft(draft,packet)
    assert unchanged["status"] == "economic_rejection"
    assert unchanged["scenario"]["net_gain_scenario"] == pytest.approx(-15.2065)
    draft["valuation"]["assumed_pe"] *= 1.15
    upside = compile_draft(draft,packet)
    assert upside["status"] == "compiled"
    assert upside["scenario"]["net_gain_scenario"] > 500
    assert upside["scenario"]["loss_fraction_nav"] == pytest.approx(.0337,abs=.0001)
    assert upside["win_probability"] is None
    assert upside["executable"] is False


def test_compiler_does_not_enlarge_unapproved_or_infeasible_size():
    packet = economic_packet()
    draft = dict(action="REFER",shares=300,holding_days=180,
                 valuation=dict(assumed_forward_eps=2,assumed_pe=20,basis_fact_ids=[],assumptions=["test only"]))
    compiled = compile_draft(draft,packet)
    assert compiled["status"] == "economic_rejection"
    assert compiled["scenario"] is None
    assert len(compiled["violations"]) == 2
