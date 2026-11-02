"""Bounded research feedback and query allocation; never authorizes a trade."""
from datetime import datetime, timedelta
import json

FEEDBACK_QUERY_POLICY = "feedback_gap_v1"


async def load_topic_research_gaps(db_service, topic, *, as_of, days=7, limit=2):
    """Read only completed, past, same-topic HOLDs; retain latest per ticker.

    Committee text is a research question, not verified evidence. Keeping the
    outcome/round ids allows the next search to be traced back to its trigger.
    """
    conn = await db_service._get_connection()
    # The existing database stores local naive timestamps.
    end = as_of.astimezone().replace(tzinfo=None) if as_of.tzinfo else as_of
    start = end - timedelta(days=days)
    async with conn.execute(
        """SELECT o.id AS outcome_id, o.round_id, o.ticker, o.evidence_gaps,
                  o.reconsider_if, o.created_at, r.completed_at
           FROM simulation_committee_outcomes o
           JOIN research_rounds r ON r.id = o.round_id
           WHERE r.base_topic = ? AND o.direction = 'hold'
             AND o.outcome_code = 'committee_hold'
             AND r.completed_at IS NOT NULL
             AND datetime(r.completed_at) <= datetime(?)
             AND datetime(o.created_at) <= datetime(?)
             AND datetime(o.created_at) >= datetime(?)
           ORDER BY datetime(o.created_at) DESC, o.id DESC LIMIT 40""",
        (topic, end.isoformat(), end.isoformat(), start.isoformat()),
    ) as cursor:
        rows = [dict(row) async for row in cursor]
    selected, seen = [], set()
    for row in rows:
        ticker = row['ticker']
        if ticker in seen:
            continue
        seen.add(ticker)
        for field in ('evidence_gaps', 'reconsider_if'):
            try:
                items = json.loads(row[field])
            except (TypeError, ValueError):
                items = []
            row[field] = [s[:400] for s in items if isinstance(s, str) and s.strip()][:4] if isinstance(items, list) else []
        if not row['evidence_gaps'] and not row['reconsider_if']:
            continue
        selected.append(row)
        if len(selected) >= max(1, int(limit)):
            break
    return selected


def select_research_queries(generated, templates, *, count, deployment,
                            policy='legacy', generation_mode='primary'):
    """Give generated gap queries a bounded share; keep legacy failure fallback."""
    def unique(values):
        seen, result = set(), []
        for value in values:
            text = str(value or '').strip()
            if text and text.lower() not in seen:
                seen.add(text.lower())
                result.append(text)
        return result

    count = max(0, int(count))
    generated, templates = unique(generated), unique(templates)
    active = (deployment and policy == FEEDBACK_QUERY_POLICY
              and generation_mode in {'primary', 'repaired'})
    if active:
        share = count // 2
        ordered = generated[:share] + templates[:count-share] + generated[share:] + templates[count-share:]
    else:
        ordered = templates + generated if deployment else generated + templates
    selected = unique(ordered)[:count]
    generated_keys = {q.lower() for q in generated}
    return selected, {
        'policy': policy,
        'feedback_allocation_active': active,
        'generated_queries': generated,
        'selected_generated_count': sum(q.lower() in generated_keys for q in selected),
        'discarded_generated_count': sum(q.lower() not in {s.lower() for s in selected} for q in generated),
    }
