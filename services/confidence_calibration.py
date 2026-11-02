"""Read-only, point-in-time prediction calibration diagnostics.

Path correctness is not realized profit. Missing policy provenance stays unknown;
overlapping ticker windows are discarded even when model confidence differs.
"""
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import math


def instant(value):
    stamp = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    return stamp.replace(tzinfo=timezone(timedelta(hours=8))) if stamp.tzinfo is None else stamp


def summarize(rows, *, cutoff, minimum_samples=30):
    cutoff = instant(cutoff)
    groups, ends = defaultdict(list), {}
    excluded = defaultdict(int)
    dated = []
    for row in rows:
        try:
            dated.append((instant(row['predicted_at']), row))
        except (ValueError, TypeError, KeyError):
            excluded['invalid_metadata'] += 1
    for _, row in sorted(dated, key=lambda pair: pair[0]):
        try:
            start = instant(row.get('entry_date') or row['predicted_at'])
            days = int(row['expected_days'])
            confidence = float(row['confidence'])
            if days <= 0 or not math.isfinite(confidence) or not 0 <= confidence <= 1:
                raise ValueError('invalid value')
            end = start + timedelta(days=days)
            validated = instant(row['validated_at'])
        except (ValueError, TypeError, KeyError):
            excluded['invalid_metadata'] += 1
            continue
        if end > cutoff or validated > cutoff or validated < start:
            excluded['not_available_at_cutoff'] += 1
            continue
        if row.get('status') != 'validated' or row.get('result') not in {'correct', 'wrong', 'partial'}:
            excluded['unjudgeable'] += 1
            continue
        version = row.get('policy_snapshot_id') or 'unknown'
        direction = row.get('direction')
        if direction not in {'long', 'short', 'hold'}:
            excluded['invalid_direction'] += 1
            continue
        # Shared market paths remain dependent across policy versions. Do not
        # turn a version bump into a fresh independent event.
        overlap_key = (row['ticker'], direction)
        if overlap_key in ends and start <= ends[overlap_key]:
            excluded['overlapping_event'] += 1
            continue
        ends[overlap_key] = end
        horizon = '1-14' if days <= 14 else '15-60' if days <= 60 else '61-180'
        bucket = min(9, int(confidence * 10))
        groups[(version, direction, horizon, bucket)].append((confidence, row['result']))
    output = []
    for (version, direction, horizon, bucket), values in sorted(groups.items()):
        n = len(values)
        correct = sum(result == 'correct' for _, result in values)
        partial = sum(result == 'partial' for _, result in values)
        p, z = correct / n, 1.96
        center = (p + z*z/(2*n)) / (1+z*z/n)
        half = z * math.sqrt(p*(1-p)/n+z*z/(4*n*n)) / (1+z*z/n)
        output.append(dict(version=version, direction=direction, horizon_days=horizon,
            confidence_bucket=bucket/10, count=n, correct=correct, partial=partial,
            strict_correct_rate=p, mean_confidence=sum(v[0] for v in values)/n,
            wilson_interval=[center-half, center+half],
            status='diagnostic_only' if n >= minimum_samples and version != 'unknown'
                   else 'insufficient_evidence'))
    return dict(groups=output, excluded=dict(excluded), promotion_eligible=False,
                probability_calibrated=False, metric='strict_price_path_correctness_not_net_profit')


async def load_summary(conn, *, cutoff):
    async with conn.execute('''SELECT p.*, r.policy_snapshot_id FROM price_predictions p
        LEFT JOIN research_rounds r ON r.id=p.round_id
        WHERE p.predicted_at IS NOT NULL ORDER BY p.predicted_at,p.id''') as cursor:
        rows = [dict(r) for r in await cursor.fetchall()]
    return summarize(rows, cutoff=cutoff)
