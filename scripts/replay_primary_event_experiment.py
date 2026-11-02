"""Verify frozen originals and reaggregate recorded terminal votes, offline.

This does not replay stochastic reasoning, create historical predictions, fetch
prices, or place orders. Unfinished meetings remain unfinished in the report.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import logging.handlers
from pathlib import Path
import sqlite3
import sys


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    args = parser.parse_args()
    run = args.run.resolve()
    protocol = json.loads((run/'protocol.json').read_text())
    if digest(run/'input.json') != protocol['input_sha256'] or digest(run/'worker.py') != protocol['worker_sha256']:
        raise ValueError('frozen inputs/worker drift')
    for name, expected in protocol['code_sha256'].items():
        if digest(run/'runtime'/'sovereign_hall'/name) != expected:
            raise ValueError('frozen source drift: '+name)
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(run/'runtime'))
    from sovereign_hall.services.primary_event_research import event_documents
    from sovereign_hall.services.experiment_runtime import write_json
    data = json.loads((run/'input.json').read_text())
    acquisition = json.loads((run.parent/'candidate_acquisition.json').read_text())
    rebuilt = []
    for receipt in acquisition['receipts']:
        event = receipt['event']
        path = run.parent/'raw'/(event['ticker']+'_'+event['announcement_id']+'.pdf')
        if digest(path) != receipt['pdf_sha256']:
            raise ValueError('original PDF bytes changed')
        docs, replay = event_documents(path.read_bytes(), event=event,
            captured_at=datetime.fromisoformat(receipt['captured_at']),
            cutoff=datetime.fromisoformat(receipt['cutoff']))
        if replay != receipt:
            raise ValueError('source receipt differs')
        rebuilt.extend(d.to_dict() for d in docs)
    # Legacy Document.timestamp is the Python object's construction clock, not
    # source availability. Compare every provenance/content field exactly;
    # preserve the original object clocks in input.json without re-aging them.
    def source_payload(doc):
        return {k:v for k,v in doc.items() if k != 'timestamp'}
    if [source_payload(d) for d in rebuilt] != [source_payload(d) for d in data['arms']['candidate']['documents']]:
        raise ValueError('original page reconstruction differs')
    old_handler = logging.handlers.RotatingFileHandler
    logging.handlers.RotatingFileHandler = lambda _filename,*a,**kw: old_handler(run/'replay.log',*a,**kw)
    try:
        from sovereign_hall import run_discussion as rd
    finally:
        logging.handlers.RotatingFileHandler = old_handler
    outputs = {}
    for side in ('baseline','candidate'):
        directory = run/side
        if not (directory/'result.json').exists():
            outputs[side] = dict(status='running', terminal_votes_replayed=0)
            continue
        result = json.loads((directory/'result.json').read_text())
        calls = [json.loads(p.read_text()) for p in sorted(directory.glob('model_call_*.json'))]
        if (result['orders'] or result['historical_predictions_written']
                or result['accounting']['startup']['total_requests']):
            raise ValueError('isolation regression')
        db = sqlite3.connect((directory/'data/sovereign_hall.db').as_uri()+'?mode=ro', uri=True)
        db.row_factory = sqlite3.Row
        decisions = []
        try:
            durable = [json.loads(r['payload_json']) for r in db.execute(
                "SELECT payload_json FROM round_events WHERE event_type='IsolatedCommitteeDecision' ORDER BY id")]
            if result['status'] == 'completed' and [d['decision_id'] for d in durable] != [d['decision_id'] for d in result.get('decisions', [])]:
                raise ValueError('completed result differs from durable decisions')
            # A later case can hit the wall budget after an earlier case has
            # finished. Its committed event remains an observed decision; it
            # must neither disappear nor turn an unfinished arm into a pass.
            for decision in durable:
                proposal = next(p for p in result['proposals'] if p['ticker'] == decision['ticker'])
                turns = [dict(r) for r in db.execute(
                    'SELECT stage,seat_label,raw_output FROM committee_turns WHERE meeting_id=? ORDER BY id',
                    (decision['meeting_id'],))]
                raw, weights, labels = [], [], []
                for vote in decision['individual_votes']:
                    matching = [t for t in turns if t['seat_label'] == vote['role'] and t['stage'] in
                        {'round4_vote','round4_vote_retry','deployment_deadlock_review','deployment_deadlock_review_retry'}]
                    if not matching:
                        # Missing tasks are abstentions, never fabricated HOLDs.
                        value = '[committee_task_absent] no recorded terminal vote'
                    else:
                        value = matching[-1]['raw_output']
                        if not rd.parse_strict_committee_vote(value)['schema_valid']:
                            value = '[committee_task_absent] strict JSON invalid'
                    raw.append(value); weights.append(vote['effective_weight']); labels.append(vote['role'])
                replayed = rd.aggregate_committee_decision(proposal, raw, vote_weights=weights, vote_labels=labels)
                fields = ('direction','confidence','target_position','vote_summary','vote_margin',
                          'direction_support','parsed_vote_count','vote_quorum_met')
                if any(replayed[k] != decision[k] for k in fields):
                    raise ValueError('terminal vote aggregation differs: '+side+'/'+decision['ticker'])
                decisions.append(dict(ticker=decision['ticker'], direction=decision['direction'],
                    quorum=decision['vote_quorum_met'], aggregation_matches=True))
            executable, rejected = rd.preflight_committee_decisions(durable,
                {p['ticker'] for p in data['state']['positions']}, lambda t:t,
                min_long_confidence=data['state']['policy']['min_committee_confidence'])
            outcomes = [dict(r) for r in db.execute('SELECT ticker,status,outcome_code FROM committee_meetings')]
        finally:
            db.close()
        stop_path = run.parent/'stop_decision.json'
        stopped = None
        if side == 'candidate' and stop_path.exists():
            stopped = json.loads(stop_path.read_text())
            if stopped['input_sha256'] != protocol['input_sha256']:
                raise ValueError('early-stop decision applies to different frozen inputs')
        outputs[side] = dict(status='rejected_early_stopped' if stopped else result['status'],
            raw_harness_status=result['status'], early_stop=stopped, terminal_decisions=decisions, meetings=outcomes,
            source_model_calls=len(calls), usage_matches_client=result['accounting']['usage_matches_client'],
            executable_preflight=executable, preflight_rejections=rejected)
    receipt = dict(experiment_id=protocol['experiment_id'], verified_source_files=len(protocol['code_sha256']),
        original_page_replay_matches=True, object_construction_clock_excluded=True,
        source_capture_and_publication_times_checked=True, outputs=outputs, new_model_calls=0, orders=0,
        full_reasoning_replayed=False, return_improvement=None)
    write_json(run/'replay_result.json', receipt)
    print(json.dumps(receipt, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
