"""Real harness cancellation with a blocked fake transport; no investment test."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

from sovereign_hall.services.experiment_runtime import snapshot_code
from sovereign_hall.core.config import get_config


def test_controller_cancellation_closes_client_and_keeps_terminal_receipt(tmp_path):
    root = Path(__file__).resolve().parents[1]
    run = tmp_path/'experiment'
    run.mkdir()
    manifest = snapshot_code(root, run/'runtime/sovereign_hall')
    shutil.copyfile(root/'scripts/run_primary_event_experiment.py', run/'worker.py')
    state = dict(cutoff='2026-10-15T14:10:00+08:00', policy=get_config().get('simulation',{}))
    data = dict(state=state, universe=['600000'], objective='engineering cancellation fixture',
        arms={'baseline':dict(description='fixture, not a quality comparison', documents=[dict(
            id='fixture', content='600000 本期营业收入与净利润属于接口测试文字。' * 10,
            source='fixture', metadata=dict(url='https://example.invalid/fixture',
                publish_time='2026-10-14',publish_time_source='structured_disclosure_notice',
                crawled_at='2026-10-15T14:00:00+08:00'))])})
    (run/'input.json').write_text(json.dumps(data))
    digest = lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
    protocol = dict(experiment_id='cancellation_engineering_fixture', code_sha256=manifest,
        input_sha256=digest(run/'input.json'),worker_sha256=digest(run/'worker.py'),
        budget=dict(max_calls_per_arm=1,estimated_usd_per_arm=.01,wall_seconds_per_arm=20))
    (run/'protocol.json').write_text(json.dumps(protocol))
    code = r'''
import asyncio, importlib.util, json, sys
from pathlib import Path
run=Path(sys.argv[1]);sys.path.insert(0,str(run/'runtime'))
from sovereign_hall.services import experiment_runtime as runtime
original=runtime.isolate_process
ready=asyncio.Event();instances=[]
class FakeClient:
    provider='openai';model='engineering-fixture';client=True
    def __init__(self,**kw):self.closed=False;instances.append(self)
    def get_stats(self):return dict(total_requests=0,total_tokens=0,prompt_tokens=0,completion_tokens=0,total_cost=0)
    async def chat(self,**kw):ready.set();await asyncio.Event().wait()
    async def close(self):self.closed=True
def isolated(arm):
    receipt=original(arm)
    from sovereign_hall.services import llm_client
    llm_client.LLMClient=FakeClient
    return receipt
runtime.isolate_process=isolated
spec=importlib.util.spec_from_file_location('harness',run/'worker.py')
harness=importlib.util.module_from_spec(spec);spec.loader.exec_module(harness)
async def main():
    task=asyncio.create_task(harness.worker(run,'baseline'))
    await asyncio.wait_for(ready.wait(),15)
    task.cancel()
    try:await task
    except asyncio.CancelledError:pass
    else:raise AssertionError('cancellation swallowed')
    row=json.loads((run/'baseline/result.json').read_text())
    assert row['status']=='cancelled' and row['orders']==row['historical_predictions_written']==0
    assert len(instances)==1 and instances[0].closed
    assert row['accounting']['startup']['total_requests']==0
asyncio.run(main())
'''
    child = subprocess.run([sys.executable,'-B','-c',code,str(run)],cwd=tmp_path,
                           capture_output=True,text=True,timeout=25)
    assert child.returncode == 0, child.stdout+'\n'+child.stderr
