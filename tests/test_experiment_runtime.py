"""Engineering boundaries only; synthetic calls do not test investment skill."""
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from sovereign_hall.services.experiment_runtime import RecordedLLM, snapshot_code


ROOT = Path(__file__).resolve().parents[1]


def child(code, cwd, package_parent):
    return subprocess.run([sys.executable, '-B', '-c', code], cwd=cwd,
                          env={**os.environ, 'PYTHONPATH': str(package_parent)},
                          capture_output=True, text=True, timeout=30)


def test_fresh_arms_ignore_inherited_stats_and_deny_outside_access(tmp_path):
    runtime = tmp_path / 'runtime' / 'sovereign_hall'
    manifest = snapshot_code(ROOT, runtime)
    assert all(p.endswith('.py') or p == 'config.yaml' for p in manifest)
    assert not (runtime / 'data').exists()
    # Reproduce the old continuation trap deliberately, outside the snapshot.
    (runtime / 'data').mkdir()
    poison = runtime / 'data' / 'session_stats.json'
    poison.write_text(json.dumps({'token_stats': {'total_tokens': 9, 'prompt_tokens': 9,
                                                'total_requests': 999}}))
    original = poison.read_bytes()
    legacy = child('''
from pathlib import Path
from sovereign_hall import core
core.DATA_DIR=Path('legacy_override')
assert core.TokenStats().total_requests == 999
print('old isolation inherited 999 requests')
''', tmp_path, runtime.parent)
    assert legacy.returncode == 0, legacy.stderr
    code = '''
import json, sqlite3
from pathlib import Path
from sovereign_hall.services.experiment_runtime import isolate_process
arm=Path(%r)
receipt=isolate_process(arm)
from sovereign_hall.core import TokenStats
from sovereign_hall.services.persistence import STATS_FILE
stats=TokenStats()
assert stats.total_requests == 0
stats.add_usage(7, 3)
assert json.loads(STATS_FILE.read_text())['token_stats']['total_requests'] == 1
for action in (lambda: Path(%r).write_text('overwrite'),
               lambda: sqlite3.connect(%r),
               lambda: sqlite3.connect('file:outside.db?mode=ro', uri=True)):
    try: action()
    except RuntimeError: pass
    else: raise AssertionError('outside access allowed')
sqlite3.connect(str(arm/'allowed.db')).close()
print('isolated')
'''
    for name in ('a', 'b'):
        result = child(code % (str(tmp_path/name), str(poison), str(tmp_path/'outside.db')),
                       tmp_path, runtime.parent)
        assert result.returncode == 0, result.stderr
    assert poison.read_bytes() == original
    assert not (tmp_path/'outside.db').exists()
    again = child(code % (str(tmp_path/'a'), str(poison), str(tmp_path/'outside.db')),
                  tmp_path, runtime.parent)
    assert again.returncode != 0 and 'FileExistsError' in again.stderr


def test_late_initialization_is_rejected(tmp_path):
    result = child('''
from pathlib import Path
from sovereign_hall.core.config import get_config
from sovereign_hall.services.experiment_runtime import isolate_process
isolate_process(Path('arm'))
''', tmp_path, ROOT.parent)
    assert result.returncode != 0 and 'must precede' in result.stderr
    assert not (tmp_path/'arm').exists()


class Client:
    model = 'test'

    def __init__(self):
        self.prompt = 0
        self.completion = 0
        self.requests = 0

    def get_stats(self):
        return dict(total_requests=self.requests, total_tokens=self.prompt+self.completion,
                    prompt_tokens=self.prompt, completion_tokens=self.completion, total_cost=0)

    async def _openai_chat(self, value):
        await asyncio.sleep(0)
        if value == 'failure':
            raise OSError('transport')
        if value == 'cancel':
            raise asyncio.CancelledError()
        if value == 'missing':
            return 'answer', None
        return value, dict(prompt_tokens=int(value), completion_tokens=0)

    async def chat(self, *, user):
        if user == 'cache':
            return 'cached'
        if user == 'retry':
            try:
                await self._openai_chat('failure')
            except OSError:
                user = '2'
        response, usage = await self._openai_chat(user)
        self.requests += 1
        if usage:
            self.prompt += usage['prompt_tokens']
            self.completion += usage['completion_tokens']
        return response


@pytest.mark.asyncio
async def test_concurrent_attempts_keep_own_usage_and_zero_completion(tmp_path):
    recorded = RecordedLLM(Client(), tmp_path, max_calls=2)
    assert await asyncio.gather(recorded.chat(user='7'), recorded.chat(user='11')) == ['7', '11']
    assert [r['attempts'][0]['usage']['prompt_tokens'] for r in recorded.calls] == [7, 11]
    assert recorded.summary()['usage_matches_client']
    with pytest.raises(RuntimeError, match='budget'):
        await recorded.chat(user='1')


@pytest.mark.asyncio
@pytest.mark.parametrize('value,error,attempt_count', [
    ('failure', OSError, 1), ('cancel', asyncio.CancelledError, 1),
    ('missing', None, 1), ('cache', None, 0), ('retry', None, 2)])
async def test_failures_missing_usage_retries_and_cache_not_billing_proof(tmp_path, value, error, attempt_count):
    recorded = RecordedLLM(Client(), tmp_path, max_calls=1)
    if error:
        with pytest.raises(error):
            await recorded.chat(user=value)
    else:
        await recorded.chat(user=value)
    summary = recorded.summary()
    assert summary['transport_attempts'] == attempt_count
    assert summary['provider_usage_complete'] is False
    assert summary['billed_cost_usd'] is None
    saved = json.loads((tmp_path/'model_call_001.json').read_text())
    assert saved['status'] == ('failed' if error else 'success')


def test_nonzero_client_cannot_start(tmp_path):
    client = Client()
    client.requests = 9
    with pytest.raises(RuntimeError, match='inherited'):
        RecordedLLM(client, tmp_path, max_calls=3)


def test_ledger_cannot_be_overwritten(tmp_path):
    RecordedLLM(Client(), tmp_path, max_calls=1)
    with pytest.raises(RuntimeError, match='already exists'):
        RecordedLLM(Client(), tmp_path, max_calls=1)
