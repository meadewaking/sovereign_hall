"""Process-local research experiment isolation and model-call accounting.

Use once, in a fresh subprocess, before importing clients/config/database.
This is an experiment boundary, not an OS sandbox or a trading entry point.
"""
from __future__ import annotations

import contextvars
import copy
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + '\n')
    temporary.replace(path)


def snapshot_code(root: Path, destination: Path) -> dict:
    """Copy only Python source and private config; never inherit runtime data."""
    destination.mkdir(parents=True, exist_ok=False)
    sources = list(root.glob('*.py'))
    for name in ('agents', 'application', 'core', 'domain', 'infrastructure',
                 'ports', 'services', 'utils'):
        sources.extend((root / name).rglob('*.py'))
    sources.append(root / 'config.yaml')
    manifest = {}
    for source in sorted(sources):
        relative = source.relative_to(root)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        if source.name == 'config.yaml':
            target.chmod(0o600)
        manifest[str(relative)] = hashlib.sha256(target.read_bytes()).hexdigest()
    return manifest


def isolate_process(arm: Path) -> dict:
    """Refuse reused arms/initialized dependencies instead of resetting counters."""
    prefix = __package__.split('.')[0]
    forbidden = ('core.config', 'services.persistence', 'services.llm_client',
                 'services.database', 'run_discussion')
    if any(prefix + '.' + name in sys.modules for name in forbidden):
        raise RuntimeError('experiment isolation must precede client/config imports')
    arm = arm.resolve()
    arm.mkdir(parents=True, exist_ok=False)
    sys.dont_write_bytecode = True
    os.chdir(arm)

    def guard(event, args):
        if event == 'sqlite3.connect':
            value = str(args[0])
            if value == ':memory:':
                return
            if value.startswith('file:') or not Path(value).resolve().is_relative_to(arm):
                raise RuntimeError('experiment SQLite access outside arm denied')
        if event == 'open':
            path, mode, flags = args
            writing = (isinstance(mode, str) and any(c in mode for c in 'wax+')) or (
                isinstance(flags, int) and flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC))
            if writing and not isinstance(path, int) and not Path(path).resolve().is_relative_to(arm):
                raise RuntimeError('experiment file write outside arm denied')

    sys.addaudithook(guard)
    from .. import core
    core.DATA_DIR = arm / 'data'
    from . import persistence
    persistence.DATA_DIR = core.DATA_DIR
    persistence.STATS_FILE = core.DATA_DIR / 'session_stats.json'
    persistence.HISTORY_DIR = core.DATA_DIR / 'session_history'
    from ..core.config import get_config
    config = get_config()
    config._config['database']['sqlite_path'] = str(core.DATA_DIR / 'sovereign_hall.db')
    # YAML may contain absolute production directories, overriding defaults.
    for section, key, name in (
        ('knowledge_wiki', 'root', 'knowledge'), ('system', 'checkpoint_dir', 'checkpoints'),
        ('system', 'log_dir', 'logs'), ('institutional_memory', 'playbook_path', 'playbooks'),
        ('output', 'reports_dir', 'reports'), ('output', 'minutes_dir', 'minutes'),
    ):
        if section in config._config and key in config._config[section]:
            config._config[section][key] = str(core.DATA_DIR / name)
    receipt = {'arm': str(arm), 'stats_file': str(persistence.STATS_FILE),
               'startup': persistence.get_persistence().load_previous_stats()}
    if receipt['startup']['total_requests'] or receipt['startup']['total_tokens']:
        raise RuntimeError('nonzero startup accounting')
    write_json(arm / 'isolation.json', receipt)
    return receipt


class RecordedLLM:
    """Record outer calls and every transport attempt, including failed retries.

    Per-attempt usage is the unmodified usage dict returned by LLMClient's
    provider adapter, not a delta of concurrently changing/global counters.
    Pricing is an estimate; provider billing remains unavailable.
    """

    def __init__(self, client, directory: Path, *, max_calls: int):
        if type(max_calls) is not int or max_calls < 1:
            raise ValueError('positive integer call budget required')
        if (directory / 'client_startup.json').exists() or any(directory.glob('model_call_*.json')):
            raise RuntimeError('call ledger already exists')
        self.client = client
        self.directory = directory
        self.max_calls = max_calls
        self.calls = []
        self.current = contextvars.ContextVar('experiment_call', default=None)
        self.startup = copy.deepcopy(client.get_stats())
        if self.startup['total_requests'] or self.startup['total_tokens']:
            raise RuntimeError('client inherited history')
        for name in ('_openai_chat', '_anthropic_chat'):
            original = getattr(client, name, None)
            if original is not None:
                setattr(client, name, self._transport(original, name))
        write_json(directory / 'client_startup.json', self.startup)

    def _transport(self, original, name):
        async def tracked(*args, **kwargs):
            record = self.current.get()
            if record is None:
                raise RuntimeError('unrecorded transport call')
            attempt = {'adapter': name, 'started_at': datetime.now().astimezone().isoformat()}
            record['attempts'].append(attempt)
            self._save(record)
            start = time.monotonic()
            try:
                response, usage = await original(*args, **kwargs)
                attempt.update(status='returned', usage=copy.deepcopy(usage))
                return response, usage
            except BaseException as exc:
                attempt.update(status='failed', error_type=type(exc).__name__)
                raise
            finally:
                attempt['elapsed_seconds'] = time.monotonic() - start
                self._save(record)
        return tracked

    def _save(self, record):
        write_json(self.directory / f"model_call_{record['call']:03}.json", record)

    async def chat(self, **kwargs):
        if len(self.calls) >= self.max_calls:
            raise RuntimeError('model call budget exhausted')
        record = {'call': len(self.calls) + 1, 'model': self.client.model,
                  'request': kwargs, 'attempts': [], 'status': 'started',
                  'started_at': datetime.now().astimezone().isoformat()}
        self.calls.append(record)
        self._save(record)
        token = self.current.set(record)
        start = time.monotonic()
        try:
            response = await self.client.chat(**kwargs)
            record.update(status='success', response=response)
            return response
        except BaseException as exc:
            record.update(status='failed', error_type=type(exc).__name__)
            raise
        finally:
            record['elapsed_seconds'] = time.monotonic() - start
            self._save(record)
            self.current.reset(token)

    def summary(self):
        attempts = [a for r in self.calls for a in r['attempts']]
        complete = bool(attempts) and all(a.get('status') == 'returned' and
                       isinstance(a.get('usage'), dict) and
                       all(type(a.get('usage', {}).get(k)) is int and a['usage'][k] >= 0
                           for k in ('prompt_tokens', 'completion_tokens')) for a in attempts)
        totals = {k: sum(a['usage'][k] for a in attempts) if complete else None
                  for k in ('prompt_tokens', 'completion_tokens')}
        stats = self.client.get_stats()
        return {'outer_calls': len(self.calls), 'transport_attempts': len(attempts),
                'provider_usage_complete': complete, 'provider_usage': totals,
                'client_stats': stats, 'startup': self.startup,
                'usage_matches_client': complete and all(totals[k] == stats[k] for k in totals),
                'configured_cost_estimate_usd': stats['total_cost'], 'billed_cost_usd': None}
