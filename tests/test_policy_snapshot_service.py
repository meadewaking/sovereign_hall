"""Tests for PR1.3 — PolicySnapshotService and the immutable policy_snapshots table."""

from __future__ import annotations

from pathlib import Path
from dataclasses import replace
import json

import aiosqlite
import pytest

from sovereign_hall.infrastructure.sqlite.migrations import apply_architecture_migrations
from sovereign_hall.services.policy_snapshot_service import (
    PolicyManifest,
    PolicySnapshotService,
    runtime_code_artifacts,
)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "policy_snapshots.db"


@pytest.fixture
def service(db_path: Path) -> PolicySnapshotService:
    return PolicySnapshotService(db_path)


@pytest.mark.asyncio
async def test_register_snapshot_is_idempotent_on_same_content(db_path, service):
    manifest = PolicyManifest(
        policy_family="run_discussion",
        version="run_discussion_canonical_v1",
        code_artifacts={"services/decision.py": "abc123"},
        effective_config={"max_single_position": 1.0, "min_committee_confidence": 0.55},
        prompt_versions={"committee": {"version": "v1", "hash": "deadbeef"}},
        model_versions={"committee": {"model": "gpt-4o", "temperature": 0.2}},
        policy_module_versions={"portfolio": "v1"},
        source_manifest_path="manifest.json",
        change_reason="initial registration",
        changes_behavior=False,
    )
    first = await service.register(manifest)
    second = await service.register(manifest)
    assert first.snapshot_id == second.snapshot_id
    assert first.content_hash == second.content_hash


@pytest.mark.asyncio
async def test_register_rejects_reusing_version_with_different_content(db_path, service):
    original = PolicyManifest(
        policy_family="run_discussion",
        version="v1",
        effective_config={"max_single_position": 1.0},
    )
    await service.register(original)
    different = PolicyManifest(
        policy_family="run_discussion",
        version="v1",
        effective_config={"max_single_position": 0.5},
    )
    with pytest.raises(ValueError, match="immutable"):
        await service.register(different)


@pytest.mark.asyncio
async def test_register_allows_same_content_under_new_version_label(
    db_path, service
):
    """The immutability contract forbids reusing a (family, version) pair
    with different content; it does not forbid promoting identical content
    to a new version label."""
    same_config = {"max_single_position": 1.0}
    first = PolicyManifest(
        policy_family="run_discussion",
        version="v1",
        effective_config=same_config,
    )
    await service.register(first)
    second = PolicyManifest(
        policy_family="run_discussion",
        version="v2",
        effective_config=same_config,
        change_reason="relabel under new version",
    )
    first_persisted = await service.register(first)
    persisted = await service.register(second)
    assert persisted.version == "v2"
    assert persisted.snapshot_id != first_persisted.snapshot_id


@pytest.mark.asyncio
async def test_credentials_are_stripped_from_manifest_and_hash(db_path, service):
    with_secret = PolicyManifest(
        policy_family="run_discussion",
        version="v1",
        effective_config={
            "max_single_position": 1.0,
            "api_key": "sk-super-secret",
            "proxy_password": "hunter2",
        },
    )
    without_secret = PolicyManifest(
        policy_family="run_discussion",
        version="v1",
        effective_config={"max_single_position": 1.0},
    )
    first = await service.register(with_secret)
    second = await service.register(without_secret)
    # The credential-bearing manifest and the stripped manifest must hash the
    # same way — credentials never enter the hash input.
    assert first.content_hash == second.content_hash

    async with aiosqlite.connect(db_path) as conn:
        async with conn.execute(
            "SELECT effective_config_json FROM policy_snapshots WHERE id = ?",
            (first.snapshot_id,),
        ) as cursor:
            row = await cursor.fetchone()
    import json

    persisted = json.loads(row[0])
    assert "api_key" not in persisted
    assert "proxy_password" not in persisted


@pytest.mark.asyncio
async def test_policy_snapshots_are_immutable_after_write(db_path, service):
    manifest = PolicyManifest(
        policy_family="run_discussion",
        version="v1",
        effective_config={"max_single_position": 1.0},
    )
    persisted = await service.register(manifest)
    async with aiosqlite.connect(db_path) as conn:
        with pytest.raises(aiosqlite.IntegrityError):
            await conn.execute(
                "UPDATE policy_snapshots SET content_hash = ? WHERE id = ?",
                ("different", persisted.snapshot_id),
            )
            await conn.commit()


@pytest.mark.asyncio
async def test_apply_architecture_migrations_extends_legacy_policy_snapshots(
    db_path,
):
    async with aiosqlite.connect(db_path) as conn:
        await apply_architecture_migrations(conn)
        async with conn.execute("PRAGMA table_info(policy_snapshots)") as cursor:
            columns = {str(row[1]) for row in await cursor.fetchall()}
    assert "content_hash" in columns
    assert "manifest_json" in columns
    assert "effective_config_json" in columns
    assert "prompt_versions_json" in columns
    assert "model_versions_json" in columns
    assert "policy_module_versions_json" in columns
    assert "source_manifest_path" in columns
    assert "effective_at" in columns
    assert "change_reason" in columns
    assert "changes_behavior" in columns


@pytest.mark.asyncio
async def test_runtime_versions_record_changed_inputs_without_relabeling_history(db_path, service):
    base = PolicyManifest(
        policy_family="run_discussion", version="runtime_v2",
        code_artifacts={"services/research_memory.py": "original"},
        effective_config={"llm": {"max_tokens": 4000, "api_key": "private"}},
        prompt_versions={"committee": {"version": "v1"}},
        model_versions={"committee": {"model": "model_a"}},
    )
    initial = base.with_content_version()
    first = await service.register(initial)
    unchanged = replace(base, effective_at="2026-10-12T14:30:00",
                        source_manifest_path="/another/check-out/run_discussion.py",
                        change_reason="restart",
                        effective_config={"llm": {"max_tokens": 4000, "api_key": "rotated"}})
    assert (await service.register(unchanged.with_content_version())).snapshot_id == first.snapshot_id
    changed = [
        replace(base, code_artifacts={"services/research_memory.py": "changed"}),
        replace(base, effective_config={"llm": {"max_tokens": 8000}}),
        replace(base, prompt_versions={"committee": {"version": "v2"}}),
        replace(base, model_versions={"committee": {"model": "model_b"}}),
    ]
    ids = {first.snapshot_id}
    for manifest in changed:
        persisted = await service.register(manifest.with_content_version())
        assert persisted.snapshot_id not in ids
        assert (await service.register(manifest.with_content_version())).snapshot_id == persisted.snapshot_id
        ids.add(persisted.snapshot_id)
    async with aiosqlite.connect(db_path) as conn:
        rows = await (await conn.execute("SELECT id, manifest_json FROM policy_snapshots")).fetchall()
    assert len(rows) == 5
    original = json.loads(dict(rows)[first.snapshot_id])
    assert original["code_artifacts"] == base.code_artifacts
    assert original["effective_config"] == {"llm": {"max_tokens": 4000}}


@pytest.mark.asyncio
async def test_rejected_drift_rolls_back_without_replacing_or_aliasing_snapshot(db_path, service):
    manifest = PolicyManifest(policy_family="run_discussion", version="fixed_v1",
                              effective_config={"simulation": {"max_daily_trades": 5}})
    first = await service.register(manifest)
    async with aiosqlite.connect(db_path) as conn:
        before = await (await conn.execute("SELECT * FROM policy_snapshots")).fetchall()
    with pytest.raises(ValueError, match="immutable"):
        await service.register(replace(manifest, effective_config={"simulation": {"max_daily_trades": 6}}))
    async with aiosqlite.connect(db_path) as conn:
        after = await (await conn.execute("SELECT * FROM policy_snapshots")).fetchall()
    assert after == before
    assert (await service.register(manifest)).snapshot_id == first.snapshot_id


@pytest.mark.asyncio
async def test_unverified_legacy_snapshot_cannot_claim_current_runtime(db_path, service):
    await service.initialize()
    async with aiosqlite.connect(db_path) as conn:
        await conn.execute("INSERT INTO policy_snapshots(id,policy_family,version,config_json,source,created_at) "
                           "VALUES ('legacy','run_discussion','legacy_v1','{}','legacy','2026-01-01')")
        await conn.commit()
    with pytest.raises(ValueError, match="unverified legacy content"):
        await service.register(PolicyManifest(policy_family="run_discussion", version="legacy_v1"))
    assert await service.resolve_id("run_discussion", "legacy_v1") == "legacy"


def test_runtime_artifacts_cover_research_memory_and_calendar_without_runtime_data(tmp_path):
    files = {"run_discussion.py": "entry", "services/research_memory.py": "memory",
             "services/llm_client.py": "client", "agents/committee.py": "prompt",
             "services/calendar_data/cn_2026.json": "calendar"}
    for name, content in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    for name in ("config.yaml", "tests/test_example.py", "runs/trial/model.py", "data/output.py"):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("private or runtime data")
    before = runtime_code_artifacts(tmp_path, "run_discussion.py")
    assert set(before) == set(files)
    (tmp_path / "services/research_memory.py").write_text("new memory behaviour")
    after = runtime_code_artifacts(tmp_path, "run_discussion.py")
    assert after["services/research_memory.py"] != before["services/research_memory.py"]
    assert after["agents/committee.py"] == before["agents/committee.py"]
    with pytest.raises(ValueError, match="entrypoint unavailable"):
        runtime_code_artifacts(tmp_path, "missing.py")
