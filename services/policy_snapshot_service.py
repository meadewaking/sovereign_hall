"""PR1.3 — Build, persist and resolve immutable policy snapshots.

A policy snapshot captures exactly what was loaded at production start: the
decision code hashes, the effective config (after env overrides), the prompt
versions and content hashes, the model identifiers and the policy-module
versions.  Credentials, API keys and proxy passwords are never part of the
manifest or its hash input.

Snapshots are append-only.  Activating a snapshot writes a
``PolicySnapshotActivated`` round event; it never mutates the snapshot row.
The same ``(policy_family, version)`` cannot be reused with different
content, and the same content hash cannot be registered under a different
``(policy_family, version)`` pair.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Mapping

import aiosqlite

from ..infrastructure.sqlite.migrations import apply_architecture_migrations
from .experiment_store import stable_hash, stable_json

logger = logging.getLogger(__name__)


# Fields that must never enter a manifest or its hash input.  The list is
# deliberately broad: any credential, API key, proxy password, OAuth token or
# secret value is stripped before hashing and before persistence.
SENSITIVE_KEY_FRAGMENTS = (
    "api_key",
    "apikey",
    "secret",
    "password",
    "passwd",
    "token",
    "credential",
    "proxy_password",
    "oauth",
    "private_key",
    "access_key",
)


def _is_sensitive_key(key: str) -> bool:
    lowered = str(key).lower()
    # These are generation/admission settings, not authentication tokens.
    # Dropping them would give different model budgets the same runtime ID.
    if lowered in {"max_tokens", "max_output_tokens", "max_completion_tokens",
                   "daily_token_budget", "token_budget"}:
        return False
    return any(fragment in lowered for fragment in SENSITIVE_KEY_FRAGMENTS)


def _strip_sensitive(value: Any) -> Any:
    """Recursively remove credential-like keys before hashing or persisting."""
    if isinstance(value, Mapping):
        return {
            str(k): _strip_sensitive(v)
            for k, v in value.items()
            if not _is_sensitive_key(str(k))
        }
    if isinstance(value, list | tuple):
        return [_strip_sensitive(item) for item in value]
    return value


@dataclass(frozen=True)
class PolicyManifest:
    """The version manifest for one policy family at one effective time."""

    policy_family: str
    version: str
    code_artifacts: dict[str, str] = field(default_factory=dict)
    effective_config: dict[str, Any] = field(default_factory=dict)
    prompt_versions: dict[str, dict[str, str]] = field(default_factory=dict)
    model_versions: dict[str, Any] = field(default_factory=dict)
    policy_module_versions: dict[str, str] = field(default_factory=dict)
    source_manifest_path: str | None = None
    effective_at: str = field(default_factory=lambda: datetime.now().isoformat())
    change_reason: str | None = None
    changes_behavior: bool = False

    def to_payload(self) -> dict[str, Any]:
        return {
            "policy_family": self.policy_family,
            "version": self.version,
            "code_artifacts": dict(self.code_artifacts),
            "effective_config": _strip_sensitive(dict(self.effective_config)),
            "prompt_versions": dict(self.prompt_versions),
            "model_versions": _strip_sensitive(dict(self.model_versions)),
            "policy_module_versions": dict(self.policy_module_versions),
            "source_manifest_path": self.source_manifest_path,
            "effective_at": self.effective_at,
            "change_reason": self.change_reason,
            "changes_behavior": bool(self.changes_behavior),
        }

    def content_hash(self) -> str:
        # Hash every field that describes behaviour, in a stable order.  The
        # effective_at timestamp and change_reason are intentionally excluded:
        # two snapshots with identical content but different activation times
        # are the same version, not two versions.
        hash_input = {
            "policy_family": self.policy_family,
            "version": self.version,
            "code_artifacts": dict(self.code_artifacts),
            "effective_config": _strip_sensitive(dict(self.effective_config)),
            "prompt_versions": dict(self.prompt_versions),
            "model_versions": _strip_sensitive(dict(self.model_versions)),
            "policy_module_versions": dict(self.policy_module_versions),
            "changes_behavior": bool(self.changes_behavior),
        }
        return stable_hash(hash_input)

    def with_content_version(self) -> PolicyManifest:
        """Give a runtime release an immutable version without manual relabeling.

        Explicit experiment versions still use ``register`` unchanged. Runtime
        identity includes all behaviour inputs, but not activation time, local
        paths, explanations, or credentials. The declared version is a namespace
        for this manifest format, rather than an alias for changing contents.
        """
        payload = self.to_payload()
        identity = {key: payload[key] for key in (
            "policy_family", "version", "code_artifacts", "effective_config",
            "prompt_versions", "model_versions", "policy_module_versions",
            "changes_behavior",
        )}
        return replace(self, version=f"{self.version}_{stable_hash(identity)}")


@dataclass(frozen=True)
class PersistedSnapshot:
    snapshot_id: str
    policy_family: str
    version: str
    content_hash: str
    changes_behavior: bool


class PolicySnapshotService:
    """Build and persist immutable policy snapshots."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    async def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as conn:
            await apply_architecture_migrations(conn)

    async def register(self, manifest: PolicyManifest) -> PersistedSnapshot:
        """Idempotently register a snapshot.

        Returns the existing row if ``(policy_family, version)`` already
        exists with the same content hash.  Raises ``ValueError`` if the same
        version has been registered before with different content, or if the
        same content hash is reused under a different version label.
        """
        await self.initialize()
        content_hash = manifest.content_hash()
        snapshot_id = (
            f"policy_snapshot_{stable_hash([manifest.policy_family, manifest.version, content_hash])[:32]}"
        )
        payload = manifest.to_payload()
        now = datetime.now().isoformat()
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("PRAGMA foreign_keys = ON")
            await conn.execute("PRAGMA busy_timeout = 5000")
            await conn.execute("BEGIN IMMEDIATE")
            try:
                async with conn.execute(
                    """
                    SELECT id, content_hash, changes_behavior FROM policy_snapshots
                    WHERE policy_family = ? AND version = ?
                    """,
                    (manifest.policy_family, manifest.version),
                ) as cursor:
                    existing = await cursor.fetchone()
                if existing is not None:
                    existing_hash = str(existing[1] or "")
                    if existing_hash != content_hash:
                        raise ValueError(
                            "policy snapshot is immutable: "
                            f"({manifest.policy_family}, {manifest.version}) "
                            f"has content_hash={existing_hash or 'unverified legacy content'}, "
                            f"requested={content_hash}"
                        )
                    # Same content already registered; nothing to do.
                    await conn.commit()
                    return PersistedSnapshot(
                        snapshot_id=str(existing[0]),
                        policy_family=manifest.policy_family,
                        version=manifest.version,
                        content_hash=content_hash,
                        changes_behavior=bool(existing[2]),
                    )
                async with conn.execute(
                    """
                    SELECT policy_family, version FROM policy_snapshots
                    WHERE content_hash = ?
                    """,
                    (content_hash,),
                ) as cursor:
                    reused = await cursor.fetchone()
                if reused is not None:
                    other_family, other_version = reused
                    if (
                        str(other_family) != manifest.policy_family
                        or str(other_version) != manifest.version
                    ):
                        raise ValueError(
                            "content_hash cannot be reused under a different "
                            f"({manifest.policy_family}, {manifest.version}); "
                            f"already registered as "
                            f"({other_family}, {other_version})"
                        )
                await conn.execute(
                    """
                    INSERT INTO policy_snapshots (
                        id, policy_family, version, config_json, source,
                        active, created_at, content_hash, manifest_json,
                        effective_config_json, prompt_versions_json,
                        model_versions_json, policy_module_versions_json,
                        source_manifest_path, effective_at, change_reason,
                        changes_behavior
                    ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot_id,
                        manifest.policy_family,
                        manifest.version,
                        stable_json(payload["effective_config"]),
                        manifest.source_manifest_path or "policy_snapshot_service",
                        now,
                        content_hash,
                        stable_json(payload),
                        stable_json(payload["effective_config"]),
                        stable_json(payload["prompt_versions"]),
                        stable_json(payload["model_versions"]),
                        stable_json(payload["policy_module_versions"]),
                        manifest.source_manifest_path,
                        manifest.effective_at,
                        manifest.change_reason,
                        1 if manifest.changes_behavior else 0,
                    ),
                )
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise
        return PersistedSnapshot(
            snapshot_id=snapshot_id,
            policy_family=manifest.policy_family,
            version=manifest.version,
            content_hash=content_hash,
            changes_behavior=manifest.changes_behavior,
        )

    async def resolve_id(
        self,
        policy_family: str,
        version: str,
    ) -> str | None:
        """Return the snapshot id for a (family, version) pair, or None."""
        if not self.db_path.exists():
            return None
        await self.initialize()
        async with aiosqlite.connect(self.db_path) as conn:
            async with conn.execute(
                """
                SELECT id FROM policy_snapshots
                WHERE policy_family = ? AND version = ?
                """,
                (policy_family, version),
            ) as cursor:
                row = await cursor.fetchone()
        return str(row[0]) if row else None

    async def content_hash_for(
        self,
        policy_family: str,
        version: str,
    ) -> str | None:
        if not self.db_path.exists():
            return None
        await self.initialize()
        async with aiosqlite.connect(self.db_path) as conn:
            async with conn.execute(
                """
                SELECT content_hash FROM policy_snapshots
                WHERE policy_family = ? AND version = ?
                """,
                (policy_family, version),
            ) as cursor:
                row = await cursor.fetchone()
        return str(row[0]) if row else None


def hash_file(path: Path) -> str:
    """Stable SHA-256 of a source file used by decision code."""
    hasher = hashlib.sha256()
    hasher.update(Path(path).read_bytes())
    return hasher.hexdigest()


def runtime_code_artifacts(repo_root: Path, entrypoint: str) -> dict[str, str]:
    """Cover the canonical pipeline, including research and memory components.

    A selected execution-file list misses independent changes to retrieval,
    model clients, committee prompts, storage, and feedback. Tests, runs,
    runtime data and private config files are excluded; the effective config
    is separately sanitized by ``PolicyManifest``.
    """
    root = Path(repo_root).resolve()
    main = root / entrypoint
    if not main.is_file() or not main.resolve().is_relative_to(root):
        raise ValueError(f"runtime entrypoint unavailable: {entrypoint}")
    paths = {main}
    for filename in ("__init__.py", "sovereign_hall/__init__.py"):
        if (root / filename).is_file():
            paths.add(root / filename)
    for directory in ("agents", "application", "core", "domain",
                      "infrastructure", "ports", "services", "utils"):
        paths.update((root / directory).rglob("*.py"))
    paths.update(path for path in (root / "services" / "calendar_data").glob("*")
                 if path.is_file() and path.suffix in {".json", ".html"})
    return {str(path.relative_to(root)): hash_file(path) for path in sorted(paths)}


def hash_code_artifacts(paths: list[Path]) -> dict[str, str]:
    """Map each decision-code path to its content hash."""
    artifacts: dict[str, str] = {}
    for path in paths:
        resolved = Path(path).resolve()
        if not resolved.exists():
            continue
        artifacts[str(resolved.relative_to(Path.cwd()) if resolved.is_relative_to(Path.cwd()) else resolved)] = (
            hash_file(resolved)
        )
    return artifacts
