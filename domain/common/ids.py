"""Stable opaque identifiers used by new architecture records."""

from __future__ import annotations

from uuid import uuid4


def new_id(prefix: str) -> str:
    normalized = "".join(ch for ch in str(prefix).lower() if ch.isalnum() or ch == "_")
    return f"{normalized or 'id'}_{uuid4().hex}"

