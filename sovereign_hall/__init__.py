"""Compatibility package for running ``python -m sovereign_hall.*`` locally.

The repository's working directory is also the package directory. When Python
is launched from here, it expects a nested ``sovereign_hall`` package. This
shim points submodule discovery back to the real package files one level up.
"""

from __future__ import annotations

from pathlib import Path

_REAL_PACKAGE_DIR = Path(__file__).resolve().parents[1]
__path__ = [str(_REAL_PACKAGE_DIR)]
