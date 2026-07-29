"""Deprecated compatibility entry for the canonical discussion runner.

``python -m sovereign_hall.run_discussion`` is the supported production entry.
This module intentionally contains no second orchestration engine.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compatibility wrapper; delegates to sovereign_hall.run_discussion"
        )
    )
    parser.add_argument("--single", "-s", action="store_true")
    parser.add_argument("--iterations", "-i", type=int, default=0)
    parser.add_argument("--interval", "-t", type=int, default=1800)
    parser.add_argument("--config", "-c")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    parser.add_argument("--model")
    parser.add_argument("--api-url")
    parser.add_argument("--skip-preflight", action="store_true")
    parser.add_argument("--local-only", action="store_true")
    return parser.parse_args(argv)


async def main(argv: list[str] | None = None) -> None:
    """Translate legacy flags and execute the one canonical use case."""
    args = parse_args(argv)
    from sovereign_hall.core.config import get_config
    from sovereign_hall.run_discussion import main as canonical_main

    config = get_config()
    if args.config:
        config.load_from_file(args.config)
    if args.model:
        config.set("llm.model", args.model)
    if args.api_url:
        config.set("llm.base_url", args.api_url)

    translated: list[str] = []
    if args.single:
        translated.append("--once")
    if args.iterations:
        translated.extend(["--max-rounds", str(args.iterations)])
    if args.skip_preflight:
        translated.append("--skip-preflight")
    if args.local_only:
        translated.append("--local-only")

    if args.interval != 1800:
        logger.warning(
            "--interval is deprecated; canonical runner owns adaptive pacing"
        )

    previous_argv = sys.argv
    try:
        sys.argv = [previous_argv[0], *translated]
        await canonical_main()
    finally:
        sys.argv = previous_argv


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n🛑 用户中断，程序退出")
        raise SystemExit(130)
