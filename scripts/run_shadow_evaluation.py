#!/usr/bin/env python3
"""Run the two isolated, canonical shadow sleeves against validated local data."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
import json
from pathlib import Path
import sys

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sovereign_hall.domain.portfolio.costs import CostSchedule  # noqa: E402
from sovereign_hall.domain.portfolio.shadow import SleeveId  # noqa: E402
from sovereign_hall.ports.broker import assert_safe_execution_mode  # noqa: E402
from sovereign_hall.services.shadow_runner import ShadowEvaluationService  # noqa: E402


def _prune_old_runs(runs_root: Path, keep: int = 30) -> None:
    """删除最旧的 run 目录，只保留最近 keep 个。按目录名排序（格式为时间戳）。"""
    import shutil
    dirs = sorted(
        [p for p in runs_root.iterdir() if p.is_dir()],
        key=lambda p: p.name,
    )
    for old_dir in dirs[:-keep]:
        shutil.rmtree(old_dir, ignore_errors=True)


async def run(args: argparse.Namespace) -> int:
    config_path = (PROJECT_ROOT / args.config).resolve()
    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) if config_path.exists() else {}
    simulation = dict((config or {}).get("simulation") or {})
    shadow = dict((config or {}).get("shadow_evaluation") or {})
    assert_safe_execution_mode(str(shadow.get("execution_mode", "paper")))
    if float(shadow.get("initial_capital", 10000)) != 10000.0:
        raise ValueError("shadow evaluation is fixed to 10000 RMB")
    schedule = CostSchedule.from_mapping(simulation)
    db_path = (PROJECT_ROOT / args.db).resolve()
    output_root = (PROJECT_ROOT / args.output_root).resolve()
    run_id = args.timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    _prune_old_runs(output_root, keep=30)

    sleeve_values = (
        [SleeveId.SYSTEMATIC_ETF_MOMENTUM_V1, SleeveId.LLM_RESEARCH]
        if args.sleeve == "all"
        else [SleeveId(args.sleeve)]
    )
    service = ShadowEvaluationService(db_path, cost_schedule=schedule)
    summary: dict[str, object] = {
        "run_id": run_id,
        "database": str(db_path),
        "initial_capital": 10000,
        "cost_model_version": schedule.version,
        "small_account_cost_pressure": schedule.small_account_pressure(10000),
        "production_simulation_mutated": False,
        "sleeves": {},
    }
    lines = [
        "# 可信收益验证（影子模式）",
        "",
        "- 起始资金：10,000 元（唯一资本档位）",
        f"- 成本模型：`{schedule.version}`",
        "- 生产模拟账户：未合并资金、未生成成交、未修改现金或持仓",
        "",
    ]
    for sleeve in sleeve_values:
        outcome = await service.run(sleeve, persist=not args.no_persist)
        result = outcome.result
        result_path = run_dir / f"{sleeve.value}.json"
        result_path.write_text(
            json.dumps(result.as_dict(include_curve=True), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        metrics = result.metrics
        qualification = dict(metrics.get("qualification") or {})
        cagr = metrics.get("net_cagr")
        data_usable = bool((metrics.get("data_quality") or {}).get("usable"))
        sleeve_summary = {
            "status": result.status,
            "metrics_validity": "qualification_eligible" if data_usable else "diagnostic_only",
            "qualification_label": qualification.get("label", "未证明"),
            "sessions": metrics.get("sessions", 0),
            "total_return": metrics.get("total_return", 0.0),
            "net_cagr": cagr,
            "annualization_status": metrics.get("annualization_status"),
            "max_drawdown": metrics.get("max_drawdown"),
            "cost_paid": metrics.get("cost_paid", 0.0),
            "fill_count": metrics.get("fill_count", 0),
            "data_quality": metrics.get("data_quality"),
            "qualification_reasons": qualification.get("reasons", []),
            "data_hash": outcome.data_hash,
            "strategy_hash": result.spec.strategy_hash,
            "persisted_run_id": outcome.persisted_run_id,
            "strategy_trial_count": outcome.strategy_trial_count,
            "evaluation_run_count": outcome.evaluation_run_count,
            "artifact": str(result_path),
        }
        summary["sleeves"][sleeve.value] = sleeve_summary  # type: ignore[index]
        if cagr is None:
            cagr_text = "样本不足（不显示推断年化）"
        elif data_usable:
            cagr_text = f"{float(cagr):.2%}"
        else:
            cagr_text = f"{float(cagr):.2%}（仅诊断；数据不具备资格）"
        lines.extend(
            [
                f"## {sleeve.value}",
                "",
                f"- 结论：{qualification.get('label', '未证明')}",
                f"- 数据状态：{result.status}",
                f"- 指标用途：{'可进入资格判断' if data_usable else '仅诊断，不可作为收益证明'}",
                f"- 交易日样本：{metrics.get('sessions', 0)}",
                f"- 累计净收益：{float(metrics.get('total_return', 0.0)):.2%}",
                f"- 净 CAGR：{cagr_text}",
                f"- 最大回撤：{float(metrics.get('max_drawdown', 0.0)):.2%}",
                f"- 成本：{float(metrics.get('cost_paid', 0.0)):.2f} 元",
                f"- 独立策略试验数：{outcome.strategy_trial_count}",
                f"- 本袖套追加评估序号：{outcome.evaluation_run_count}",
                f"- 资格门未通过项：{', '.join(qualification.get('reasons', [])) or '无'}",
                "",
            ]
        )

    (run_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (run_dir / "README.md").write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run canonical 10,000 RMB shadow evaluations")
    parser.add_argument("--db", default="data/sovereign_hall.db")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--output-root", default="runs/shadow_evaluation")
    parser.add_argument("--timestamp", default=None)
    parser.add_argument(
        "--sleeve",
        choices=("all", SleeveId.SYSTEMATIC_ETF_MOMENTUM_V1.value, SleeveId.LLM_RESEARCH.value),
        default="all",
    )
    parser.add_argument("--no-persist", action="store_true", help="Write artifacts but do not append DB evaluation rows")
    return parser


def main() -> int:
    return asyncio.run(run(build_parser().parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
