# Heuristic Learning Cycle

## Run
- Run time: 20260526_101048
- Data source: `/Users/wangziming/PycharmProjects/PythonProject/sovereign_hall/data/sovereign_hall.db`
- Samples consumed: 155758 prediction rows
- Best policy: `combined_guarded_policy`
- Current best score: -0.375672
- Previous best comparison: Previous best score -0.375672 from /Users/wangziming/PycharmProjects/PythonProject/sovereign_hall/runs/heuristic_cycle/20260526_100908/summary.csv; delta -0.000000.

## What Changed
- Added a local-only delayed-signal heuristic evaluation loop for this cycle.
- Tested small interpretable changes: trend filtering, volatility scaling, anomaly veto, drawdown guard, and rebalance friction.
- Wrote the retained policy snapshot to `policy_snapshot.py`.

## Best Metrics
- Total return: 2.1902%
- Annualized return: 21.5298%
- Max drawdown: -1.4193%
- Sharpe: 2.879
- Sortino: 4.555
- Win rate: 46.67%
- Turnover: 6.753
- Trade count: 30
- Cost assumption: fee=0.0300%, stamp_duty=0.1000%, slippage=0.0500%, applied on turnover

## Failed Or Weaker Directions
- baseline_default_policy: score=-0.983930, notes=local delayed daily signal simulation; no external market data
- trend_filter: score=-0.463164, notes=local delayed daily signal simulation; no external market data
- volatility_scaled: score=-0.919882, notes=local delayed daily signal simulation; no external market data
- risk_agent_veto: score=-1.047403, notes=local delayed daily signal simulation; no external market data
- drawdown_rebalance_guard: score=-1.024993, notes=local delayed daily signal simulation; no external market data
- simplified_best_policy: score=-0.396021, notes=simplification stage: removed volatility scaling and excess anomaly tuning

## Overfitting Risk
```json
{
  "split_date": "2026-05-15",
  "train": {
    "sample_start": "2026-04-28",
    "sample_end": "2026-05-14",
    "days": 16,
    "total_return": -0.0071241359025840945,
    "annualized_return": -0.10649802257461649,
    "max_drawdown": -0.009361958754375066,
    "sharpe": -2.8873648040200934,
    "sortino": -2.0714630196652624,
    "win_rate": 0.35714285714285715,
    "turnover": 2.794492215112202,
    "trade_count": 14,
    "cost_paid": 0.0035122845895343147,
    "cost_assumption": "fee=0.0300%, stamp_duty=0.1000%, slippage=0.0500%, applied on turnover",
    "score": -0.29414050805255854
  },
  "out_of_sample": {
    "sample_start": "2026-05-14",
    "sample_end": "2026-05-26",
    "days": 12,
    "total_return": 0.02941409632057601,
    "annualized_return": 0.8381981751872472,
    "max_drawdown": -0.005518530751945194,
    "sharpe": 6.97195819980413,
    "sortino": 21.541569893520087,
    "win_rate": 0.5294117647058824,
    "turnover": 3.917119519440714,
    "trade_count": 17,
    "cost_paid": 0.004915318399969522,
    "cost_assumption": "fee=0.0300%, stamp_duty=0.1000%, slippage=0.0500%, applied on turnover",
    "score": 0.5388116394672336
  },
  "cost_stress_3x_slippage": {
    "sample_start": "2026-04-28",
    "sample_end": "2026-05-26",
    "days": 28,
    "total_return": 0.015030458588604967,
    "annualized_return": 0.14369881483192315,
    "max_drawdown": -0.015726501465664677,
    "sharpe": 1.9989606167147955,
    "sortino": 3.1427636143898257,
    "win_rate": 0.4666666666666667,
    "turnover": 6.75272231477601,
    "trade_count": 30,
    "cost_paid": 0.015354324348681421,
    "cost_assumption": "fee=0.0300%, stamp_duty=0.1000%, slippage=0.1500%, applied on turnover",
    "score": -0.4547909917271916
  },
  "overfit_risk": true
}
```

Flag: suspected overfit risk.

## Reproduce
```bash
python scripts/run_heuristic_cycle.py --db data/sovereign_hall.db
```

## Next 3 Directions
- Add a losing-streak cooldown and compare it against the drawdown guard.
- Require independent confirmation from simulation trade outcomes once validated prices are available.
- Build a small local leaderboard that separates ETF and single-stock universes before mixing them in one portfolio.
