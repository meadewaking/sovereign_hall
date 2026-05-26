"""Best heuristic policy snapshot from the latest local cycle.

This file is generated for reproducibility. It does not place orders and does
not call market data services.
"""

POLICY_CONFIG = {
    "name": "combined_guarded_policy",
    "min_confidence": 0.66,
    "max_names": 5,
    "max_position": 0.1,
    "max_gross": 0.55,
    "min_risk_reward": 0.9,
    "trend_lookback": 2,
    "require_positive_trend": true,
    "vol_lookback": 3,
    "high_vol_threshold": 0.05,
    "high_vol_scale": 0.55,
    "anomaly_return_threshold": 0.14,
    "use_anomaly_veto": true,
    "drawdown_guard": 0.5,
    "drawdown_guard_threshold": 0.025,
    "rebalance_threshold": 0.02,
    "min_signal_count": 1,
    "max_stop_gap": 0.55
}

COST_CONFIG = {
    "trading_fee": 0.0003,
    "stamp_duty": 0.001,
    "slippage": 0.0005
}


def score_candidate(row):
    """Return an interpretable ranking score for one daily signal row."""
    risk_reward = min(max(float(row.get("risk_reward", 0.0)), 0.0), 3.0)
    observations = max(int(row.get("close_observations", 0)), 0)
    confidence = float(row.get("confidence", 0.0))
    return confidence * __import__("math").log1p(observations) * (1.0 + risk_reward / 6.0)
