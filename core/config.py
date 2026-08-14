"""
🏛️ Sovereign Hall - Configuration Manager
配置管理模块
"""

import os
import json
import copy
from pathlib import Path
from typing import Any, Dict, Optional, Union
from dataclasses import dataclass, field
import logging
import yaml

from . import PROJECT_ROOT, DATA_DIR

logger = logging.getLogger(__name__)


class Config:
    """配置管理器"""

    _instance = None
    _lock = __import__('threading').Lock()
    _config: Dict[str, Any] = {}

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, "_loaded_files"):
            self._loaded_files: set[Path] = set()
        if not self._config:
            self.load_defaults()

    def load_defaults(self):
        """加载默认配置"""
        self._config = {
            # LLM配置
            'llm': {
                'provider': 'openai',
                'model': 'GLM-5.2-FP8',
                'api_key_env': 'OPENAI_API_KEY',
                'base_url': 'http://172.18.5.19:8000/v1',
                'model_uuid': '',
                'max_concurrent': 16,
                'temperature': 0.7,
                'max_tokens': 4000,
                'timeout': 120,
                'embedding_model': 'text-embedding-3-small',
                'embedding_dim': 1536,
                'embedding_input_per_1k': 0.0,
            },

            # 爬虫配置
            'spider': {
                'max_concurrent': 50,
                'timeout': 30,
                'user_agent': 'SovereignHall/1.0 (Research Bot)',
                'retry_times': 3,
                'retry_delay': 2,
                'respect_robots_txt': True,
                'requests_per_minute': 100,
                'burst': 20,
            },

            # 向量数据库
            'vector_db': {
                'provider': 'legacy',
                'dimension': 1536,
                'index_type': 'obsidian_wiki',
                'nlist': 100,
                'nprobe': 10,
                'metric': 'cosine',
            },

            # Obsidian-compatible LLM Wiki knowledge base
            'knowledge_wiki': {
                'root': str(DATA_DIR / 'knowledge'),
                'max_size_bytes': 1024 * 1024 * 1024,
                'target_size_ratio': 0.70,
                'maintenance_ratio': 0.80,
                'bulk_write_stop_ratio': 0.90,
                'source_excerpt_chars': 500,
                'aggregate_page_max_chars': 32768,
                'aggregate_source_limit': 50,
                'log_rotate_bytes': 8 * 1024 * 1024,
                'log_rotate_keep': 5,
                'raw_hot_retention_days': 30,
                'embedding_enabled': True,
                'lazy_migration_batch_size': 10,
                'min_wiki_hits': 5,
                'chunk_target_chars': 1000,
                'chunk_max_chars': 1500,
                'chunk_min_chars': 200,
                'chunk_overlap_chars': 200,
            },

            # 缓存配置
            'cache': {
                'enabled': True,
                'memory_max_size': 1000,
                'memory_ttl': 3600,
                'disk_enabled': True,
                'disk_path': './cache',
                'disk_ttl': 604800,
            },

            # 数据库配置
            'database': {
                'provider': 'sqlite',
                'sqlite_path': str(DATA_DIR / 'sovereign_hall.db'),
                'max_bundle_size_bytes': 2 * 1024 * 1024 * 1024,
                'max_main_file_bytes': 1984 * 1024 * 1024,
                'target_size_ratio': 0.70,
                'maintenance_ratio': 0.80,
                'bulk_write_stop_ratio': 0.90,
                'max_round_document_links': 300,
                'wal_autocheckpoint_pages': 1000,
                'journal_size_limit_bytes': 16 * 1024 * 1024,
                'online_maintenance_batch_rows': 1000,
                'snapshot_full_retention_days': 30,
                'snapshot_daily_retention_days': 180,
                'non_trade_quote_retention_days': 7,
                'knowledge_rescan_interval_rounds': 24,
                'postgresql_host': 'localhost',
                'postgresql_port': 5432,
                'postgresql_database': 'sovereign_hall',
                'postgresql_pool_size': 20,
            },

            # 系统运行配置
            'system': {
                'mode': 'production',
                'daily_token_budget': 0,
                'daily_budget_pause_seconds': 3600,
                'storage_pressure_pause_seconds': 60,
                'iteration_interval': 1800,
                'max_iterations': None,
                'checkpoint_interval': 10,
                'checkpoint_dir': str(DATA_DIR / 'checkpoints'),
                'keep_checkpoints': 5,
                'log_level': 'INFO',
                'log_format': 'text',
                'log_dir': str(DATA_DIR / 'logs'),
                'enable_rate_limiting': True,
                'enable_circuit_breaker': True,
                'circuit_breaker_failure_threshold': 5,
                'circuit_breaker_timeout': 60,
            },

            # 研究讨论配置
            'research': {
                'max_tokens': 15000,
                'reflection_limit': 5,
                'rounds': 6,
                'conclusion_max_length': 20000,
                'search_query_count': 30,
                'search_results_per_query': 10,
                'force_search_interval': 1,
                'stage2_max_docs': 30,
                'stage2_doc_chars': 1200,
                'stage2_context_chars': 24000,
                'agent_search_max_docs': 10,
                'agent_search_doc_chars': 1200,
                'committee_proposal_limit': 5,
                'committee_min_review_depth': 'focused',
                'committee_full_discussion': False,
                'committee_round1_max_tokens': 9000,
                'committee_round2_max_tokens': 7000,
                'committee_revision_max_tokens': 6000,
                'committee_vote_max_tokens': 3500,
                'committee_vote_retry_timeout_seconds': 60,
                'committee_vote_retry_max_tokens': 1800,
                'committee_summary_chars': 1200,
                'committee_vote_context_chars': 6000,
                'conclusion_discussion_context_chars': 24000,
            },

            # 投委会配置
            'investment_committee': {
                'max_rounds': 3,
                'quorum': 5,
                'approval_threshold': 0.6,
                'strong_buy_threshold': 0.8,
                'defer_threshold': 0.5,
                'max_tokens_per_meeting': 50000,
                'proposal_max_tokens': 6000,
                'challenge_max_tokens': 3000,
                'defense_max_tokens': 5000,
                'voting_weights': {
                    'cio': 2.0,
                    'risk_officer': 1.5,
                    'quant_researcher': 1.0,
                    'macro_strategist': 1.0,
                    'analyst': 1.0,
                },
            },

            # 分析师配置
            'analysts': {
                'enabled_roles': [
                    'tmt_analyst',
                    'consumer_analyst',
                    'cycle_analyst',
                    'macro_strategist',
                ],
                'sector_mapping': {
                    'TMT': 'tmt_analyst',
                    '科技': 'tmt_analyst',
                    '半导体': 'tmt_analyst',
                    'AI': 'tmt_analyst',
                    '消费': 'consumer_analyst',
                    '医药': 'consumer_analyst',
                    '白酒': 'consumer_analyst',
                    '周期': 'cycle_analyst',
                    '制造': 'cycle_analyst',
                    '化工': 'cycle_analyst',
                    '大宗商品': 'cycle_analyst',
                    '金融': 'macro_strategist',
                    '地产': 'macro_strategist',
                },
            },

            # 机构记忆配置
            'institutional_memory': {
                'playbook_path': str(DATA_DIR / 'playbooks'),
                'version_control': True,
                'min_outcome_samples': 3,
                'confidence_adjustment_factor': 0.1,
                'blacklist_threshold': 3,
                'blacklist_duration': 2592000,
                'retrieval_top_k': 5,
                'similarity_threshold': 0.7,
            },

            # 模拟投资硬边界。config.yaml may override economic parameters,
            # but runtime validation protects the product invariants.
            'simulation': {
                'enabled': True,
                'execution_mode': 'simulation',
                'initial_capital': 10000,
                'target_invested_ratio': 1.0,
                'realtime_quotes_required': True,
                'trade_during_market_hours_only': True,
                'max_realtime_quote_age_seconds': 120,
                'max_trade_price_age_days': 3,
                'stop_loss_pct': -0.08,
                'take_profit_pct': 0.15,
                'max_holding_days': 30,
                'max_single_position': 0.10,
                'max_gross': 1.0,
                'min_committee_confidence': 0.65,
                'min_risk_reward': 0.8,
                'min_holding_days': 10,
                'minimum_healthy_invested_ratio': 0.80,
                'no_trade_failure_days': 3,
                'min_unit': 100,
                'trading_fee': 0.0003,
                'minimum_commission': 5.0,
                'stamp_duty': 0.0005,
                'etf_stamp_duty': 0.0,
                'slippage_rate': 0.0005,
                'cost_model_version': 'cn_a_share_etf_v1_20260814',
                'max_daily_trades': 5,
            },

            # Read-only shadow evaluation.  It never shares cash or positions
            # with the authoritative simulation account.
            'shadow_evaluation': {
                'enabled': True,
                'execution_mode': 'paper',
                'initial_capital': 10000,
                'cagr_target': 0.15,
                'max_drawdown_budget': 0.15,
                'risk_reduction_drawdown': 0.10,
                'minimum_annualization_sessions': 252,
                'systematic_qualification_sessions': 756,
                'llm_qualification_sessions': 252,
                'llm_minimum_closed_trades': 100,
            },

            # 监控配置
            'monitoring': {
                'enabled': True,
                'prometheus_enabled': True,
                'prometheus_port': 9090,
                'scrape_interval': 15,
            },

            # 数据源配置
            'data_sources': {
                'news': [
                    {'provider': 'google_news', 'enabled': True, 'languages': ['zh', 'en']},
                ],
                'market_data': [
                    {'provider': 'yahoo_finance', 'enabled': True},
                ],
                'fundamentals': [
                    {'provider': 'sec_edgar', 'enabled': True},
                ],
            },

            # 输出配置
            'output': {
                'reports_dir': str(DATA_DIR / 'reports'),
                'minutes_dir': str(DATA_DIR / 'minutes'),
                'archive_enabled': True,
                'archive_retention_days': 180,
            },

            # 安全配置
            'security': {
                'use_env_vars': True,
                'enable_auth': False,
                'audit_log_enabled': True,
                'audit_log_path': './logs/audit.log',
            },

            # 开发配置
            'development': {
                'debug': False,
                'verbose': False,
                'use_mock_data': False,
                'mock_llm_responses': False,
                'enable_profiling': False,
                'profile_output': './profiles',
            },

            # Token计费配置（用于估算成本）
            # Local OpenAI-compatible endpoints can override pricing here when a billing rate is known.
            'pricing': {
                'anthropic': {
                    'input_per_1k': 0.0006,   # ¥1.20/百万 → ¥0.0006/千 (半价)
                    'output_per_1k': 0.0042,  # ¥8.40/百万 → ¥0.0042/千 (半价)
                },
                'openai': {
                    'MiniMax/MiniMax-M2.5': {'input_per_1k': 0.0006, 'output_per_1k': 0.0042},
                    'MiniMax-M2.7-INT8': {'input_per_1k': 0.0003, 'output_per_1k': 0.0012},
                    'GLM-5.2-FP8': {'input_per_1k': 0.0003, 'output_per_1k': 0.0012},
                    'gpt-4': {'input_per_1k': 0.03, 'output_per_1k': 0.06},
                    'gpt-4-turbo': {'input_per_1k': 0.01, 'output_per_1k': 0.03},
                    'gpt-3.5-turbo': {'input_per_1k': 0.0005, 'output_per_1k': 0.0015},
                },
                'local': {
                    'input_per_1k': 0.0,
                    'output_per_1k': 0.0,
                },
            },
        }

    def load_from_file(self, filepath: Union[str, Path], *, force: bool = False):
        """Load one configuration file at most once.

        Re-merging the same YAML file used to duplicate list-valued settings
        every time an LLM client was constructed.  Configuration is process
        state, so a resolved file path is now idempotent unless a caller
        explicitly requests a reload.
        """
        filepath = Path(filepath).expanduser().resolve()
        if not filepath.exists():
            logger.warning(f"Config file not found: {filepath}")
            return False
        if filepath in self._loaded_files and not force:
            return True

        try:
            if filepath.suffix == '.yaml' or filepath.suffix == '.yml':
                with open(filepath, 'r', encoding='utf-8') as f:
                    file_config = yaml.safe_load(f)
            elif filepath.suffix == '.json':
                with open(filepath, 'r', encoding='utf-8') as f:
                    file_config = json.load(f)
            else:
                logger.error(f"Unsupported config format: {filepath.suffix}")
                return False

            if file_config:
                self._merge_config(self._config, file_config)

            self._loaded_files.add(filepath)
            logger.info(f"Loaded config from: {filepath}")
            return True
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return False

    def _merge_config(self, base: Dict, update: Dict):
        """Deep-merge mappings while treating lists as complete values."""
        for key, value in update.items():
            if key in base:
                if isinstance(base[key], dict) and isinstance(value, dict):
                    self._merge_config(base[key], value)
                elif isinstance(base[key], list) and isinstance(value, list):
                    base[key] = copy.deepcopy(value)
                else:
                    base[key] = copy.deepcopy(value)
            else:
                base[key] = copy.deepcopy(value)

    def validate_runtime_invariants(self) -> list[str]:
        """Return configuration errors that would weaken product invariants."""
        errors: list[str] = []
        simulation = self.get("simulation", {}) or {}
        execution_mode = str(simulation.get("execution_mode", "simulation")).strip().lower()
        if execution_mode not in {"simulation", "paper"}:
            errors.append("simulation.execution_mode only permits simulation/paper; live is forbidden")
        if not bool(simulation.get("realtime_quotes_required", True)):
            errors.append("simulation.realtime_quotes_required must remain true")
        if not bool(simulation.get("trade_during_market_hours_only", True)):
            errors.append("simulation.trade_during_market_hours_only must remain true")
        if int(simulation.get("max_daily_trades", 5) or 0) != 5:
            errors.append("simulation.max_daily_trades must equal the shared hard limit 5")
        target = float(simulation.get("target_invested_ratio", 1.0) or 0.0)
        if target != 1.0:
            errors.append("simulation.target_invested_ratio must equal 1.0")
        if float(simulation.get("max_gross", 1.0) or 0.0) > 1.0:
            errors.append("simulation.max_gross cannot exceed 1.0")
        if float(simulation.get("trading_fee", 0.0) or 0.0) <= 0:
            errors.append("simulation.trading_fee must be positive")
        if float(simulation.get("minimum_commission", 0.0) or 0.0) <= 0:
            errors.append("simulation.minimum_commission must be positive")
        if float(simulation.get("stamp_duty", 0.0) or 0.0) <= 0:
            errors.append("simulation.stamp_duty must be positive")
        if float(simulation.get("slippage_rate", 0.0) or 0.0) <= 0:
            errors.append("simulation.slippage_rate must be positive")
        if int(simulation.get("initial_capital", 10000) or 0) != 10000:
            errors.append("simulation.initial_capital must remain 10000 for the current account")
        shadow = self.get("shadow_evaluation", {}) or {}
        shadow_mode = str(shadow.get("execution_mode", "paper")).strip().lower()
        if shadow_mode not in {"simulation", "paper"}:
            errors.append("shadow_evaluation.execution_mode only permits simulation/paper")
        if int(shadow.get("initial_capital", 10000) or 0) != 10000:
            errors.append("shadow_evaluation.initial_capital must equal 10000")
        quote_age = int(
            simulation.get("max_realtime_quote_age_seconds", 120) or 0
        )
        if not 0 < quote_age <= 300:
            errors.append(
                "simulation.max_realtime_quote_age_seconds must be in 1..300"
            )
        return errors

    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值，支持点号分隔的路径"""
        keys = key.split('.')
        value = self._config
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value

    def set(self, key: str, value: Any):
        """设置配置值"""
        keys = key.split('.')
        config = self._config
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        config[keys[-1]] = value

    def get_llm_config(self) -> Dict:
        """获取LLM配置"""
        return self._config.get('llm', {})

    def get_spider_config(self) -> Dict:
        """获取爬虫配置"""
        return self._config.get('spider', {})

    def get_vector_db_config(self) -> Dict:
        """获取向量数据库配置"""
        return self._config.get('vector_db', {})

    def get_ic_config(self) -> Dict:
        """获取投委会配置"""
        return self._config.get('investment_committee', {})

    def get_analyst_roles(self) -> list:
        """获取启用的分析师角色"""
        return self._config.get('analysts', {}).get('enabled_roles', [])

    def get_sector_mapping(self, sector: str) -> str:
        """获取行业到分析师角色的映射"""
        mapping = self._config.get('analysts', {}).get('sector_mapping', {})
        for key, role in mapping.items():
            if key in sector or sector in key:
                return role
        return 'tmt_analyst'  # 默认

    def get_api_key(self, key_env: str) -> Optional[str]:
        """获取API密钥"""
        if self._config.get('security', {}).get('use_env_vars', True):
            return os.environ.get(key_env)
        return None

    def get_pricing(self, provider: str = None, model: str = None) -> Dict:
        """获取计费配置"""
        if provider is None:
            provider = self._config.get('llm', {}).get('provider', 'anthropic')

        provider_pricing = self._config.get('pricing', {}).get(provider, {})

        if model and isinstance(provider_pricing, dict):
            model_pricing = provider_pricing.get(model, provider_pricing)

            if isinstance(model_pricing, dict) and 'input_per_1k' in model_pricing:
                return model_pricing

        return provider_pricing if isinstance(provider_pricing, dict) else {}

    def estimate_cost(self, prompt_tokens: int, completion_tokens: int, provider: str = None, model: str = None) -> float:
        """估算API成本（美元）"""
        pricing = self.get_pricing(provider, model)

        if not pricing:
            return 0.0

        input_cost = (prompt_tokens / 1000) * pricing.get('input_per_1k', 0)
        output_cost = (completion_tokens / 1000) * pricing.get('output_per_1k', 0)

        return input_cost + output_cost

    def to_dict(self) -> Dict:
        """获取完整配置（不包含敏感信息）"""
        config_copy = self._copy_without_sensitive()
        return config_copy

    def _copy_without_sensitive(self) -> Dict:
        """复制配置，不包含敏感信息"""
        import copy
        config = copy.deepcopy(self._config)

        # 移除API密钥
        sensitive_keys = ['api_key', 'api_key_env', 'password', 'password_env', 'secret']
        self._remove_sensitive(config, sensitive_keys)

        return config

    def _remove_sensitive(self, obj: Any, sensitive_keys: list):
        """递归移除敏感信息"""
        if isinstance(obj, dict):
            keys_to_remove = []
            for key in obj:
                if any(sk in key.lower() for sk in sensitive_keys):
                    keys_to_remove.append(key)
                else:
                    self._remove_sensitive(obj[key], sensitive_keys)
            for key in keys_to_remove:
                obj.pop(key, None)
        elif isinstance(obj, list):
            for item in obj:
                self._remove_sensitive(item, sensitive_keys)


# 全局配置实例
config = Config()


def get_config() -> Config:
    """Return the process configuration with one idempotent YAML load."""
    global config
    possible_paths = [
        Path(__file__).parent.parent.parent / "config.yaml",
        Path.cwd() / "config.yaml",
        Path(__file__).parent.parent / "config.yaml",
    ]
    for path in possible_paths:
        if path.exists():
            config.load_from_file(path)
            break
    return config
