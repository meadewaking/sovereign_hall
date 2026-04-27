"""Service package exports.

Imports are intentionally lazy so importing one service does not require every
optional runtime dependency in the project.
"""


def __getattr__(name):
    if name == "LLMClient":
        from .llm_client import LLMClient
        return LLMClient
    if name == "SpiderSwarm":
        from .spider_service import SpiderSwarm
        return SpiderSwarm
    if name == "VectorDatabase":
        from .vector_db import VectorDatabase
        return VectorDatabase
    if name == "DatabaseService":
        from .database import DatabaseService
        return DatabaseService
    if name == "ResearchDiscussionSystem":
        from .research_discussion import ResearchDiscussionSystem
        return ResearchDiscussionSystem
    if name in {"InvestmentSimulation", "show_investment_status", "run_daily_simulation"}:
        from .investment_simulation import InvestmentSimulation, show_investment_status, run_daily_simulation
        return {
            "InvestmentSimulation": InvestmentSimulation,
            "show_investment_status": show_investment_status,
            "run_daily_simulation": run_daily_simulation,
        }[name]
    if name == "get_db_stats":
        from .db_inspector import get_db_stats
        return get_db_stats
    if name == "get_db_stats_v2":
        from .db_viewer import get_db_stats as get_db_stats_v2
        return get_db_stats_v2
    raise AttributeError(name)


def get_investment_committee():
    from .investment_committee import InvestmentCommittee
    return InvestmentCommittee


def get_research_discussion():
    from .research_discussion import ResearchDiscussionSystem
    return ResearchDiscussionSystem


def get_investment_simulation():
    from .investment_simulation import InvestmentSimulation
    return InvestmentSimulation

__all__ = [
    'LLMClient',
    'SpiderSwarm',
    'VectorDatabase',
    'DatabaseService',
    'get_db_stats',
    'get_db_stats_v2',
    'ResearchDiscussionSystem',
    'get_investment_committee',
    'InvestmentSimulation',
    'show_investment_status',
    'run_daily_simulation',
]
