"""
🏛️ Sovereign Hall - Services
服务模块导出

注意：部分模块存在循环导入问题，使用延迟导入
"""

from .llm_client import LLMClient
from .spider_service import SpiderSwarm
from .vector_db import VectorDatabase
from .database import DatabaseService
from .db_inspector import get_db_stats
from .db_viewer import get_db_stats as get_db_stats_v2
from .research_discussion import ResearchDiscussionSystem
from .investment_simulation import InvestmentSimulation, show_investment_status, run_daily_simulation

# 延迟导入，避免循环导入问题
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