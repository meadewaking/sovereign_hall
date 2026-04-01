"""
🏛️ Sovereign Hall - Entry Point
君临殿入口文件
"""

import asyncio
import argparse
import logging
import sys
from pathlib import Path
from typing import Dict

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sovereign_hall.core.sovereign_hall import SovereignHall
from sovereign_hall.core.config import get_config
from sovereign_hall.utils import setup_logging

logger = logging.getLogger(__name__)


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="🏛️ Sovereign Hall - 全自动化多智能体投资研究系统"
    )

    parser.add_argument(
        '--single', '-s',
        action='store_true',
        help='运行单次迭代后退出'
    )

    parser.add_argument(
        '--iterations', '-i',
        type=int,
        default=0,
        help='运行轮数（0=无限，默认0）'
    )

    parser.add_argument(
        '--interval', '-t',
        type=int,
        default=1800,
        help='每轮间隔秒数（默认1800=30分钟）'
    )

    parser.add_argument(
        '--config', '-c',
        type=str,
        help='配置文件路径'
    )

    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='日志级别'
    )

    parser.add_argument(
        '--model',
        type=str,
        help='使用的模型名称'
    )

    parser.add_argument(
        '--api-url',
        type=str,
        help='API基础URL'
    )

    return parser.parse_args()


async def main():
    """主函数"""
    args = parse_args()

    # 加载配置（如果有指定）
    config = get_config()
    if args.config:
        config.load_from_file(args.config)

    # 构建配置
    run_config = {}

    if args.log_level:
        run_config['log_level'] = args.log_level

    if args.model:
        run_config['model'] = args.model

    if args.api_url:
        run_config['base_url'] = args.api_url

    # 初始化系统
    hall = SovereignHall(config=run_config)

    # 运行
    if args.single:
        # 单次运行
        success = await hall.run_single_iteration()
        db_stats = await hall.db.get_stats_summary()
        hall._print_final_stats(db_stats)
        await hall.shutdown()
        sys.exit(0 if success else 1)
    else:
        # 持续运行（无限）
        await hall.run_continuous(
            iterations=args.iterations,
            interval=args.interval,
        )


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n🛑 用户中断，程序退出")
        sys.exit(0)
    except Exception as e:
        logger.error(f"程序异常退出: {e}", exc_info=True)
        sys.exit(1)