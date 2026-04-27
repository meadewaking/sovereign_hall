#!/usr/bin/env python3
"""
🏛️ Sovereign Hall - 交互式研究讨论
功能：输入问题，多智能体讨论并生成投资建议，保存报告
用法：直接运行此脚本
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root.parent))

from sovereign_hall.services.research_discussion import ResearchDiscussionSystem


async def print_report(context):
    """打印研究报告"""
    print(f"\n{'#'*70}")
    print(f"# 研究报告")
    print(f"# 问题: {context.question}")
    print(f"{'#'*70}\n")

    print("="*70)
    print("📊 摘要")
    print("="*70)
    print(f"问题: {context.question}")
    print(f"本次讨论轮次: {len(context.discussion_history)} 轮")

    print("\n" + "="*70)
    print("🗣️  讨论过程")
    print("="*70)
    for i, view in enumerate(context.discussion_history, 1):
        print(f"\n【第{i}轮】")
        print(view[:8000] + "..." if len(view) > 8000 else view)

    print("\n" + "="*70)
    print("⚖️  最终结论")
    print("="*70)
    print(context.final_conclusion)

    print("\n" + "="*70)


async def main():
    print("\n" + "="*60)
    print("🏛️ Sovereign Hall - 交互式研究系统")
    print("="*60)
    print("输入问题开始讨论，直接回车使用默认问题")
    print("输入 'quit' 退出")
    print("="*60 + "\n")

    default_question = "从现在出发，找出目前a股中一支适合持有三个月到半年的股票，并预估他的止盈止损价格"

    while True:
        question = input("❓ 问题: ").strip()

        if not question:
            question = default_question
            print(f"   使用默认问题: {question[:50]}...")

        if question.lower() in ['quit', 'exit', 'q']:
            print("👋 再见！")
            break

        try:
            system = ResearchDiscussionSystem(
                enable_search=True,
                enable_web=False
            )
            context = await system.research(question)
            await print_report(context)

            # 保存到文件
            report_file = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(f"# 研究报告\n\n")
                f.write(f"**问题**: {context.question}\n\n")
                f.write(f"**时间**: {datetime.now().isoformat()}\n\n")
                f.write("---\n\n")
                f.write("## 结论\n\n")
                f.write(context.final_conclusion)
                f.write("\n\n---\n\n")
                f.write("## 讨论过程\n\n")
                for i, view in enumerate(context.discussion_history, 1):
                    f.write(f"### 第{i}轮\n\n")
                    f.write(view)
                    f.write("\n\n")

            print(f"\n💾 报告已保存到: {report_file}")

        except KeyboardInterrupt:
            print("\n\n🛑 用户中断")
        except Exception as e:
            print(f"\n❌ 错误: {e}")
            import traceback
            traceback.print_exc()

        print("\n" + "="*60)
        print("继续输入新问题，或 'quit' 退出")
        print("="*60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
