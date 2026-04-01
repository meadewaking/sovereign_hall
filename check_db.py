#!/usr/bin/env python3
"""
🏛️ Sovereign Hall - 数据库统计查看
功能：查看数据库统计，并可选择浏览内容或进行讨论
用法：直接运行此脚本
"""

import sys
import os
import sqlite3
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


def format_size(size_bytes: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"


def show_investment_status(db_path):
    """显示投资模拟状态"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # 检查表是否存在
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='simulation_positions'")
    if not c.fetchone():
        print("\n" + "="*60)
        print("📊 投资模拟状态")
        print("="*60)
        print("   初始资金: 10,000.00 元")
        print("   当前资产: 10,000.00 元")
        print("   📈 盈亏: +0.00 元 (+0.00%)")
        print("   现金: 10,000.00 元")
        print("\n   📦 当前持仓:")
        print("   (空仓)")
        print("\n   📜 最近交易:")
        print("   (无交易记录)")
        conn.close()
        return

    # 获取初始资金
    c.execute("SELECT value FROM system_stats WHERE key = 'simulation_cash'")
    cash_row = c.fetchone()
    initial_capital = 10000
    cash = float(cash_row[0]) if cash_row else initial_capital

    # 获取持仓
    try:
        c.execute("SELECT ticker, shares, avg_cost FROM simulation_positions")
        positions = c.fetchall()
    except:
        positions = []

    # 获取最近交易
    try:
        c.execute("""
            SELECT ticker, direction, shares, price, reason, traded_at
            FROM simulation_trades
            ORDER BY traded_at DESC LIMIT 10
        """)
        trades = c.fetchall()
    except:
        trades = []

    conn.close()

    # 计算当前资产（简化版，需要真实价格）
    total_value = cash
    position_details = []
    for pos in positions:
        # 简化：使用成本价估算
        position_value = pos[1] * pos[2]
        total_value += position_value
        position_details.append(f"  {pos[0]}: {pos[1]}股 @ {pos[2]:.2f}")

    profit = total_value - initial_capital
    profit_pct = (profit / initial_capital) * 100

    print("\n" + "="*60)
    print("📊 投资模拟状态")
    print("="*60)
    print(f"   初始资金: {initial_capital:.2f} 元")
    print(f"   当前资产: {total_value:.2f} 元")
    if profit >= 0:
        print(f"   📈 盈亏: +{profit:.2f} 元 ({profit_pct:+.2f}%)")
    else:
        print(f"   📉 盈亏: {profit:.2f} 元 ({profit_pct:+.2f}%)")
    print(f"   现金: {cash:.2f} 元")

    print(f"\n   📦 当前持仓:")
    if position_details:
        for pd in position_details:
            print(pd)
    else:
        print("   (空仓)")

    print(f"\n   📜 最近交易:")
    if trades:
        for trade in trades:
            print(f"   {trade[5][:10]} {trade[1]} {trade[0]} {trade[2]}股 @ {trade[3]:.2f}")
    else:
        print("   (无交易记录)")


def show_stats(db_path):
    """显示数据库统计"""
    print("\n" + "="*60)
    print("📊 Sovereign Hall - 数据库统计")
    print("="*60)

    # 先显示投资状态
    show_investment_status(db_path)

    print(f"\n   数据库: {db_path.name}")
    print(f"   大小: {format_size(os.path.getsize(db_path))}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = [row[0] for row in c.fetchall()]

    print(f"\n   📋 数据表:")
    for i, table in enumerate(tables, 1):
        try:
            c.execute(f"SELECT COUNT(*) FROM {table}")
            count = c.fetchone()[0]
            print(f"      {i}. {table}: {count:,} 条")
        except:
            print(f"      {i}. {table}: (无法读取)")

    c.execute("SELECT COUNT(*) FROM report_conclusions")
    rc_count = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM reflection_summary")
    rs_count = c.fetchone()[0]

    print(f"\n   📈 研究讨论统计:")
    print(f"      - 讨论结论: {rc_count} 条")
    print(f"      - 反思总结: {rs_count} 条")

    conn.close()
    return tables


def browse_table(db_path, table_name):
    """浏览表内容"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute(f"SELECT COUNT(*) FROM {table_name}")
    total = c.fetchone()[0]

    print(f"\n   📄 {table_name} (共 {total:,} 条)")

    limit = 3
    offset = 0

    while True:
        c.execute(f"SELECT * FROM {table_name} ORDER BY ROWID LIMIT ? OFFSET ?", (limit, offset))
        rows = c.fetchall()

        if not rows:
            break

        for row in rows:
            print(f"\n   {'-'*40}")
            row_dict = dict(row)
            for key, val in row_dict.items():
                if val is None:
                    continue
                val_str = str(val)
                if len(val_str) > 100:
                    val_str = val_str[:100] + "..."
                print(f"   {key}: {val_str}")

        offset += limit
        more = input(f"\n   显示更多 {limit} 条? (y/n): ").strip().lower()
        if more != 'y':
            break

    conn.close()


def show_recent_conclusions(db_path, limit=5):
    """显示最近的讨论结论"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("""SELECT * FROM report_conclusions ORDER BY created_at DESC LIMIT ?""", (limit,))
    rows = c.fetchall()

    if not rows:
        print("\n   暂无讨论结论")
        conn.close()
        return

    print(f"\n   📋 最近 {len(rows)} 条讨论结论:")
    for i, row in enumerate(rows, 1):
        print(f"\n   【{i}】{row['ticker'] or 'N/A'} | {row['direction'] or 'N/A'} | 置信度: {row['confidence']:.0%}")
        print(f"   时间: {row['created_at'][:19]}")
        conclusion = row['conclusion'][:200] + "..." if row['conclusion'] and len(row['conclusion']) > 200 else row['conclusion'] or ""
        print(f"   结论: {conclusion}")

    conn.close()


def generate_topic_from_db(db_path) -> str:
    """从数据库动态生成研究议题"""
    import random

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # 1. 获取最近的提案
    c.execute("SELECT ticker, direction, thesis FROM proposals ORDER BY created_at DESC LIMIT 20")
    proposals = c.fetchall()

    # 2. 获取最近的文档
    c.execute("SELECT title, sector FROM documents ORDER BY crawled_at DESC LIMIT 10")
    docs = c.fetchall()

    # 3. 获取最近的讨论结论
    c.execute("SELECT conclusion, ticker FROM report_conclusions ORDER BY created_at DESC LIMIT 5")
    conclusions = c.fetchall()

    conn.close()

    # 从提案中随机选择一个有投资价值的
    if proposals:
        # 选择有明确方向的提案
        valid_proposals = [p for p in proposals if p['direction'] and p['ticker']]
        if valid_proposals:
            prop = random.choice(valid_proposals)
            topic = f"分析 {prop['ticker']} 的投资价值，当前方向: {prop['direction']}"
            thesis_preview = prop['thesis'][:100] if prop['thesis'] else ""
            if thesis_preview:
                topic += f"，参考逻辑: {thesis_preview}..."
            return topic

    # 从文档中选择
    if docs:
        doc = random.choice(docs)
        if doc['sector']:
            return f"{doc['sector']}行业近期动态分析"
        return f"{doc['title'][:30]}相关投资机会"

    # 默认议题
    return "A股市场近期走势与投资机会"


def run_discussion_once(db_path):
    """运行一次讨论"""
    import asyncio

    from sovereign_hall.services.research_discussion import ResearchDiscussionSystem

    # 生成议题
    topic = generate_topic_from_db(db_path)
    print(f"\n   🎯 生成议题: {topic}")

    async def do_research():
        system = ResearchDiscussionSystem(
            enable_search=False,
            enable_web=False
        )
        context = await system.research(topic)
        return context

    try:
        context = asyncio.run(do_research())
        print(f"\n✅ 讨论完成！结论已保存到数据库")
        return True
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        return False


def clean_database(db_path):
    """清洗数据库 - 删除无实际内容的文档和提案"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    print("\n" + "="*60)
    print("🧹 数据库清洗")
    print("="*60)

    # 1. 统计当前情况
    c.execute("SELECT COUNT(*) FROM documents")
    total_docs = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM proposals")
    total_proposals = c.fetchone()[0]

    print(f"清洗前:")
    print(f"  文档: {total_docs} 条")
    print(f"  提案: {total_proposals} 条")

    # 2. 定义需要删除的内容模式
    delete_patterns = [
        "Detailed content",
        "测试文档",
        "Mock",
        "Example domain",
        "占位符",
    ]

    placeholders = [
        "Detailed content about",
        "This is a test",
        "测试标题",
    ]

    # 3. 删除文档
    conditions = []
    for pattern in delete_patterns:
        conditions.append(f"content LIKE '%{pattern}%'")
    for ph in placeholders:
        conditions.append(f"title LIKE '%{ph}%'")
        conditions.append(f"content LIKE '%{ph}%'")

    # 内容过短
    conditions.append("length(content) < 50")
    # URL 为空
    conditions.append("url IS NULL OR url = ''")
    # 来源为 mock
    conditions.append("source LIKE '%mock%' OR source = 'MockSource'")

    where_clause = " OR ".join(conditions)
    where_clause = f"({where_clause})"

    # 获取要删除的文档ID
    c.execute(f"SELECT id, title FROM documents WHERE {where_clause}")
    to_delete_docs = c.fetchall()
    doc_ids_to_delete = [row[0] for row in to_delete_docs]

    if doc_ids_to_delete:
        print(f"\n将删除 {len(doc_ids_to_delete)} 条无效文档:")
        for row in to_delete_docs[:10]:
            print(f"  - {row[1][:50]}...")
        if len(doc_ids_to_delete) > 10:
            print(f"  ... 共 {len(doc_ids_to_delete)} 条")

        c.execute(f"DELETE FROM documents WHERE id IN ({','.join(['?' for _ in doc_ids_to_delete])})", doc_ids_to_delete)
        deleted_docs = c.rowcount
    else:
        deleted_docs = 0
        print("\n✅ 没有需要删除的文档")

    # 4. 删除无效提案 (proposals表主键是 proposal_id)
    c.execute("SELECT proposal_id, ticker FROM proposals WHERE ticker IS NULL OR ticker = '' OR ticker LIKE '%test%' OR ticker LIKE '%mock%' OR ticker LIKE '%BASKET'")
    to_delete_proposals = c.fetchall()
    proposal_ids_to_delete = [row[0] for row in to_delete_proposals]

    if proposal_ids_to_delete:
        c.execute(f"DELETE FROM proposals WHERE proposal_id IN ({','.join(['?' for _ in proposal_ids_to_delete])})", proposal_ids_to_delete)
        deleted_proposals = c.rowcount
    else:
        deleted_proposals = 0

    # 5. 删除没有有效ticker的提案
    c.execute("SELECT proposal_id FROM proposals WHERE ticker IS NULL OR TRIM(ticker) = '' OR length(ticker) < 2 OR ticker = 'NULL'")
    empty_ticker = [row[0] for row in c.fetchall()]
    if empty_ticker:
        c.execute(f"DELETE FROM proposals WHERE proposal_id IN ({','.join(['?' for _ in empty_ticker])})", empty_ticker)
        deleted_proposals += c.rowcount

    conn.commit()

    # 6. 显示清洗后结果
    c.execute("SELECT COUNT(*) FROM documents")
    after_docs = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM proposals")
    after_proposals = c.fetchone()[0]

    print(f"\n清洗后:")
    print(f"  文档: {after_docs} 条 (删除 {deleted_docs} 条)")
    print(f"  提案: {after_proposals} 条 (删除 {deleted_proposals} 条)")

    conn.close()
    return deleted_docs, deleted_proposals


def main():
    db_path = project_root / "data" / "sovereign_hall.db"

    if not db_path.exists():
        print(f"❌ 数据库不存在: {db_path}")
        return

    tables = show_stats(db_path)

    print("\n" + "="*60)
    print("请选择操作:")
    print("   1. 查看统计数据")
    print("   2. 浏览数据表内容")
    print("   3. 查看最近讨论结论")
    print("   4. 运行一次讨论（动态生成议题）")
    print("   5. 🧹 清洗数据库（删除无效数据）")
    print("   q. 退出")
    print("="*60)

    while True:
        choice = input("\n👉 请选择 (1/2/3/4/5/q): ").strip().lower()

        if choice == '1':
            show_stats(db_path)
        elif choice == '2':
            print(f"\n   可浏览的表: {', '.join(tables)}")
            table = input("   输入表名: ").strip()
            if table in tables:
                browse_table(db_path, table)
            else:
                print(f"   表 '{table}' 不存在")
        elif choice == '3':
            show_recent_conclusions(db_path)
        elif choice == '4':
            run_discussion_once(db_path)
        elif choice == '5':
            clean_database(db_path)
        elif choice == 'q':
            print("👋 再见！")
            break
        else:
            print("   无效选择")

        print("\n" + "-"*60)


if __name__ == "__main__":
    main()