#!/usr/bin/env python3
"""
数据库检查器 - 查看 Sovereign Hall 数据库统计和内容切片
增强版：修复表名问题，提供更详细的统计和切片功能
"""

import sqlite3
import os
import sys
from datetime import datetime

DB_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "sovereign_hall.db")


def format_size(size_bytes: int) -> str:
    """格式化文件大小"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"


def get_db_stats():
    """获取数据库统计"""
    if not os.path.exists(DB_FILE):
        print(f"\n❌ 数据库不存在: {DB_FILE}")
        return

    size = os.path.getsize(DB_FILE)

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # 获取所有表
    c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in c.fetchall()]

    print(f"\n{'='*60}")
    print(f"📊 数据库统计")
    print(f"{'='*60}")
    print(f"  文件: {DB_FILE}")
    print(f"  大小: {format_size(size)}")
    print(f"  包含表: {', '.join(tables)}")

    if not tables:
        print("  ⚠️ 数据库中没有表")
        conn.close()
        print(f"{'='*60}\n")
        return

    # 各表统计
    total_rows = 0
    for table in tables:
        try:
            c.execute(f"SELECT COUNT(*) FROM {table}")
            count = c.fetchone()[0]
            total_rows += count

            # 获取表大小估算
            c.execute(f"SELECT * FROM {table} LIMIT 1")
            columns = [description[0] for description in c.description]
            print(f"  [✓] {table}: {count:,} 条记录, {len(columns)} 列")
        except sqlite3.OperationalError as e:
            print(f"  [✗] {table}: 无法读取 - {e}")

    print(f"  总记录数: {total_rows:,}")

    # 最后更新时间
    print(f"\n  🕐 各表最后更新时间:")
    for table in tables:
        try:
            # 尝试不同的可能的时间字段
            time_fields = ['created_at', 'updated_at', 'archived_at', 'crawled_at']
            last_time = None
            for field in time_fields:
                c.execute(f"SELECT MAX({field}) FROM {table}")
                result = c.fetchone()[0]
                if result:
                    last_time = result
                    break

            if last_time:
                print(f"    - {table}: {last_time[:19]}")
            else:
                print(f"    - {table}: 无时间信息")
        except Exception:
            print(f"    - {table}: 无法获取")

    conn.close()
    print(f"{'='*60}\n")


def get_table_info():
    """获取表结构信息"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in c.fetchall()]

    print(f"\n{'='*60}")
    print(f"📋 表结构详情")
    print(f"{'='*60}")

    for table in tables:
        print(f"\n  📌 {table}")
        c.execute(f"PRAGMA table_info({table})")
        columns = c.fetchall()
        for col in columns:
            pk = " [主键]" if col[5] else ""
            print(f"     - {col[1]}: {col[2]}{pk}")

    conn.close()
    print(f"\n{'='*60}\n")


def show_content(table: str, limit: int = 10, offset: int = 0):
    """显示表内容切片"""
    if not os.path.exists(DB_FILE):
        print(f"❌ 数据库不存在: {DB_FILE}")
        return

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # 检查表是否存在
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    if not c.fetchone():
        print(f"\n❌ 表 '{table}' 不存在")
        # 显示可用表
        c.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in c.fetchall()]
        print(f"可用表: {', '.join(tables)}")
        conn.close()
        return

    # 获取记录数
    c.execute(f"SELECT COUNT(*) FROM {table}")
    total = c.fetchone()[0]

    # 获取数据
    c.execute(f"SELECT * FROM {table} ORDER BY ROWID LIMIT ? OFFSET ?", (limit, offset))
    rows = c.fetchall()

    # 获取列名
    c.execute(f"PRAGMA table_info({table})")
    columns = [row[1] for row in c.fetchall()]

    print(f"\n{'='*60}")
    print(f"📄 {table} 内容 (显示 {offset}-{offset+limit}/{total} 条)")
    print(f"{'='*60}")

    if not rows:
        print("  (空表)")
    else:
        for i, row in enumerate(rows):
            print(f"\n  [#{offset+i+1}]")
            for j, col in enumerate(columns):
                val = row[j]
                if val is None:
                    continue
                # 截断长文本
                val_str = str(val)
                if len(val_str) > 200:
                    val_str = val_str[:200] + "... [截断]"
                print(f"    {col}: {val_str}")

    conn.close()
    print(f"\n{'='*60}\n")


def show_documents(limit: int = 10, offset: int = 0):
    """显示文档列表 - 修复表名问题"""
    if not os.path.exists(DB_FILE):
        print(f"❌ 数据库不存在: {DB_FILE}")
        return

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # 检查实际表名
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%doc%'")
    tables = [row[0] for row in c.fetchall()]

    if not tables:
        print("  ⚠️ 没有找到文档表")
        conn.close()
        return

    table = tables[0]  # 使用第一个匹配的表

    c.execute(f"SELECT COUNT(*) FROM {table}")
    total = c.fetchone()[0]

    c.execute(f"""
        SELECT id, title, source, sector, substr(content, 1, 150) as preview
        FROM {table}
        ORDER BY crawled_at DESC
        LIMIT ? OFFSET ?
    """, (limit, offset))

    print(f"\n{'='*60}")
    print(f"📄 文档列表 (显示 {offset}-{min(offset+limit, total)}/{total} 条)")
    print(f"{'='*60}")

    for row in c.fetchall():
        print(f"\n  ID: {row[0][:20]}...")
        print(f"  标题: {row[1]}")
        print(f"  来源: {row[2]} | 行业: {row[3]}")
        print(f"  预览: {row[4]}...")

    conn.close()
    print(f"\n{'='*60}\n")


def show_proposals(limit: int = 10, offset: int = 0):
    """显示提案列表 - 修复表名问题"""
    if not os.path.exists(DB_FILE):
        print(f"❌ 数据库不存在: {DB_FILE}")
        return

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # 检查实际表名
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%prop%'")
    tables = [row[0] for row in c.fetchall()]

    if not tables:
        print("  ⚠️ 没有找到提案表")
        conn.close()
        return

    table = tables[0]

    c.execute(f"SELECT COUNT(*) FROM {table}")
    total = c.fetchone()[0]

    c.execute(f"""
        SELECT proposal_id, ticker, direction, confidence, substr(thesis, 1, 100) as thesis_preview
        FROM {table}
        ORDER BY created_at DESC
        LIMIT ? OFFSET ?
    """, (limit, offset))

    print(f"\n{'='*60}")
    print(f"📋 提案列表 (显示 {offset}-{min(offset+limit, total)}/{total} 条)")
    print(f"{'='*60}")

    for row in c.fetchall():
        print(f"\n  标的: {row[1]} | 方向: {row[2]} | 置信度: {row[3]:.0%}" if row[3] else f"\n  标的: {row[1]} | 方向: {row[2]}")
        print(f"  摘要: {row[4]}...")

    conn.close()
    print(f"\n{'='*60}\n")


def show_meetings(limit: int = 10, offset: int = 0):
    """显示会议列表 - 修复表名问题"""
    if not os.path.exists(DB_FILE):
        print(f"❌ 数据库不存在: {DB_FILE}")
        return

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # 检查实际表名 (可能是 meetings 或 meeting_minutes)
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE '%meet%' OR name LIKE '%minute%')")
    tables = [row[0] for row in c.fetchall()]

    if not tables:
        print("  ⚠️ 没有找到会议表")
        conn.close()
        return

    table = tables[0]

    c.execute(f"SELECT COUNT(*) FROM {table}")
    total = c.fetchone()[0]

    # 根据表结构选择列
    c.execute(f"SELECT * FROM {table} LIMIT 1")
    columns = [d[0] for d in c.description]

    # 根据实际表结构选择列
    col_decision = 'decision'
    col_discussion = 'rationale' if 'rationale' in columns else 'key_concerns'

    c.execute(f"""
        SELECT meeting_id, proposal_id, {col_decision}, substr({col_discussion}, 1, 100) as disc
        FROM {table}
        ORDER BY created_at DESC
        LIMIT ? OFFSET ?
    """, (limit, offset))

    # 检查是否有数据
    rows = c.fetchall()
    if not rows:
        print("  (空表)")
        conn.close()
        return

    print(f"\n{'='*60}")
    print(f"🏛️ 会议列表 (显示 {offset}-{min(offset+limit, total)}/{total} 条)")
    print(f"{'='*60}")

    for row in c.fetchall():
        print(f"\n  会议ID: {row[0][:16]}...")
        print(f"  提案: {row[1]} | 裁决: {row[2]}")
        print(f"  讨论: {row[3]}...")

    conn.close()
    print(f"\n{'='*60}\n")


def analyze_db_growth():
    """分析数据库增长情况"""
    if not os.path.exists(DB_FILE):
        print(f"❌ 数据库不存在: {DB_FILE}")
        return

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    print(f"\n{'='*60}")
    print(f"📈 数据库增长分析")
    print(f"{'='*60}")

    # 检查表的数据分布
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in c.fetchall()]

    for table in tables:
        try:
            # 估算每条记录的平均大小
            c.execute(f"SELECT COUNT(*) FROM {table}")
            count = c.fetchone()[0]

            if count == 0:
                print(f"\n  {table}: 空表")
                continue

            # 获取一条记录
            c.execute(f"SELECT * FROM {table} LIMIT 1")
            row = c.fetchone()
            if row:
                # 计算单条记录的平均大小
                total_size = sum(len(str(v).encode('utf-8')) if v else 0 for v in row)
                print(f"\n  {table}:")
                print(f"    - 记录数: {count:,}")
                print(f"    - 单条估算大小: {total_size/1024:.2f} KB")
                print(f"    - 表总估算: {count * total_size/1024/1024:.2f} MB")

        except Exception as e:
            print(f"\n  {table}: 分析失败 - {e}")

    conn.close()
    print(f"\n{'='*60}\n")


def vacuum_and_check():
    """压缩数据库并检查前后大小"""
    if not os.path.exists(DB_FILE):
        print(f"❌ 数据库不存在: {DB_FILE}")
        return

    size_before = os.path.getsize(DB_FILE)
    print(f"\n{'='*60}")
    print(f"🔧 数据库压缩")
    print(f"{'='*60}")
    print(f"  压缩前: {format_size(size_before)}")

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # 清理临时检查点
    try:
        c.execute("DELETE FROM checkpoints WHERE id NOT IN (SELECT id FROM checkpoints ORDER BY id DESC LIMIT 5)")
        print(f"  清理旧检查点: {c.rowcount} 条")
    except:
        pass

    c.execute("VACUUM")
    conn.close()

    size_after = os.path.getsize(DB_FILE)
    saved = size_before - size_after
    print(f"  压缩后: {format_size(size_after)}")
    if saved > 0:
        print(f"  节省: {format_size(saved)}")
    else:
        print(f"  变化: {format_size(saved)}")
    print(f"{'='*60}\n")


def diagnose_why_not_growing():
    """诊断数据库为什么不增长"""
    if not os.path.exists(DB_FILE):
        print(f"❌ 数据库不存在: {DB_FILE}")
        return

    print(f"\n{'='*60}")
    print(f"🔍 数据库不增长诊断")
    print(f"{'='*60}")

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # 1. 检查是否有表
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in c.fetchall()]
    print(f"\n  1. 数据库表数量: {len(tables)}")

    # 2. 检查总记录数
    total_records = 0
    for table in tables:
        try:
            c.execute(f"SELECT COUNT(*) FROM {table}")
            count = c.fetchone()[0]
            total_records += count
            print(f"    - {table}: {count:,} 条")
        except:
            pass
    print(f"\n  2. 总记录数: {total_records:,}")

    # 3. 检查数据库大小
    size = os.path.getsize(DB_FILE)
    print(f"\n  3. 数据库大小: {format_size(size)}")

    if total_records == 0:
        print(f"\n  ⚠️ 诊断结果: 数据库为空，没有数据写入")
        print(f"\n  可能原因:")
        print(f"    - sovereign_hall_detailed.py 没有运行")
        print(f"    - 爬虫没有成功抓取数据")
        print(f"    - LLM 调用失败导致流程中断")
        print(f"    - 数据被清理机制删除")
    elif size < 1024 * 100:  # 小于100KB
        print(f"\n  ⚠️ 诊断结果: 数据库很小，但有数据")
        print(f"\n  可能原因:")
        print(f"    - 数据被频繁清理 (VACUUM)")
        print(f"    - 数据写入后又删除")
        print(f"    - 记录内容很短")

    conn.close()
    print(f"{'='*60}\n")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Sovereign Hall 数据库检查器",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python db_inspector.py              # 基本统计
  python db_inspector.py --tables     # 查看表结构
  python db_inspector.py --docs 5     # 查看5条文档
  python db_inspector.py --proposals  # 查看提案
  python db_inspector.py --meetings   # 查看会议
  python db_inspector.py --analyze    # 增长分析
  python db_inspector.py --diagnose   # 诊断问题
  python db_inspector.py --vacuum     # 压缩数据库
  python db_inspector.py --all        # 显示所有信息
        """
    )

    parser.add_argument("--stats", action="store_true", help="显示统计")
    parser.add_argument("--tables", action="store_true", help="查看表结构")
    parser.add_argument("--docs", type=int, nargs="?", const=10, help="显示文档")
    parser.add_argument("--proposals", type=int, nargs="?", const=10, help="显示提案")
    parser.add_argument("--meetings", type=int, nargs="?", const=10, help="显示会议")
    parser.add_argument("--analyze", action="store_true", help="增长分析")
    parser.add_argument("--diagnose", action="store_true", help="诊断问题")
    parser.add_argument("--vacuum", action="store_true", help="压缩数据库")
    parser.add_argument("--all", action="store_true", help="显示所有信息")

    args = parser.parse_args()

    # 如果没有参数，默认显示统计
    if not any([args.stats, args.tables, args.docs, args.proposals,
                args.meetings, args.analyze, args.diagnose, args.vacuum, args.all]):
        get_db_stats()

    if args.stats or args.all:
        get_db_stats()

    if args.tables or args.all:
        get_table_info()

    if args.docs or args.all:
        show_documents(args.docs if args.docs else 10)

    if args.proposals or args.all:
        show_proposals(args.proposals if args.proposals else 10)

    if args.meetings or args.all:
        show_meetings(args.meetings if args.meetings else 10)

    if args.analyze:
        analyze_db_growth()

    if args.diagnose:
        diagnose_why_not_growing()

    if args.vacuum:
        vacuum_and_check()


if __name__ == "__main__":
    main()