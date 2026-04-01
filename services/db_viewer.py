#!/usr/bin/env python3
"""
数据库查看器 - 查看 Sovereign Hall 数据库内容和统计
"""

import sqlite3
import os
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
        print(f"❌ 数据库不存在: {DB_FILE}")
        return

    size = os.path.getsize(DB_FILE)
    print(f"\n{'='*60}")
    print(f"📊 数据库统计")
    print(f"{'='*60}")
    print(f"  文件: {DB_FILE}")
    print(f"  大小: {format_size(size)}")

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # 获取所有实际存在的表
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
    tables = [row[0] for row in c.fetchall()]

    # 各表统计
    for table in tables:
        try:
            c.execute(f"SELECT COUNT(*) FROM {table}")
            count = c.fetchone()[0]
            print(f"  [✓] {table}: {count:,} 条")
        except Exception as e:
            print(f"  [✗] {table}: 读取失败 - {e}")

    # 最后更新时间（带错误处理）
    time_cols = ['created_at', 'crawled_at', 'archived_at', 'updated_at']
    for table in tables:
        for col in time_cols:
            try:
                c.execute(f"SELECT MAX({col}) FROM {table}")
                last_time = c.fetchone()[0]
                if last_time:
                    print(f"  {table} 最后更新: {str(last_time)[:19]}")
                    break
            except:
                continue

    conn.close()
    print(f"{'='*60}\n")


def show_documents(limit: int = 10):
    """显示文档列表"""
    if not os.path.exists(DB_FILE):
        print(f"❌ 数据库不存在: {DB_FILE}")
        return

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # 检查是否存在 created_at 列，否则使用 crawled_at
    c.execute("PRAGMA table_info(documents)")
    columns = [col[1] for col in c.fetchall()]
    time_col = 'created_at' if 'created_at' in columns else 'crawled_at'

    c.execute(f"""
        SELECT id, title, source, sector, substr(content, 1, 100) as preview
        FROM documents
        ORDER BY {time_col} DESC
        LIMIT ?
    """, (limit,))

    print(f"\n{'='*60}")
    print(f"📄 最新文档 (前{limit}条)")
    print(f"{'='*60}")
    for row in c.fetchall():
        print(f"\n  ID: {row[0][:16]}...")
        print(f"  标题: {row[1]}")
        print(f"  来源: {row[2]} | 行业: {row[3]}")
    print(f"\n{'='*60}\n")
    conn.close()


def show_proposals(limit: int = 10):
    """显示提案列表"""
    if not os.path.exists(DB_FILE):
        print(f"❌ 数据库不存在: {DB_FILE}")
        return

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # 检查是否存在 status 列
    c.execute("PRAGMA table_info(proposals)")
    columns = [col[1] for col in c.fetchall()]
    has_status = 'status' in columns

    if has_status:
        c.execute("""
            SELECT proposal_id, ticker, direction, confidence, substr(thesis, 1, 80) as thesis_preview
            FROM proposals
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,))
    else:
        c.execute("""
            SELECT proposal_id, ticker, direction, confidence, substr(thesis, 1, 80) as thesis_preview
            FROM proposals
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,))

    print(f"\n{'='*60}")
    print(f"📋 最新提案 (前{limit}条)")
    print(f"{'='*60}")
    for row in c.fetchall():
        proposal_id, ticker, direction, confidence, thesis_preview = row
        status = "N/A"  # proposals 表没有 status 列
        print(f"\n  提案ID: {proposal_id[:16]}...")
        print(f"  标的: {ticker} | {direction} | 置信度: {confidence:.0%}")
        print(f"  摘要: {thesis_preview}...")
    print(f"\n{'='*60}\n")
    conn.close()


def show_meetings(limit: int = 10):
    """显示会议记录"""
    if not os.path.exists(DB_FILE):
        print(f"❌ 数据库不存在: {DB_FILE}")
        return

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # 检查表是否存在
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='meeting_minutes'")
    if not c.fetchone():
        print(f"\n  ⚠️ 会议表不存在")
        conn.close()
        return

    c.execute("""
        SELECT meeting_id, proposal_id, decision, substr(rationale, 1, 100) as disc
        FROM meeting_minutes
        ORDER BY created_at DESC
        LIMIT ?
    """, (limit,))

    print(f"\n{'='*60}")
    print(f"🏛️  最新会议 (前{limit}条)")
    print(f"{'='*60}")
    for row in c.fetchall():
        print(f"\n  会议: {row[0][:16]}...")
        print(f"  提案: {row[1]} | 裁决: {row[2]}")
        print(f"  讨论: {row[3]}...")
    print(f"\n{'='*60}\n")
    conn.close()


def show_playbook(limit: int = 10):
    """显示经验记录"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute("""
        SELECT entry_id, ticker, category, substr(lesson, 1, 80) as lesson_preview
        FROM playbook
        ORDER BY created_at DESC
        LIMIT ?
    """, (limit,))

    print(f"\n{'='*60}")
    print(f"📖 投资手册 (前{limit}条)")
    print(f"{'='*60}")
    for row in c.fetchall():
        print(f"\n  标的: {row[1]} | 类型: {row[2]}")
        print(f"  经验: {row[3]}...")
    print(f"\n{'='*60}\n")


def show_checkpoints(limit: int = 5):
    """显示检查点"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute("""
        SELECT id, iteration, substr(stats, 1, 100) as stats_preview
        FROM checkpoints
        ORDER BY id DESC
        LIMIT ?
    """, (limit,))

    print(f"\n{'='*60}")
    print(f"💾 检查点 (前{limit}条)")
    print(f"{'='*60}")
    for row in c.fetchall():
        print(f"\n  ID: {row[0]} | 迭代: {row[1]}")
        print(f"  统计: {row[2]}...")
    print(f"\n{'='*60}\n")


def search_content(keyword: str, table: str = "all", limit: int = 20):
    """搜索内容"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    results = []

    if table in ["all", "documents"]:
        c.execute("""
            SELECT 'document' as type, id, title FROM documents
            WHERE title LIKE ? OR content LIKE ?
            LIMIT ?
        """, (f"%{keyword}%", f"%{keyword}%", limit))
        results.extend([('document', r[1], r[2]) for r in c.fetchall()])

    if table in ["all", "proposals"]:
        c.execute("""
            SELECT 'proposal' as type, proposal_id, ticker FROM proposals
            WHERE thesis LIKE ? OR ticker LIKE ?
            LIMIT ?
        """, (f"%{keyword}%", f"%{keyword}%", limit))
        results.extend([('proposal', r[1], r[2]) for r in c.fetchall()])

    if table in ["all", "playbook"]:
        c.execute("""
            SELECT 'playbook' as type, entry_id, ticker FROM playbook
            WHERE situation LIKE ? OR lesson LIKE ?
            LIMIT ?
        """, (f"%{keyword}%", f"%{keyword}%", limit))
        results.extend([('playbook', r[1], r[2]) for r in c.fetchall()])

    conn.close()

    print(f"\n{'='*60}")
    print(f"🔍 搜索结果: '{keyword}' (共{len(results)}条)")
    print(f"{'='*60}")
    for r in results[:limit]:
        print(f"  [{r[0]}] {r[1]}: {r[2]}")
    print(f"\n{'='*60}\n")


def vacuum_db():
    """压缩数据库"""
    if not os.path.exists(DB_FILE):
        print(f"❌ 数据库不存在: {DB_FILE}")
        return

    size_before = os.path.getsize(DB_FILE)
    print(f"\n压缩前大小: {format_size(size_before)}")

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("VACUUM")
    conn.close()

    size_after = os.path.getsize(DB_FILE)
    print(f"压缩后大小: {format_size(size_after)}")
    print(f"节省: {format_size(size_before - size_after)}\n")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="数据库查看器")
    parser.add_argument("--stats", action="store_true", help="显示统计")
    parser.add_argument("--docs", type=int, nargs="?", const=10, help="显示文档")
    parser.add_argument("--proposals", type=int, nargs="?", const=10, help="显示提案")
    parser.add_argument("--meetings", type=int, nargs="?", const=10, help="显示会议")
    parser.add_argument("--playbook", type=int, nargs="?", const=10, help="显示经验")
    parser.add_argument("--checkpoints", type=int, nargs="?", const=5, help="显示检查点")
    parser.add_argument("--search", type=str, nargs="?", const="", help="搜索内容")
    parser.add_argument("--vacuum", action="store_true", help="压缩数据库")
    parser.add_argument("--all", action="store_true", help="显示所有信息")

    args = parser.parse_args()

    # 如果没有参数，显示基本统计
    if not any([args.stats, args.docs, args.proposals, args.meetings,
                args.playbook, args.checkpoints, args.search, args.vacuum, args.all]):
        get_db_stats()
        show_documents(5)
        show_proposals(5)
        show_meetings(5)

    if args.stats or args.all:
        get_db_stats()

    if args.docs or args.all:
        show_documents(args.docs if args.docs else 10)

    if args.proposals or args.all:
        show_proposals(args.proposals if args.proposals else 10)

    if args.meetings or args.all:
        show_meetings(args.meetings if args.meetings else 10)

    if args.playbook or args.all:
        show_playbook(args.playbook if args.playbook else 10)

    if args.checkpoints or args.all:
        show_checkpoints(args.checkpoints if args.checkpoints else 5)

    if args.search:
        search_content(args.search)

    if args.vacuum:
        vacuum_db()


if __name__ == "__main__":
    main()