import csv
import logging
from datetime import datetime

from dateutil.relativedelta import relativedelta

from aitw.database.connection import connect

TARGET_AGENTS = ['codex', 'copilot', 'cursor', 'claude']

EXPORT_COLUMNS = [
    'id', 'agent', 'url', 'title', 'description', 'created_at', 'closed_at',
    'merged', 'is_draft', 'additions', 'deletions', 'changed_files',
    'comments', 'commits', 'reviewers', 'base_repo_id', 'head_repo_id',
    'base_ref', 'head_ref', 'author_login', 'author_type',
    'repo_name', 'repo_stars'
]


def _filters_match(min_stars, start, end):
    return """
        FROM prs p
        JOIN repos r ON r.id = p.base_repo_id
        WHERE r.stars > %s
          AND r.visibility = 'PUBLIC'
          AND p.agent = ANY(%s)
          AND p.created_at >= %s
          AND p.created_at < %s
    """


def _stats(conn, min_stars, start, end):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT count(*), count(DISTINCT p.base_repo_id),
                   min(r.stars), max(r.stars),
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY r.stars)
            {_filters_match(min_stars, start, end)}
        """, (min_stars, TARGET_AGENTS, start, end))
        total, unique_repos, stars_min, stars_max, stars_median = cur.fetchone()

        cur.execute(f"""
            SELECT p.agent, count(*)
            {_filters_match(min_stars, start, end)}
            GROUP BY p.agent
            ORDER BY count(*) DESC
        """, (min_stars, TARGET_AGENTS, start, end))
        per_agent = cur.fetchall()

    return {
        'window_start': start,
        'window_end': end,
        'total_prs': total,
        'unique_repos': unique_repos,
        'stars_min': stars_min,
        'stars_median': stars_median,
        'stars_max': stars_max,
        'per_agent': per_agent,
    }


def export_filtered(conninfo, output, months=9, min_stars=500):
    end = datetime.now().astimezone()
    start = end - relativedelta(months=months)

    conn = connect(conninfo)
    stats = _stats(conn, min_stars, start, end)

    written = 0
    with conn.cursor(name='export_filtered_stream') as cur:
        cur.itersize = 10000
        cur.execute(f"""
            SELECT p.id, p.agent, p.url, p.title, p.description, p.created_at, p.closed_at,
                   p.merged, p.is_draft, p.additions, p.deletions, p.changed_files,
                   p.comments, p.commits, p.reviewers, p.base_repo_id, p.head_repo_id,
                   p.base_ref, p.head_ref, p.author_login, p.author_type,
                   r.name, r.stars
            {_filters_match(min_stars, start, end)}
            ORDER BY p.created_at
        """, (min_stars, TARGET_AGENTS, start, end))

        with open(output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(EXPORT_COLUMNS)
            for row in cur:
                writer.writerow([v.isoformat() if isinstance(v, datetime) else v for v in row])
                written += 1

    conn.close()

    logging.info(f"📊 Exported {written} pull requests to {output}")
    logging.info(f"🪟 Window: [{stats['window_start']} .. {stats['window_end']})")
    logging.info(f"📦 Repos: {stats['unique_repos']} unique | stars min={stats['stars_min']} median={stats['stars_median']} max={stats['stars_max']}")
    for agent, count in stats['per_agent']:
        logging.info(f"🤖 {agent}: {count}")

    return stats
