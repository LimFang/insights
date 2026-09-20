import csv
import json
import logging
import os
import random
import subprocess
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

from aitw.database.connection import connect
from aitw.scrape.job import CreateScrapeJob, JobManager

WINDOW_START = datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
WINDOW_END = datetime(2025, 12, 31, 0, 0, 0, tzinfo=timezone.utc)
SLICE_SECONDS = 600
SLICES_PER_DAY = 24 * 3600 // SLICE_SECONDS
AGENTS = ['codex', 'copilot', 'cursor', 'claude']
AGENT_PREFIXES = {
    'codex': 'codex/',
    'copilot': 'copilot/',
    'cursor': 'cursor/',
    'claude': 'claude/',
}
TARGET_PER_AGENT = 1000
QUOTA_HIGH = 34
QUOTA_LOW = 33
HIGH_QUOTA_EVERY = 3
SEED = 20251201
RUN_NAME = 'pilot-2025-12'


def window_days():
    days = []
    cursor = WINDOW_START
    while cursor < WINDOW_END:
        days.append(cursor)
        cursor = cursor + timedelta(days=1)
    return days


DAYS = window_days()


def base_quota(day_index):
    return QUOTA_HIGH if day_index % HIGH_QUOTA_EVERY == 0 else QUOTA_LOW


def slice_bounds(day_start):
    return [
        (
            day_start + timedelta(seconds=i * SLICE_SECONDS),
            day_start + timedelta(seconds=(i + 1) * SLICE_SECONDS),
        )
        for i in range(SLICES_PER_DAY)
    ]


def slice_order(seed, agent, day_start):
    items = slice_bounds(day_start)
    rng = random.Random(f'aitw-pilot:{seed}:{agent}:{day_start.isoformat()}')
    rng.shuffle(items)
    return items


def group_name(agent, day_start):
    return f'pilot:{agent}:{day_start.date().isoformat()}'


def parse_group(group):
    if group is None:
        return None
    parts = group.split(':')
    if len(parts) != 3 or parts[0] != 'pilot':
        return None
    try:
        return parts[1], date.fromisoformat(parts[2])
    except ValueError:
        return None


def classifier_commit():
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    try:
        res = subprocess.run(
            ['git', '-C', repo_root, 'rev-parse', 'HEAD'],
            capture_output=True, text=True, timeout=15,
        )
        if res.returncode == 0:
            return res.stdout.strip()
    except Exception:
        pass
    return None


def ensure_run(conninfo, name=RUN_NAME):
    conn = connect(conninfo)
    with conn.cursor() as cur:
        cur.execute('SELECT id FROM pilot_runs WHERE name = %s', (name,))
        row = cur.fetchone()
        if row is not None:
            run_id = row[0]
        else:
            cur.execute(
                """
                INSERT INTO pilot_runs (name, window_start, window_end, seed, classifier_commit)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (name, WINDOW_START, WINDOW_END, SEED, classifier_commit()),
            )
            run_id = cur.fetchone()[0]
    conn.commit()
    conn.close()
    return run_id


def run_info(conninfo, name=RUN_NAME):
    conn = connect(conninfo)
    with conn.cursor() as cur:
        cur.execute('SELECT id, name, window_start, window_end, seed, classifier_commit FROM pilot_runs WHERE name = %s', (name,))
        row = cur.fetchone()
    conn.close()
    return row


def submitted_count(conninfo, group):
    conn = connect(conninfo)
    with conn.cursor() as cur:
        cur.execute('SELECT count(*) FROM jobs WHERE "group" = %s', (group,))
        n = cur.fetchone()[0]
    conn.close()
    return n


def pending_count(conninfo, group):
    conn = connect(conninfo)
    with conn.cursor() as cur:
        cur.execute('SELECT count(*) FROM jobs WHERE "group" = %s AND status IN (\'open\', \'running\')', (group,))
        n = cur.fetchone()[0]
    conn.close()
    return n


def qualified_count(conninfo, run_id, agent, day_start):
    conn = connect(conninfo)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FROM pilot_candidates
            WHERE run_id = %s AND source_agent = %s AND day = %s AND qualifies_final
            """,
            (run_id, agent, day_start.date()),
        )
        n = cur.fetchone()[0]
    conn.close()
    return n


def qualified_total(conninfo, run_id, agent):
    conn = connect(conninfo)
    with conn.cursor() as cur:
        cur.execute(
            'SELECT count(*) FROM pilot_candidates WHERE run_id = %s AND source_agent = %s AND qualifies_final',
            (run_id, agent),
        )
        n = cur.fetchone()[0]
    conn.close()
    return n


def submit_slices(conninfo, agent, day_start, start_index, count):
    order = slice_order(SEED, agent, day_start)
    window = order[start_index:start_index + count]
    if not window:
        return 0
    jobs = [
        CreateScrapeJob(
            from_date=start,
            to_date=end,
            query=f'head:{AGENT_PREFIXES[agent]}',
            group=group_name(agent, day_start),
            time_key='created',
        )
        for start, end in window
    ]
    manager = JobManager(conninfo)
    manager.create_jobs(jobs)
    manager.close()
    return len(jobs)


def reset_failed(conninfo, group, max_failures=5):
    conn = connect(conninfo)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs SET status = 'open', failure_count = failure_count + 1
            WHERE "group" = %s AND status = 'failed' AND failure_count < %s
            """,
            (group, max_failures),
        )
        n = cur.rowcount
    conn.commit()
    conn.close()
    return n


def fill_day(conninfo, run_id, agent, day_start, target, batch, poll, label='', max_queue=6):
    group = group_name(agent, day_start)
    while True:
        qualified = qualified_count(conninfo, run_id, agent, day_start)
        if qualified >= target:
            return qualified
        submitted = submitted_count(conninfo, group)
        if submitted >= SLICES_PER_DAY:
            if pending_count(conninfo, group) == 0:
                return qualified
            time.sleep(poll)
            continue
        if pending_count(conninfo, group) > max_queue:
            time.sleep(poll)
            continue
        if submitted > 0:
            reset_failed(conninfo, group)
        n = min(batch, SLICES_PER_DAY - submitted)
        submit_slices(conninfo, agent, day_start, submitted, n)
        logging.info(
            f'  {label}{agent} {day_start.date()} submitted={submitted + n}/{SLICES_PER_DAY} '
            f'qualified={qualified}/{target}'
        )
        time.sleep(poll)


def status(conninfo, name=RUN_NAME):
    info = run_info(conninfo, name)
    if info is None:
        print(f'No pilot run named {name!r}.')
        return
    run_id = info[0]
    print(f'run_id={run_id} window=[{info[2]} .. {info[3]}) classifier={info[5]}')
    print(f'{"agent":8} {"day":12} {"quota":>5} {"qualified":>9} {"candidates":>10} {"submitted":>9} {"pending":>7}')
    conn = connect(conninfo)
    with conn.cursor() as cur:
        for agent in AGENTS:
            for i, day_start in enumerate(DAYS):
                cur.execute(
                    """
                    SELECT count(*), count(*) FILTER (WHERE qualifies_final)
                    FROM pilot_candidates
                    WHERE run_id = %s AND source_agent = %s AND day = %s
                    """,
                    (run_id, agent, day_start.date()),
                )
                candidates, qualified = cur.fetchone()
                group = group_name(agent, day_start)
                cur.execute('SELECT count(*) FROM jobs WHERE "group" = %s', (group,))
                submitted = cur.fetchone()[0]
                cur.execute('SELECT count(*) FROM jobs WHERE "group" = %s AND status IN (\'open\', \'running\')', (group,))
                pending = cur.fetchone()[0]
                print(f'{agent:8} {day_start.date().isoformat():12} {base_quota(i):5} {qualified:9} {candidates:10} {submitted:9} {pending:7}')
            cur.execute(
                'SELECT count(*) FROM pilot_candidates WHERE run_id = %s AND source_agent = %s',
                (run_id, agent),
            )
            total_candidates = cur.fetchone()[0]
            total_qualified = qualified_total(conninfo, run_id, agent)
            print(f'-- {agent}: candidates={total_candidates} qualified={total_qualified} target={TARGET_PER_AGENT}')
    conn.close()


def finalize(conninfo, name=RUN_NAME):
    info = run_info(conninfo, name)
    if info is None:
        raise RuntimeError(f'No pilot run named {name!r}')
    run_id = info[0]
    conn = connect(conninfo)
    achieved = {}
    with conn.cursor() as cur:
        cur.execute('DELETE FROM pilot_samples WHERE run_id = %s', (run_id,))
        for agent in AGENTS:
            cur.execute(
                """
                SELECT day, pr_id, repo_id, repo_stars, repo_visibility, seen_at
                FROM pilot_candidates
                WHERE run_id = %s AND source_agent = %s AND qualifies_final
                ORDER BY day, pr_id
                """,
                (run_id, agent),
            )
            by_day = defaultdict(list)
            for row in cur.fetchall():
                by_day[row[0]].append(row)

            selected = {d: [] for d in DAYS}
            for i, day_start in enumerate(DAYS):
                pool = by_day[day_start.date()]
                selected[day_start] = list(pool[:base_quota(i)])

            shortfall = TARGET_PER_AGENT - sum(len(v) for v in selected.values())
            while shortfall > 0:
                progressed = False
                for day_start in DAYS:
                    if shortfall <= 0:
                        break
                    pool = by_day[day_start.date()]
                    if len(selected[day_start]) < len(pool):
                        selected[day_start].append(pool[len(selected[day_start])])
                        shortfall -= 1
                        progressed = True
                if not progressed:
                    break

            written = 0
            for day_start in DAYS:
                for day, pr_id, repo_id, stars, visibility, seen_at in selected[day_start]:
                    cur.execute(
                        """
                        INSERT INTO pilot_samples
                            (run_id, agent, day, pr_id, repo_id, stars_at_selection,
                             visibility_at_selection, stars_observed_at, classifier_commit, source_query)
                        SELECT %s, %s, %s, %s, %s, %s, %s, %s,
                               (SELECT classifier_commit FROM pilot_runs WHERE id = %s), %s
                        ON CONFLICT (run_id, agent, pr_id) DO NOTHING
                        """,
                        (run_id, agent, day, pr_id, repo_id, stars, visibility, seen_at,
                         run_id, f'head:{AGENT_PREFIXES[agent]}'),
                    )
                    written += 1
            achieved[agent] = written
    conn.commit()
    conn.close()
    return achieved


def export(conninfo, name=RUN_NAME, output_dir='data/export/pilot'):
    info = run_info(conninfo, name)
    if info is None:
        raise RuntimeError(f'No pilot run named {name!r}')
    run_id, _, window_start, window_end, seed, commit = info
    os.makedirs(output_dir, exist_ok=True)

    conn = connect(conninfo)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.agent, s.day, s.pr_id, p.url, p.title, p.created_at, p.head_ref,
                   p.author_login, s.repo_id, r.name, s.stars_at_selection,
                   s.visibility_at_selection, s.stars_observed_at, s.classifier_commit, s.source_query
            FROM pilot_samples s
            JOIN prs p ON p.id = s.pr_id
            LEFT JOIN repos r ON r.id = s.repo_id
            WHERE s.run_id = %s
            ORDER BY s.agent, s.day, s.pr_id
            """,
            (run_id,),
        )
        rows = cur.fetchall()
        columns = ['agent', 'day', 'pr_id', 'url', 'title', 'created_at', 'head_ref',
                   'author_login', 'repo_id', 'repo_name', 'stars_at_selection',
                   'visibility_at_selection', 'stars_observed_at', 'classifier_commit', 'source_query']
        samples_path = os.path.join(output_dir, 'pilot_samples.csv')
        with open(samples_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            for row in rows:
                writer.writerow([v.isoformat() if isinstance(v, datetime) else v for v in row])

        cur.execute(
            """
            SELECT source_agent, count(*), count(*) FILTER (WHERE qualifies_final)
            FROM pilot_candidates WHERE run_id = %s GROUP BY source_agent
            """,
            (run_id,),
        )
        candidate_stats = {r[0]: {'candidates': r[1], 'qualified': r[2]} for r in cur.fetchall()}

        cur.execute(
            'SELECT agent, count(*) FROM pilot_samples WHERE run_id = %s GROUP BY agent',
            (run_id,),
        )
        selected_stats = {r[0]: r[1] for r in cur.fetchall()}

        stratum_path = os.path.join(output_dir, 'pilot_by_stratum.csv')
        with open(stratum_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['agent', 'day', 'base_quota', 'candidates', 'qualified', 'selected', 'shortfall'])
            for agent in AGENTS:
                for i, day_start in enumerate(DAYS):
                    cur.execute(
                        """
                        SELECT count(*), count(*) FILTER (WHERE qualifies_final)
                        FROM pilot_candidates
                        WHERE run_id = %s AND source_agent = %s AND day = %s
                        """,
                        (run_id, agent, day_start.date()),
                    )
                    candidates, qualified = cur.fetchone()
                    cur.execute(
                        'SELECT count(*) FROM pilot_samples WHERE run_id = %s AND agent = %s AND day = %s',
                        (run_id, agent, day_start.date()),
                    )
                    selected = cur.fetchone()[0]
                    writer.writerow([agent, day_start.date().isoformat(), base_quota(i),
                                     candidates, qualified, selected, max(0, base_quota(i) - selected)])

        cur.execute('SELECT count(*) FROM completed_slices')
        slices_done = cur.fetchone()[0]
        cur.execute('SELECT count(*) FROM jobs WHERE "group" LIKE \'pilot:%%\'')
        jobs_total = cur.fetchone()[0]
        cur.execute('SELECT status, count(*) FROM jobs WHERE "group" LIKE \'pilot:%%\' GROUP BY status')
        jobs_by_status = {r[0]: r[1] for r in cur.fetchall()}
    conn.close()

    summary = {
        'run_id': run_id,
        'run_name': name,
        'window': [window_start.isoformat(), window_end.isoformat()],
        'window_note': 'half-open [2025-12-01T00:00:00Z, 2025-12-31T00:00:00Z); 2025-12-31 excluded',
        'seed': seed,
        'classifier_commit': commit,
        'target_per_agent': TARGET_PER_AGENT,
        'agents': {},
        'jobs': {'total': jobs_total, 'by_status': jobs_by_status},
        'completed_slices': slices_done,
        'known_limitation': (
            'Candidate discovery uses head:<prefix> search. Claude Code PRs whose head branch '
            'does not start with claude/ but whose first commit author is claude[bot] are NOT '
            'covered by this pilot; the size of this gap is unknown.'
        ),
        'reuse_note': (
            'completed_slices enables request-level reuse only for identical head: queries and '
            '10-minute slices. Full-universe (no head:) collection may reuse ingested rows only.'
        ),
    }
    for agent in AGENTS:
        cand = candidate_stats.get(agent, {'candidates': 0, 'qualified': 0})
        achieved = selected_stats.get(agent, 0)
        summary['agents'][agent] = {
            'candidates': cand['candidates'],
            'qualified': cand['qualified'],
            'selected': achieved,
            'target': TARGET_PER_AGENT,
            'shortfall': max(0, TARGET_PER_AGENT - achieved),
            'retention_rate': (cand['qualified'] / cand['candidates']) if cand['candidates'] else None,
        }
    summary_path = os.path.join(output_dir, 'pilot_summary.json')
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    logging.info(f'📊 Exported {len(rows)} samples to {samples_path}')
    logging.info(f'📊 Summary: {summary_path}')
    logging.info(f'📊 By stratum: {stratum_path}')
    return summary


def _sched_counts(cur, run_id, agent, day_start, group):
    cur.execute(
        """
        SELECT count(*) FILTER (WHERE qualifies_final), count(*)
        FROM pilot_candidates
        WHERE run_id = %s AND source_agent = %s AND day = %s
        """,
        (run_id, agent, day_start.date()),
    )
    qualified, candidates = cur.fetchone()
    cur.execute('SELECT count(*) FROM jobs WHERE "group" = %s', (group,))
    submitted = cur.fetchone()[0]
    cur.execute('SELECT count(*) FROM jobs WHERE "group" = %s AND status IN (\'open\', \'running\')', (group,))
    pending = cur.fetchone()[0]
    return qualified, candidates, submitted, pending


def schedule(conninfo, run_id, targets, batch, poll, max_queue):
    conn = connect(conninfo)
    conn.autocommit = True
    try:
        active = dict(targets)
        while active:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE jobs SET status = 'open', failure_count = failure_count + 1
                    WHERE status = 'running' AND started_at < NOW() - interval '10 minutes'
                      AND failure_count < 5
                    """
                )
                cur.execute(
                    """
                    UPDATE jobs SET status = 'failed'
                    WHERE status = 'running' AND started_at < NOW() - interval '10 minutes'
                      AND failure_count >= 5
                    """
                )
                for key in list(active.keys()):
                    agent, day_start = key
                    target = active[key]
                    group = group_name(agent, day_start)
                    qualified, _, submitted, pending = _sched_counts(cur, run_id, agent, day_start, group)
                    if qualified >= target:
                        del active[key]
                        continue
                    if submitted >= SLICES_PER_DAY:
                        if pending == 0:
                            del active[key]
                        continue
                    if pending > max_queue:
                        continue
                    if submitted > 0:
                        cur.execute(
                            """
                            UPDATE jobs SET status = 'open', failure_count = failure_count + 1
                            WHERE "group" = %s AND status = 'failed' AND failure_count < 5
                            """,
                            (group,),
                        )
                    n = min(batch, SLICES_PER_DAY - submitted)
                    submit_slices(conninfo, agent, day_start, submitted, n)
            time.sleep(poll)
    finally:
        conn.close()


def run(conninfo, name=RUN_NAME, batch=10, poll=5, max_queue=15):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', force=True)
    run_id = ensure_run(conninfo, name)
    logging.info(f'🚀 Pilot run id={run_id} window=[{WINDOW_START} .. {WINDOW_END})')

    targets = {}
    for agent in AGENTS:
        for i, day_start in enumerate(DAYS):
            targets[(agent, day_start)] = base_quota(i)

    schedule(conninfo, run_id, targets, batch, poll, max_queue)

    for agent in AGENTS:
        total = qualified_total(conninfo, run_id, agent)
        shortfall = TARGET_PER_AGENT - total
        logging.info(f'➡️  {agent}: phase1 qualified={total} shortfall={shortfall}')
        while shortfall > 0:
            days_left = [d for d in DAYS if submitted_count(conninfo, group_name(agent, d)) < SLICES_PER_DAY]
            if not days_left:
                break
            extra = (shortfall + len(days_left) - 1) // len(days_left)
            t2 = {}
            for d in days_left:
                t2[(agent, d)] = base_quota(DAYS.index(d)) + extra
            schedule(conninfo, run_id, t2, batch, poll, max_queue)
            new_total = qualified_total(conninfo, run_id, agent)
            new_shortfall = TARGET_PER_AGENT - new_total
            if new_shortfall >= shortfall:
                break
            shortfall = new_shortfall
        final_total = qualified_total(conninfo, run_id, agent)
        logging.info(f'✅ {agent}: qualified={final_total} target={TARGET_PER_AGENT} shortfall={max(0, TARGET_PER_AGENT - final_total)}')

    achieved = finalize(conninfo, name)
    logging.info(f'🏁 Finalized samples: {achieved}')
    return achieved