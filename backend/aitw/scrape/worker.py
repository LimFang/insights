import traceback
import logging
import time
from typing import Optional

from tqdm import tqdm

from aitw.scrape.logging import setup_logging
from aitw.scrape.pr_classifier import PrClassifier
from aitw.database.pull_request_ingestor import BatchedPullRequestIngestor
from aitw.database.repository_ingestor import BatchedRepositoryIngestor
from aitw.database.pull_request import PullRequest
from aitw.database.repository import Repository
from aitw.database.connection import connect

from aitw.scrape.job import ScrapeJob, mark_job_done, mark_job_failed, pick_job
from aitw.scrape.scraper import GitHubScraper
from aitw.scrape.pilot import parse_group


def execute_pilot_job(job: ScrapeJob, token: str, db_conn: str):
    parsed = parse_group(job.group)
    if parsed is None:
        raise ValueError(f'Not a pilot job group: {job.group}')
    agent, day = parsed

    logging.info(f'🧪 Pilot job id={job.id} agent={agent} day={day} start={job.from_date} end={job.to_date}')

    scraper = GitHubScraper(token, time_key=job.time_key)
    conn = connect(db_conn)
    try:
        pr_ingestor = BatchedPullRequestIngestor(conn, conn.cursor(), auto_commit=False)
        repo_ingestor = BatchedRepositoryIngestor(conn, conn.cursor(), auto_commit=False)

        with conn.cursor() as cur:
            cur.execute('SELECT id FROM pilot_runs ORDER BY id DESC LIMIT 1')
            row = cur.fetchone()
            if row is None:
                raise RuntimeError('No pilot run row found')
            run_id = row[0]

        expected_total = scraper.count(start_date=job.from_date, end_date=job.to_date, filter=job.query)
        logging.info(f'🧪 Expecting {expected_total} candidates')

        prs = {}
        repos = {}
        for obj in scraper.scrape(start_date=job.from_date, end_date=job.to_date, filter=job.query):
            if isinstance(obj, PullRequest):
                if obj.id in prs:
                    continue
                PrClassifier.classify(obj)
                prs[obj.id] = obj
            elif isinstance(obj, Repository):
                repos[obj.id] = obj

        if len(prs) != expected_total:
            logging.warning(f'⚠️  Pilot saw {len(prs)} but expected {expected_total}')

        candidate_rows = []
        for pr in prs.values():
            base_repo = repos.get(pr.base_repo_id)
            qualifies = bool(
                pr.agent == agent
                and base_repo is not None
                and base_repo.visibility == 'PUBLIC'
                and base_repo.stargazers > 500
            )
            pr_ingestor.ingest(pr)
            candidate_rows.append((
                run_id, agent, day, pr.id, pr.agent,
                base_repo.id if base_repo is not None else None,
                base_repo.visibility if base_repo is not None else None,
                base_repo.stargazers if base_repo is not None else None,
                qualifies,
            ))
        for repo in repos.values():
            repo_ingestor.ingest(repo)

        pr_ingestor.flush()
        repo_ingestor.flush()

        with conn.cursor() as cur:
            if candidate_rows:
                cur.executemany(
                    """
                    INSERT INTO pilot_candidates
                        (run_id, source_agent, day, pr_id, native_agent_label,
                         repo_id, repo_visibility, repo_stars, qualifies_final)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, source_agent, pr_id) DO NOTHING
                    """,
                    candidate_rows,
                )
            cur.execute(
                """
                INSERT INTO completed_slices
                    (time_key, query, start, "end", agent, day, candidates_seen)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (time_key, query, start, "end") DO NOTHING
                """,
                (job.time_key, job.query, job.from_date, job.to_date, agent, day, len(prs)),
            )
            cur.execute("UPDATE jobs SET status = 'done' WHERE id = %s", (job.id,))
        conn.commit()
        logging.info(f'✅ Pilot job {job.id} committed: candidates={len(prs)} qualified={sum(1 for r in candidate_rows if r[8])}')
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_job(job: ScrapeJob, token: str, db_conn: str):
    start_date = job.from_date
    end_date = job.to_date
    query = job.query

    logging.info(f'ℹ️  Executing job id={job.id} group={job.group} start={start_date} end={end_date} query={query}...')
        
    scraper = GitHubScraper(token, time_key=job.time_key)
            
    conn = connect(db_conn)
    pr_ingestor = BatchedPullRequestIngestor(conn, conn.cursor())
    repo_ingestor = BatchedRepositoryIngestor(conn, conn.cursor())
    
    seen = set()
    expected_total = scraper.count(start_date=start_date, end_date=end_date, filter=query)
    logging.info(f'ℹ️  Expecting to scrape {expected_total} pull requests')
    with tqdm(total=expected_total) as lbar: 
        for obj in scraper.scrape(start_date=start_date, end_date=end_date, filter=query):
            if isinstance(obj, PullRequest):
                if obj.id in seen:
                    continue
                
                PrClassifier.classify(obj)
                pr_ingestor.ingest(obj)
                
                seen.add(obj.id)
                lbar.update(1)
                
            if isinstance(obj, Repository):
                repo_ingestor.ingest(obj)
                
            lbar.update(0)
        
    if len(seen) != expected_total:
        logging.warning(f'⚠️  Worker has seen {len(seen)} but expected to see {expected_total} ({start_date=} {end_date=} {query=})')
    
    pr_ingestor.flush()
    repo_ingestor.flush()
    
    conn.close()
    
def worker(token, id, group, db_conn):
    if group is not None:
        print(f'Only working on jobs of group {group}')
    setup_logging(id)
    
    while True:
        job: Optional[ScrapeJob] = pick_job(db_conn, group)
        
        if job is None:
            logging.info('ℹ️  No jobs pending. Retry in 10s...')
            time.sleep(10)
            continue
        
        try:
            if job.group is not None and job.group.startswith('pilot:'):
                execute_pilot_job(job=job, token=token, db_conn=db_conn)
            else:
                execute_job(job=job, token=token, db_conn=db_conn)
                mark_job_done(db_conn, job)
        except Exception as ex:
            mark_job_failed(db_conn, job)
            
            logging.error(f'❌ Exception while executing job={job}:')
            logging.exception(ex)
            traceback.print_exc()