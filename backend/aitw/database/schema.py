import logging

from aitw.database.connection import connect

SCHEMA = """
CREATE TABLE IF NOT EXISTS prs (
    id BIGINT PRIMARY KEY,
    agent TEXT,
    url TEXT,
    title TEXT,
    description TEXT,
    created_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    merged BOOLEAN,
    is_draft BOOLEAN,
    additions INTEGER,
    deletions INTEGER,
    changed_files INTEGER,
    comments INTEGER,
    commits INTEGER,
    reviewers INTEGER,
    base_repo_id BIGINT,
    head_repo_id BIGINT,
    base_ref TEXT,
    head_ref TEXT,
    author_login TEXT,
    author_type TEXT,
    files JSONB,
    commits_list JSONB,
    comments_list JSONB,
    primary_language TEXT
);

CREATE TABLE IF NOT EXISTS repos (
    id BIGINT PRIMARY KEY,
    name TEXT,
    url TEXT,
    fork BOOLEAN,
    forks INTEGER,
    watchers INTEGER,
    stars INTEGER,
    primary_language TEXT,
    visibility TEXT
);

CREATE TABLE IF NOT EXISTS jobs (
    id BIGSERIAL PRIMARY KEY,
    "group" TEXT,
    status TEXT NOT NULL DEFAULT 'open',
    start TIMESTAMPTZ,
    "end" TIMESTAMPTZ,
    query TEXT,
    time_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    failure_count INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS prs_created_at_idx ON prs (created_at);
CREATE INDEX IF NOT EXISTS prs_agent_idx ON prs (agent);
CREATE INDEX IF NOT EXISTS prs_base_repo_idx ON prs (base_repo_id);
CREATE INDEX IF NOT EXISTS jobs_group_created_idx ON jobs ("group", created_at);
CREATE INDEX IF NOT EXISTS jobs_group_status_idx ON jobs ("group", status);

CREATE TABLE IF NOT EXISTS pilot_runs (
    id BIGSERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    seed BIGINT NOT NULL,
    classifier_commit TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS completed_slices (
    id BIGSERIAL PRIMARY KEY,
    time_key TEXT NOT NULL,
    query TEXT NOT NULL,
    start TIMESTAMPTZ NOT NULL,
    "end" TIMESTAMPTZ NOT NULL,
    agent TEXT,
    day DATE,
    candidates_seen INTEGER NOT NULL DEFAULT 0,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (time_key, query, start, "end")
);

CREATE TABLE IF NOT EXISTS pilot_candidates (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES pilot_runs(id),
    source_agent TEXT NOT NULL,
    day DATE NOT NULL,
    pr_id BIGINT NOT NULL,
    native_agent_label TEXT,
    repo_id BIGINT,
    repo_visibility TEXT,
    repo_stars INTEGER,
    qualifies_final BOOLEAN NOT NULL,
    seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, source_agent, pr_id)
);
CREATE INDEX IF NOT EXISTS pilot_candidates_lookup_idx
    ON pilot_candidates (run_id, source_agent, day, qualifies_final);

CREATE TABLE IF NOT EXISTS pilot_samples (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES pilot_runs(id),
    agent TEXT NOT NULL,
    day DATE NOT NULL,
    pr_id BIGINT NOT NULL,
    repo_id BIGINT,
    stars_at_selection INTEGER,
    visibility_at_selection TEXT,
    stars_observed_at TIMESTAMPTZ,
    classifier_commit TEXT,
    source_query TEXT,
    selected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, agent, pr_id)
);
"""


def init_schema(conninfo):
    conn = connect(conninfo)
    with conn.cursor() as cur:
        cur.execute(SCHEMA)
    conn.commit()
    conn.close()

    logging.info('✅ Schema initialized.')
