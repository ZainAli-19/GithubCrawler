-- 001_create_tables.sql
DROP TABLE IF EXISTS repositories;
DROP TABLE IF EXISTS crawl_logs;

CREATE TABLE repositories (
    repo_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    stars INTEGER DEFAULT 0,
    forks INTEGER DEFAULT 0,
    url TEXT,
    owner TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE crawl_logs (
    id SERIAL PRIMARY KEY,
    run_id TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    repos_crawled INTEGER DEFAULT 0,
    status TEXT
);
