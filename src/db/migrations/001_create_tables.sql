-- src/db/migrations/001_create_tables.sql
CREATE TABLE IF NOT EXISTS repositories (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    owner TEXT NOT NULL,
    stars INT,
    forks INT,  -- ✅ Added this missing column
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    pushed_at TIMESTAMPTZ,
    url TEXT
);

CREATE TABLE IF NOT EXISTS crawl_logs (
    id SERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    total_repos INT,
    notes TEXT
);
