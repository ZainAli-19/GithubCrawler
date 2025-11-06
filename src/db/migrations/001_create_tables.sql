-- 001_create_tables.sql
-- Clean start (safe re-run)
DROP TABLE IF EXISTS repositories CASCADE;
DROP TABLE IF EXISTS crawl_logs CASCADE;

-- ────────────────────────────────────────────────
-- Core table for crawled GitHub repositories
-- ────────────────────────────────────────────────
CREATE TABLE repositories (
    id TEXT PRIMARY KEY,                  -- GitHub's GraphQL node ID
    name TEXT NOT NULL,
    owner TEXT NOT NULL,
    stars INTEGER NOT NULL DEFAULT 0,
    forks INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NULL,
    updated_at TIMESTAMP NULL,
    pushed_at TIMESTAMP NULL,
    url TEXT UNIQUE,                      -- ensures no duplicates by URL
    inserted_at TIMESTAMP DEFAULT NOW()   -- track first insert
);

-- Useful indexes for queries and exports
CREATE INDEX IF NOT EXISTS idx_repositories_stars ON repositories (stars DESC);
CREATE INDEX IF NOT EXISTS idx_repositories_owner ON repositories (owner);
CREATE INDEX IF NOT EXISTS idx_repositories_language ON repositories (name);

-- ────────────────────────────────────────────────
-- Crawl logs for worker diagnostics
-- ────────────────────────────────────────────────
CREATE TABLE crawl_logs (
    id SERIAL PRIMARY KEY,
    worker_name TEXT,
    range_label TEXT,
    success BOOLEAN DEFAULT TRUE,
    attempted_at TIMESTAMP DEFAULT NOW(),
    error_message TEXT
);
