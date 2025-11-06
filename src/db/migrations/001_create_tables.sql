-- 001_create_tables.sql

DROP TABLE IF EXISTS repositories CASCADE;
DROP TABLE IF EXISTS crawl_logs CASCADE;

CREATE TABLE repositories (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    owner TEXT NOT NULL,
    stars INTEGER NOT NULL DEFAULT 0,
    forks INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    pushed_at TIMESTAMP,
    url TEXT
);

CREATE TABLE crawl_logs (
    id SERIAL PRIMARY KEY,
    worker_name TEXT,
    range_label TEXT,
    success BOOLEAN,
    attempted_at TIMESTAMP DEFAULT NOW(),
    error_message TEXT
);
