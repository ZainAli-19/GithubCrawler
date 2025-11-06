CREATE TABLE IF NOT EXISTS repositories (
    id TEXT PRIMARY KEY,
    name TEXT,
    full_name TEXT,
    stars INTEGER,
    language TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    owner TEXT
);

CREATE TABLE IF NOT EXISTS crawl_checkpoint (
    id SERIAL PRIMARY KEY,
    cursor_value TEXT,
    total_repos INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW()
);
