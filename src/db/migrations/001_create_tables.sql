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

CREATE TABLE IF NOT EXISTS crawl_state (
    id SERIAL PRIMARY KEY,
    last_cursor TEXT,
    total_repos INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
