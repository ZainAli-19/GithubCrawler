
DROP TABLE IF EXISTS repositories CASCADE;
DROP TABLE IF EXISTS crawl_logs CASCADE;

CREATE TABLE repositories (
    id TEXT PRIMARY KEY,                  
    name TEXT NOT NULL,
    owner TEXT NOT NULL,
    stars INTEGER NOT NULL DEFAULT 0,
    forks INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NULL,
    updated_at TIMESTAMP NULL,
    pushed_at TIMESTAMP NULL,
    url TEXT UNIQUE,                     
    inserted_at TIMESTAMP DEFAULT NOW()  
);

CREATE INDEX IF NOT EXISTS idx_repositories_stars ON repositories (stars DESC);
CREATE INDEX IF NOT EXISTS idx_repositories_owner ON repositories (owner);
CREATE INDEX IF NOT EXISTS idx_repositories_language ON repositories (name);

CREATE TABLE crawl_logs (
    id SERIAL PRIMARY KEY,
    worker_name TEXT,
    range_label TEXT,
    success BOOLEAN DEFAULT TRUE,
    attempted_at TIMESTAMP DEFAULT NOW(),
    error_message TEXT
);
