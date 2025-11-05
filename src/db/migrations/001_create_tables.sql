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
