CREATE TABLE IF NOT EXISTS repositories (
    id SERIAL PRIMARY KEY,
    node_id TEXT UNIQUE,
    owner TEXT NOT NULL,
    name TEXT NOT NULL,
    full_name TEXT NOT NULL,
    stars INTEGER NOT NULL,
    language TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    last_crawled_at TIMESTAMP DEFAULT now()
);
