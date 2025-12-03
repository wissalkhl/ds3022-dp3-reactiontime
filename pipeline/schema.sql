CREATE TABLE IF NOT EXISTS social_events (
    id TEXT,
    author TEXT,
    handle TEXT,
    text TEXT,
    created_at TIMESTAMP,
    uri TEXT,
    sentiment REAL,
    topic TEXT
);

CREATE TABLE IF NOT EXISTS wiki_events (
    id TEXT,
    page_title TEXT,
    comment TEXT,
    namespace INTEGER,
    is_bot BOOLEAN,
    is_minor BOOLEAN,
    timestamp TIMESTAMP,
    user_text TEXT,
    topic TEXT
);
