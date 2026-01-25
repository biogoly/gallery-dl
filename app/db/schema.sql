PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    size INTEGER NOT NULL,
    mime TEXT,
    width INTEGER,
    height INTEGER,
    created_at INTEGER,
    modified_at INTEGER,
    accessed_at INTEGER,
    indexed_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY,
    file_id INTEGER NOT NULL,
    url TEXT,
    extractor TEXT,
    metadata_json TEXT,
    created_at INTEGER NOT NULL,
    UNIQUE (file_id, url, extractor),
    FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tags (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS file_tags (
    file_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    PRIMARY KEY (file_id, tag_id),
    FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS collections (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT
);

CREATE TABLE IF NOT EXISTS collection_files (
    collection_id INTEGER NOT NULL,
    file_id INTEGER NOT NULL,
    position INTEGER,
    PRIMARY KEY (collection_id, file_id),
    FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE,
    FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
);
