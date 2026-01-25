"""Database helpers for the library indexer."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

SCHEMA_PATH = Path(__file__).with_name("schema.sql")


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def initialize(conn: sqlite3.Connection) -> None:
    schema = SCHEMA_PATH.read_text(encoding="utf-8")
    conn.executescript(schema)
    conn.commit()


def upsert_file(
    conn: sqlite3.Connection,
    *,
    path: str,
    size: int,
    mime: Optional[str],
    width: Optional[int],
    height: Optional[int],
    created_at: Optional[int],
    modified_at: Optional[int],
    accessed_at: Optional[int],
    indexed_at: int,
) -> int:
    conn.execute(
        """
        INSERT INTO files (
            path, size, mime, width, height, created_at, modified_at,
            accessed_at, indexed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
            size=excluded.size,
            mime=excluded.mime,
            width=excluded.width,
            height=excluded.height,
            created_at=excluded.created_at,
            modified_at=excluded.modified_at,
            accessed_at=excluded.accessed_at,
            indexed_at=excluded.indexed_at
        """,
        (
            path,
            size,
            mime,
            width,
            height,
            created_at,
            modified_at,
            accessed_at,
            indexed_at,
        ),
    )
    row = conn.execute("SELECT id FROM files WHERE path = ?", (path,)).fetchone()
    if row is None:
        raise RuntimeError(f"Failed to upsert file record for {path}")
    return int(row["id"])


def upsert_source(
    conn: sqlite3.Connection,
    *,
    file_id: int,
    url: Optional[str],
    extractor: Optional[str],
    metadata: Optional[Dict[str, Any]],
    created_at: int,
) -> None:
    metadata_json = json.dumps(metadata, ensure_ascii=False) if metadata else None
    conn.execute(
        """
        INSERT INTO sources (file_id, url, extractor, metadata_json, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(file_id, url, extractor) DO UPDATE SET
            metadata_json=excluded.metadata_json,
            created_at=excluded.created_at
        """,
        (file_id, url, extractor, metadata_json, created_at),
    )


def upsert_tag(conn: sqlite3.Connection, name: str) -> int:
    conn.execute(
        """
        INSERT INTO tags (name)
        VALUES (?)
        ON CONFLICT(name) DO NOTHING
        """,
        (name,),
    )
    row = conn.execute("SELECT id FROM tags WHERE name = ?", (name,)).fetchone()
    if row is None:
        raise RuntimeError(f"Failed to ensure tag for {name}")
    return int(row["id"])


def link_file_tag(conn: sqlite3.Connection, file_id: int, tag_id: int) -> None:
    conn.execute(
        """
        INSERT INTO file_tags (file_id, tag_id)
        VALUES (?, ?)
        ON CONFLICT(file_id, tag_id) DO NOTHING
        """,
        (file_id, tag_id),
    )


def upsert_collection(
    conn: sqlite3.Connection,
    *,
    name: str,
    description: Optional[str],
) -> int:
    conn.execute(
        """
        INSERT INTO collections (name, description)
        VALUES (?, ?)
        ON CONFLICT(name) DO UPDATE SET
            description=excluded.description
        """,
        (name, description),
    )
    row = conn.execute(
        "SELECT id FROM collections WHERE name = ?",
        (name,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"Failed to ensure collection for {name}")
    return int(row["id"])


def link_collection_file(
    conn: sqlite3.Connection,
    *,
    collection_id: int,
    file_id: int,
    position: Optional[int],
) -> None:
    conn.execute(
        """
        INSERT INTO collection_files (collection_id, file_id, position)
        VALUES (?, ?, ?)
        ON CONFLICT(collection_id, file_id) DO UPDATE SET
            position=excluded.position
        """,
        (collection_id, file_id, position),
    )


def flush(conn: sqlite3.Connection) -> None:
    conn.commit()


def batch_upsert_sources(
    conn: sqlite3.Connection,
    *,
    file_id: int,
    sources: Iterable[Dict[str, Any]],
    created_at: int,
) -> None:
    for source in sources:
        upsert_source(
            conn,
            file_id=file_id,
            url=source.get("url"),
            extractor=source.get("extractor"),
            metadata=source.get("metadata"),
            created_at=created_at,
        )
