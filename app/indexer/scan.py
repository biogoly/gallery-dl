"""Scan a library root and upsert metadata into the index database."""
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import struct
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional

from app.db import queries

IMAGE_MIME_PREFIX = "image/"


class MetadataError(Exception):
    """Raised when metadata parsing fails."""


def _read_uint16_le(data: bytes) -> int:
    return struct.unpack("<H", data)[0]


def _read_uint16_be(data: bytes) -> int:
    return struct.unpack(">H", data)[0]


def _read_uint32_be(data: bytes) -> int:
    return struct.unpack(">I", data)[0]


def _image_size_from_gif(header: bytes) -> Optional[tuple[int, int]]:
    if len(header) < 10 or not header.startswith((b"GIF87a", b"GIF89a")):
        return None
    width = _read_uint16_le(header[6:8])
    height = _read_uint16_le(header[8:10])
    return width, height


def _image_size_from_png(header: bytes) -> Optional[tuple[int, int]]:
    if len(header) < 24 or not header.startswith(b"\x89PNG\r\n\x1a\n"):
        return None
    if header[12:16] != b"IHDR":
        return None
    width = _read_uint32_be(header[16:20])
    height = _read_uint32_be(header[20:24])
    return width, height


def _image_size_from_jpeg(data: bytes) -> Optional[tuple[int, int]]:
    if len(data) < 4 or not data.startswith(b"\xff\xd8"):
        return None
    offset = 2
    while offset < len(data) - 1:
        if data[offset] != 0xFF:
            offset += 1
            continue
        marker = data[offset + 1]
        if marker in {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF}:
            if offset + 7 >= len(data):
                return None
            height = _read_uint16_be(data[offset + 5 : offset + 7])
            width = _read_uint16_be(data[offset + 7 : offset + 9])
            return width, height
        if offset + 4 > len(data):
            return None
        length = _read_uint16_be(data[offset + 2 : offset + 4])
        if length < 2:
            return None
        offset += 2 + length
    return None


def detect_image_size(path: Path) -> Optional[tuple[int, int]]:
    try:
        with path.open("rb") as handle:
            header = handle.read(32)
            size = _image_size_from_gif(header)
            if size:
                return size
            size = _image_size_from_png(header)
            if size:
                return size
            if header.startswith(b"\xff\xd8"):
                data = header + handle.read(2048)
                return _image_size_from_jpeg(data)
    except OSError:
        return None
    return None


def guess_mime(path: Path) -> Optional[str]:
    mime, _ = mimetypes.guess_type(path.as_posix())
    return mime


def read_metadata_file(path: Path) -> Optional[Dict[str, Any]]:
    candidates = [
        path.with_suffix(path.suffix + ".json"),
        path.with_suffix(".json"),
        path.with_suffix(".info.json"),
        path.with_name(path.name + ".json"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return _load_metadata(candidate, path)
    return None


def _load_metadata(metadata_path: Path, file_path: Path) -> Optional[Dict[str, Any]]:
    try:
        content = metadata_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise MetadataError(f"Failed to read metadata file {metadata_path}") from exc
    try:
        payload = json.loads(content)
        return _select_metadata_entry(payload, file_path)
    except json.JSONDecodeError:
        entries = []
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        if not entries:
            return None
        for entry in entries:
            selected = _select_metadata_entry(entry, file_path)
            if selected is not None:
                return selected
        return entries[0] if isinstance(entries[0], dict) else None


def _select_metadata_entry(payload: Any, file_path: Path) -> Optional[Dict[str, Any]]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list):
        for entry in payload:
            if isinstance(entry, dict) and _matches_metadata(entry, file_path):
                return entry
        for entry in payload:
            if isinstance(entry, dict):
                return entry
    return None


def _matches_metadata(entry: Dict[str, Any], file_path: Path) -> bool:
    filename = file_path.name
    for key in ("filename", "file", "path", "name"):
        value = entry.get(key)
        if isinstance(value, str) and Path(value).name == filename:
            return True
    return False


def extract_sources(metadata: Dict[str, Any]) -> list[Dict[str, Any]]:
    sources: list[Dict[str, Any]] = []
    if not metadata:
        return sources
    extractor = None
    for key in ("extractor", "gdl_extractor", "_extractor"):
        value = metadata.get(key)
        if isinstance(value, str):
            extractor = value
            break
    url_keys = [
        "source",
        "source_url",
        "url",
        "file_url",
        "gdl_file_url",
        "gdl_url",
    ]
    urls: list[str] = []
    for key in url_keys:
        value = metadata.get(key)
        if isinstance(value, str):
            urls.append(value)
        elif isinstance(value, list):
            urls.extend([item for item in value if isinstance(item, str)])
    if not urls:
        return []
    for url in urls:
        sources.append({"url": url, "extractor": extractor, "metadata": metadata})
    return sources


def iter_files(root: Path) -> Iterator[Path]:
    for base, _, files in os.walk(root):
        for filename in files:
            path = Path(base) / filename
            if path.name.startswith("."):
                continue
            if is_metadata_sidecar(path):
                continue
            yield path


def is_metadata_sidecar(path: Path) -> bool:
    if path.suffix != ".json":
        return False
    if path.name.endswith(".info.json"):
        return True
    if len(path.suffixes) >= 2 and path.suffixes[-2] != ".json":
        return True
    return False


def build_file_record(path: Path, root: Path) -> Dict[str, Any]:
    stat = path.stat()
    mime = guess_mime(path)
    width = height = None
    if mime and mime.startswith(IMAGE_MIME_PREFIX):
        size = detect_image_size(path)
        if size:
            width, height = size
    return {
        "path": str(path.relative_to(root)),
        "size": stat.st_size,
        "mime": mime,
        "width": width,
        "height": height,
        "created_at": int(stat.st_ctime),
        "modified_at": int(stat.st_mtime),
        "accessed_at": int(stat.st_atime),
    }


def scan(root: Path, db_path: Path) -> int:
    conn = queries.connect(db_path)
    queries.initialize(conn)
    indexed_at = int(time.time())
    processed = 0
    for file_path in iter_files(root):
        record = build_file_record(file_path, root)
        file_id = queries.upsert_file(conn, indexed_at=indexed_at, **record)
        metadata = read_metadata_file(file_path)
        if metadata:
            sources = extract_sources(metadata)
            if sources:
                queries.batch_upsert_sources(
                    conn,
                    file_id=file_id,
                    sources=sources,
                    created_at=indexed_at,
                )
        processed += 1
        if processed % 200 == 0:
            queries.flush(conn)
    queries.flush(conn)
    return processed


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path, help="Library root to scan")
    parser.add_argument(
        "--db",
        dest="db_path",
        type=Path,
        default=Path("library.sqlite"),
        help="Path to the sqlite database",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = parse_args(argv)
    processed = scan(args.root, args.db_path)
    print(f"Indexed {processed} files into {args.db_path}")


if __name__ == "__main__":
    main()
