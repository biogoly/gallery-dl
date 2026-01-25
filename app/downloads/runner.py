"""Run gallery-dl in subprocesses and track job history."""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterable


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    STOPPED = "stopped"


@dataclass(frozen=True)
class JobRecord:
    job_id: int
    urls: list[str]
    options: list[str]
    profile: str | None
    dest_root: str | None
    status: JobStatus
    start_time: str | None
    end_time: str | None
    exit_code: int | None
    log_path: str | None


class JobHistory:
    """Persistence layer for download job history."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS job_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    urls TEXT NOT NULL,
                    options TEXT NOT NULL,
                    profile TEXT,
                    dest_root TEXT,
                    status TEXT NOT NULL,
                    start_time TEXT,
                    end_time TEXT,
                    exit_code INTEGER,
                    log_path TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def create_job(
        self,
        urls: list[str],
        options: list[str],
        profile: str | None,
        dest_root: str | None,
        status: JobStatus,
        start_time: str | None,
        log_path: str | None,
    ) -> int:
        timestamp = _utc_now()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO job_history (
                    urls, options, profile, dest_root, status,
                    start_time, end_time, exit_code, log_path,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    json.dumps(urls),
                    json.dumps(options),
                    profile,
                    dest_root,
                    status.value,
                    start_time,
                    None,
                    None,
                    log_path,
                    timestamp,
                    timestamp,
                ),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def update_job(self, job_id: int, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = _utc_now()
        columns = ", ".join(f"{key} = ?" for key in fields)
        values = list(fields.values()) + [job_id]
        with self._connect() as conn:
            conn.execute(f"UPDATE job_history SET {columns} WHERE id = ?", values)
            conn.commit()

    def get_job(self, job_id: int) -> JobRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, urls, options, profile, dest_root, status,
                       start_time, end_time, exit_code, log_path
                FROM job_history
                WHERE id = ?
                """,
                (job_id,),
            ).fetchone()
        if row is None:
            return None
        return JobRecord(
            job_id=row[0],
            urls=json.loads(row[1]),
            options=json.loads(row[2]),
            profile=row[3],
            dest_root=row[4],
            status=JobStatus(row[5]),
            start_time=row[6],
            end_time=row[7],
            exit_code=row[8],
            log_path=row[9],
        )

    def list_jobs(self, limit: int = 100, offset: int = 0) -> list[JobRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, urls, options, profile, dest_root, status,
                       start_time, end_time, exit_code, log_path
                FROM job_history
                ORDER BY id DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ).fetchall()
        return [
            JobRecord(
                job_id=row[0],
                urls=json.loads(row[1]),
                options=json.loads(row[2]),
                profile=row[3],
                dest_root=row[4],
                status=JobStatus(row[5]),
                start_time=row[6],
                end_time=row[7],
                exit_code=row[8],
                log_path=row[9],
            )
            for row in rows
        ]


@dataclass
class JobHandle:
    process: subprocess.Popen[str]
    log_path: Path
    started_at: str
    events: list[dict[str, Any]]
    writer_lock: threading.Lock


class DownloadJobService:
    """UI-facing API for managing gallery-dl jobs."""

    def __init__(
        self,
        history: JobHistory,
        log_dir: str | Path,
        executable: str | None = None,
    ) -> None:
        self.history = history
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.executable = executable or sys.executable
        self._active: dict[int, JobHandle] = {}
        self._lock = threading.Lock()

    def start_job(
        self,
        urls: Iterable[str],
        options: Iterable[str] | None = None,
        profile: str | None = None,
        dest_root: str | None = None,
    ) -> int:
        url_list = list(urls)
        option_list = list(options or [])
        start_time = _utc_now()
        job_id = self.history.create_job(
            urls=url_list,
            options=option_list,
            profile=profile,
            dest_root=dest_root,
            status=JobStatus.QUEUED,
            start_time=start_time,
            log_path=None,
        )
        log_path = self.log_dir / f"job_{job_id}.jsonl"
        self.history.update_job(job_id, status=JobStatus.RUNNING.value, log_path=str(log_path))

        cmd = [self.executable, "-m", "gallery_dl"]
        if profile:
            cmd.extend(["--profile", profile])
        if dest_root:
            cmd.extend(["-d", dest_root])
        cmd.extend(option_list)
        cmd.extend(url_list)

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env={**os.environ},
            start_new_session=True,
        )
        handle = JobHandle(
            process=process,
            log_path=log_path,
            started_at=start_time,
            events=[],
            writer_lock=threading.Lock(),
        )
        with self._lock:
            self._active[job_id] = handle

        self._start_stream_thread(job_id, process.stdout, "stdout")
        self._start_stream_thread(job_id, process.stderr, "stderr")
        self._start_wait_thread(job_id)
        return job_id

    def stop_job(self, job_id: int, timeout: float = 10.0) -> bool:
        handle = self._active.get(job_id)
        if not handle:
            return False
        process = handle.process
        if process.poll() is not None:
            return False
        _terminate_process_group(process, signal.SIGTERM)
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            _terminate_process_group(process, signal.SIGKILL)
            process.wait()
        self.history.update_job(
            job_id,
            status=JobStatus.STOPPED.value,
            end_time=_utc_now(),
            exit_code=process.returncode,
        )
        with self._lock:
            self._active.pop(job_id, None)
        return True

    def retry_job(self, job_id: int) -> int:
        record = self.history.get_job(job_id)
        if record is None:
            raise ValueError(f"Job {job_id} not found")
        return self.start_job(
            urls=record.urls,
            options=record.options,
            profile=record.profile,
            dest_root=record.dest_root,
        )

    def get_status(self, job_id: int) -> dict[str, Any] | None:
        record = self.history.get_job(job_id)
        if record is None:
            return None
        handle = self._active.get(job_id)
        status = record.status.value
        if handle and handle.process.poll() is None:
            status = JobStatus.RUNNING.value
        return {
            "job_id": record.job_id,
            "urls": record.urls,
            "options": record.options,
            "profile": record.profile,
            "dest_root": record.dest_root,
            "status": status,
            "start_time": record.start_time,
            "end_time": record.end_time,
            "exit_code": record.exit_code,
            "log_path": record.log_path,
        }

    def list_jobs(self, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        records = self.history.list_jobs(limit=limit, offset=offset)
        return [
            {
                "job_id": record.job_id,
                "urls": record.urls,
                "options": record.options,
                "profile": record.profile,
                "dest_root": record.dest_root,
                "status": record.status.value,
                "start_time": record.start_time,
                "end_time": record.end_time,
                "exit_code": record.exit_code,
                "log_path": record.log_path,
            }
            for record in records
        ]

    def get_logs(self, job_id: int, limit: int = 200, offset: int = 0) -> list[dict[str, Any]]:
        record = self.history.get_job(job_id)
        if record is None or not record.log_path:
            return []
        log_path = Path(record.log_path)
        if not log_path.exists():
            return []
        with log_path.open("r", encoding="utf-8") as handle:
            lines = handle.readlines()
        entries = [json.loads(line) for line in lines]
        if offset < 0:
            offset = 0
        if limit <= 0:
            return entries[offset:]
        return entries[offset : offset + limit]

    def _start_stream_thread(
        self,
        job_id: int,
        stream: Iterable[str] | None,
        stream_name: str,
    ) -> None:
        if stream is None:
            return

        thread = threading.Thread(
            target=self._stream_output,
            args=(job_id, stream, stream_name),
            daemon=True,
        )
        thread.start()

    def _stream_output(
        self,
        job_id: int,
        stream: Iterable[str],
        stream_name: str,
    ) -> None:
        handle = self._active.get(job_id)
        if handle is None:
            return
        for line in stream:
            message = line.rstrip("\n")
            event = {
                "timestamp": _utc_now(),
                "stream": stream_name,
                "message": message,
                "progress": _extract_progress(message),
            }
            handle.events.append(event)
            with handle.writer_lock:
                with handle.log_path.open("a", encoding="utf-8") as log_file:
                    log_file.write(json.dumps(event) + "\n")

    def _start_wait_thread(self, job_id: int) -> None:
        thread = threading.Thread(target=self._wait_for_job, args=(job_id,), daemon=True)
        thread.start()

    def _wait_for_job(self, job_id: int) -> None:
        handle = self._active.get(job_id)
        if handle is None:
            return
        exit_code = handle.process.wait()
        record = self.history.get_job(job_id)
        if record and record.status == JobStatus.STOPPED:
            return
        status = JobStatus.COMPLETED if exit_code == 0 else JobStatus.FAILED
        self.history.update_job(
            job_id,
            status=status.value,
            end_time=_utc_now(),
            exit_code=exit_code,
        )
        with self._lock:
            self._active.pop(job_id, None)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_progress(message: str) -> dict[str, Any] | None:
    percent = _find_percent(message)
    if percent is None:
        return None
    return {"percent": percent}


def _find_percent(message: str) -> float | None:
    buffer = []
    for char in message:
        if char.isdigit() or char == ".":
            buffer.append(char)
        elif char == "%" and buffer:
            try:
                return float("".join(buffer))
            except ValueError:
                return None
        else:
            buffer = []
    return None


def _terminate_process_group(process: subprocess.Popen[str], sig: signal.Signals) -> None:
    try:
        os.killpg(process.pid, sig)
    except (AttributeError, ProcessLookupError, OSError):
        process.send_signal(sig)
