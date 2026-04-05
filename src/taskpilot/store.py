from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite

from taskpilot._types import (
    CleanupResponse,
    RetryRecord,
    StatusSummary,
    TaskDetail,
    TaskListResponse,
    TaskRecord,
    TaskSummary,
)
from taskpilot.schema import init_schema


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _parse_tags(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    return json.loads(raw)


def _parse_retry_config(raw: str | None) -> dict | None:
    if raw is None:
        return None
    return json.loads(raw)


class TaskStore:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self._db: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA foreign_keys=ON")
        await init_schema(self._db)

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    @property
    def db(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("TaskStore not connected. Call connect() first.")
        return self._db

    async def insert_task(
        self,
        task_id: str,
        function_name: str,
        args_hash: str,
        tags: list[str] | None = None,
        retry_config: dict | None = None,
        parent_task_id: str | None = None,
    ) -> TaskRecord:
        now = _now_iso()
        tags_json = json.dumps(tags) if tags else None
        retry_json = json.dumps(retry_config) if retry_config else None

        await self.db.execute(
            """INSERT INTO taskpilot_tasks
               (task_id, parent_task_id, function_name, args_hash, status, tags,
                retry_config, created_at, updated_at)
               VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?)""",
            (task_id, parent_task_id, function_name, args_hash, tags_json,
             retry_json, now, now),
        )
        await self.db.commit()

        return TaskRecord(
            task_id=task_id,
            parent_task_id=parent_task_id,
            function_name=function_name,
            args_hash=args_hash,
            status="queued",
            tags=tags,
            retry_config=retry_config,
            created_at=now,
            updated_at=now,
        )

    async def update_status(
        self,
        task_id: str,
        status: str,
        error_message: str | None = None,
        error_traceback: str | None = None,
        duration_ms: int | None = None,
        retry_count: int | None = None,
    ) -> None:
        now = _now_iso()
        sets = ["status = ?", "updated_at = ?"]
        params: list[Any] = [status, now]

        if status == "running":
            sets.append("started_at = ?")
            params.append(now)

        if status in ("succeeded", "failed", "dead"):
            sets.append("completed_at = ?")
            params.append(now)

        if error_message is not None:
            sets.append("error_message = ?")
            params.append(error_message)

        if error_traceback is not None:
            sets.append("error_traceback = ?")
            params.append(error_traceback)

        if duration_ms is not None:
            sets.append("duration_ms = ?")
            params.append(duration_ms)

        if retry_count is not None:
            sets.append("retry_count = ?")
            params.append(retry_count)

        params.append(task_id)
        await self.db.execute(
            f"UPDATE taskpilot_tasks SET {', '.join(sets)} WHERE task_id = ?",
            params,
        )
        await self.db.commit()

    async def get_task(self, task_id: str) -> TaskRecord | None:
        async with self.db.execute(
            "SELECT * FROM taskpilot_tasks WHERE task_id = ?", (task_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            return self._row_to_record(row)

    async def get_task_by_prefix(self, prefix: str) -> TaskRecord | list[TaskRecord]:
        async with self.db.execute(
            "SELECT * FROM taskpilot_tasks WHERE task_id LIKE ? || '%'", (prefix,)
        ) as cursor:
            rows = await cursor.fetchall()
            if len(rows) == 0:
                return []
            if len(rows) == 1:
                return self._row_to_record(rows[0])
            return [self._row_to_record(r) for r in rows]

    async def list_tasks(
        self,
        status: str | None = None,
        function_name: str | None = None,
        since: str | None = None,
        tags: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> TaskListResponse:
        where_clauses: list[str] = []
        params: list[Any] = []

        if status:
            where_clauses.append("status = ?")
            params.append(status)
        if function_name:
            where_clauses.append("function_name = ?")
            params.append(function_name)
        if since:
            where_clauses.append("created_at >= ?")
            params.append(since)
        if tags:
            where_clauses.append("tags LIKE ?")
            params.append(f"%{tags}%")

        where = ""
        if where_clauses:
            where = "WHERE " + " AND ".join(where_clauses)

        count_sql = f"SELECT COUNT(*) FROM taskpilot_tasks {where}"
        async with self.db.execute(count_sql, params) as cursor:
            row = await cursor.fetchone()
            total = row[0]

        list_sql = (
            f"SELECT * FROM taskpilot_tasks {where} "
            f"ORDER BY created_at DESC LIMIT ? OFFSET ?"
        )
        async with self.db.execute(list_sql, [*params, limit, offset]) as cursor:
            rows = await cursor.fetchall()

        tasks = [self._row_to_summary(r) for r in rows]
        return TaskListResponse(tasks=tasks, total_matching=total, limit=limit, offset=offset)

    async def get_task_detail(self, task_id: str) -> TaskDetail | None:
        record = await self.get_task(task_id)
        if record is None:
            return None

        retries = await self.get_retries(task_id)
        result = await self.get_result(task_id)

        return TaskDetail(
            task_id=record.task_id,
            parent_task_id=record.parent_task_id,
            function_name=record.function_name,
            status=record.status,
            created_at=record.created_at,
            started_at=record.started_at,
            completed_at=record.completed_at,
            duration_ms=record.duration_ms,
            args_hash=record.args_hash,
            retry_count=record.retry_count,
            error_message=record.error_message,
            error_traceback=record.error_traceback,
            tags=record.tags,
            retries=retries,
            result=result,
        )

    async def insert_retry(
        self,
        task_id: str,
        attempt: int,
        status: str,
        error_message: str | None = None,
        error_traceback: str | None = None,
        delay_seconds: float | None = None,
        duration_ms: int | None = None,
    ) -> None:
        await self.db.execute(
            """INSERT INTO taskpilot_task_retries
               (task_id, attempt, status, error_message, error_traceback,
                delay_seconds, duration_ms)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (task_id, attempt, status, error_message, error_traceback,
             delay_seconds, duration_ms),
        )
        await self.db.commit()

    async def get_retries(self, task_id: str) -> list[RetryRecord]:
        async with self.db.execute(
            "SELECT * FROM taskpilot_task_retries WHERE task_id = ? ORDER BY attempt",
            (task_id,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [
                RetryRecord(
                    id=r["id"],
                    task_id=r["task_id"],
                    attempt=r["attempt"],
                    status=r["status"],
                    error_message=r["error_message"],
                    error_traceback=r["error_traceback"],
                    delay_seconds=r["delay_seconds"],
                    attempted_at=r["attempted_at"],
                    duration_ms=r["duration_ms"],
                )
                for r in rows
            ]

    async def store_result(self, task_id: str, result_json: str) -> None:
        await self.db.execute(
            """INSERT OR REPLACE INTO taskpilot_task_results (task_id, result_json)
               VALUES (?, ?)""",
            (task_id, result_json),
        )
        await self.db.commit()

    async def get_result(self, task_id: str) -> Any:
        async with self.db.execute(
            "SELECT result_json FROM taskpilot_task_results WHERE task_id = ?",
            (task_id,),
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            return json.loads(row["result_json"])

    async def get_status_summary(self, since: str | None = None) -> StatusSummary:
        where = ""
        params: list[Any] = []
        if since:
            where = "WHERE created_at >= ?"
            params.append(since)

        async with self.db.execute(
            f"SELECT status, COUNT(*) as cnt FROM taskpilot_tasks {where} GROUP BY status",
            params,
        ) as cursor:
            rows = await cursor.fetchall()

        by_status = {
            "queued": 0, "running": 0, "succeeded": 0,
            "failed": 0, "retrying": 0, "dead": 0,
        }
        total = 0
        for r in rows:
            by_status[r["status"]] = r["cnt"]
            total += r["cnt"]

        async with self.db.execute(
            f"SELECT AVG(duration_ms) as avg_dur FROM taskpilot_tasks {where} AND duration_ms IS NOT NULL"
            if where else
            "SELECT AVG(duration_ms) as avg_dur FROM taskpilot_tasks WHERE duration_ms IS NOT NULL",
            params,
        ) as cursor:
            row = await cursor.fetchone()
            avg_dur = row["avg_dur"] if row else None

        failed = by_status.get("failed", 0) + by_status.get("dead", 0)
        failure_rate = round(failed / total * 100, 1) if total > 0 else None

        since_label = since or "all"

        return StatusSummary(
            since=since_label,
            total=total,
            by_status=by_status,
            avg_duration_ms=round(avg_dur, 1) if avg_dur is not None else None,
            failure_rate_pct=failure_rate,
        )

    async def cleanup(
        self,
        older_than: str | None = None,
        status: str | None = None,
        dry_run: bool = False,
    ) -> CleanupResponse:
        where_clauses: list[str] = []
        params: list[Any] = []

        if older_than:
            where_clauses.append("created_at < ?")
            params.append(older_than)
        if status:
            where_clauses.append("status = ?")
            params.append(status)

        if not where_clauses:
            where_clauses.append("1=1")

        where = "WHERE " + " AND ".join(where_clauses)

        async with self.db.execute(
            f"SELECT COUNT(*) FROM taskpilot_tasks {where}", params
        ) as cursor:
            row = await cursor.fetchone()
            count = row[0]

        if not dry_run and count > 0:
            await self.db.execute(f"DELETE FROM taskpilot_tasks {where}", params)
            await self.db.commit()

        return CleanupResponse(deleted=count, dry_run=dry_run)

    async def get_recent_changes(
        self,
        since: str,
        status: str | None = None,
        function_name: str | None = None,
    ) -> list[TaskRecord]:
        where_clauses = ["updated_at > ?"]
        params: list[Any] = [since]

        if status:
            where_clauses.append("status = ?")
            params.append(status)
        if function_name:
            where_clauses.append("function_name = ?")
            params.append(function_name)

        where = "WHERE " + " AND ".join(where_clauses)

        async with self.db.execute(
            f"SELECT * FROM taskpilot_tasks {where} ORDER BY updated_at ASC",
            params,
        ) as cursor:
            rows = await cursor.fetchall()
            return [self._row_to_record(r) for r in rows]

    def _row_to_record(self, row) -> TaskRecord:
        return TaskRecord(
            task_id=row["task_id"],
            parent_task_id=row["parent_task_id"],
            function_name=row["function_name"],
            args_hash=row["args_hash"],
            status=row["status"],
            tags=_parse_tags(row["tags"]),
            retry_config=_parse_retry_config(row["retry_config"]),
            retry_count=row["retry_count"],
            error_message=row["error_message"],
            error_traceback=row["error_traceback"],
            created_at=row["created_at"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            duration_ms=row["duration_ms"],
            updated_at=row["updated_at"],
        )

    def _row_to_summary(self, row) -> TaskSummary:
        return TaskSummary(
            task_id=row["task_id"],
            function_name=row["function_name"],
            status=row["status"],
            created_at=row["created_at"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            duration_ms=row["duration_ms"],
            retry_count=row["retry_count"],
            error_message=row["error_message"],
            tags=_parse_tags(row["tags"]),
        )
