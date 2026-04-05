from __future__ import annotations

from pydantic import BaseModel


class TaskRecord(BaseModel):
    task_id: str
    parent_task_id: str | None = None
    function_name: str
    args_hash: str
    status: str
    tags: list[str] | None = None
    retry_config: dict | None = None
    retry_count: int = 0
    error_message: str | None = None
    error_traceback: str | None = None
    created_at: str
    started_at: str | None = None
    completed_at: str | None = None
    duration_ms: int | None = None
    updated_at: str


class TaskSummary(BaseModel):
    task_id: str
    function_name: str
    status: str
    created_at: str
    started_at: str | None = None
    completed_at: str | None = None
    duration_ms: int | None = None
    retry_count: int = 0
    error_message: str | None = None
    tags: list[str] | None = None


class RetryRecord(BaseModel):
    id: int | None = None
    task_id: str
    attempt: int
    status: str
    error_message: str | None = None
    error_traceback: str | None = None
    delay_seconds: float | None = None
    attempted_at: str
    duration_ms: int | None = None


class TaskDetail(BaseModel):
    task_id: str
    parent_task_id: str | None = None
    function_name: str
    status: str
    created_at: str
    started_at: str | None = None
    completed_at: str | None = None
    duration_ms: int | None = None
    args_hash: str
    retry_count: int = 0
    error_message: str | None = None
    error_traceback: str | None = None
    tags: list[str] | None = None
    retries: list[RetryRecord] = []
    result: dict | list | str | None = None


class StatusSummary(BaseModel):
    since: str
    total: int
    by_status: dict[str, int]
    avg_duration_ms: float | None = None
    failure_rate_pct: float | None = None


class TaskListResponse(BaseModel):
    tasks: list[TaskSummary]
    total_matching: int
    limit: int
    offset: int


class CleanupResponse(BaseModel):
    deleted: int
    dry_run: bool


class RetryResponse(BaseModel):
    original_task_id: str
    new_task_id: str
    status: str


class TailEvent(BaseModel):
    timestamp: str
    task_id: str
    function_name: str
    status: str
    duration_ms: int | None = None
    error_message: str | None = None
    retry_attempt: int | None = None
    retry_max: int | None = None
