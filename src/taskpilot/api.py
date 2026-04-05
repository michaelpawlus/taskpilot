from __future__ import annotations

from pathlib import Path
from typing import Any

from taskpilot.store import TaskStore


def create_task_router(
    db_path: str | Path = "./taskpilot.db",
    prefix: str = "/tasks",
) -> Any:
    from fastapi import APIRouter, HTTPException

    router = APIRouter(prefix=prefix, tags=["taskpilot"])
    store = TaskStore(db_path)
    _connected = False

    async def ensure_connected() -> TaskStore:
        nonlocal _connected
        if not _connected:
            await store.connect()
            _connected = True
        return store

    @router.get("/")
    async def list_tasks(
        status: str | None = None,
        function_name: str | None = None,
        since: str | None = None,
        tags: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ):
        s = await ensure_connected()
        result = await s.list_tasks(
            status=status, function_name=function_name,
            since=since, tags=tags, limit=limit, offset=offset,
        )
        return result.model_dump()

    @router.get("/status")
    async def status_summary(since: str | None = None):
        s = await ensure_connected()
        result = await s.get_status_summary(since=since)
        return result.model_dump()

    @router.get("/{task_id}")
    async def get_task(task_id: str):
        s = await ensure_connected()
        detail = await s.get_task_detail(task_id)
        if detail is None:
            raise HTTPException(status_code=404, detail="Task not found")
        return detail.model_dump()

    @router.post("/{task_id}/retry")
    async def retry_task(task_id: str):
        s = await ensure_connected()
        task = await s.get_task(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="Task not found")
        if task.status not in ("failed", "dead"):
            raise HTTPException(
                status_code=400,
                detail=f"Cannot retry task with status '{task.status}'",
            )
        import uuid
        new_id = str(uuid.uuid4())
        await s.insert_task(
            task_id=new_id,
            function_name=task.function_name,
            args_hash=task.args_hash,
            tags=task.tags,
            retry_config=task.retry_config,
            parent_task_id=task.task_id,
        )
        return {"original_task_id": task.task_id, "new_task_id": new_id, "status": "queued"}

    @router.delete("/cleanup")
    async def cleanup_tasks(
        older_than: str | None = None,
        status: str | None = None,
        dry_run: bool = False,
    ):
        s = await ensure_connected()
        result = await s.cleanup(older_than=older_than, status=status, dry_run=dry_run)
        return result.model_dump()

    return router
