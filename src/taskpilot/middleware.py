from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

from taskpilot._config import configure, get_config
from taskpilot.store import TaskStore


class TaskPilotContext:
    def __init__(self, store: TaskStore):
        self._store = store

    async def run(self, func: Callable, *args: Any, **kwargs: Any) -> str:
        if not getattr(func, "_taskpilot_tracked", False):
            raise ValueError(
                f"{func.__name__} is not decorated with @taskpilot.track"
            )
        await func(*args, **kwargs)
        # The wrapper creates its own store connection, so we read back
        # the most recent task for this function to get its ID
        result = await self._store.list_tasks(
            function_name=getattr(func, "_taskpilot_func", func).__name__,
            limit=1,
        )
        if result.tasks:
            return result.tasks[0].task_id
        raise RuntimeError("Task was not recorded")

    async def status(self, task_id: str) -> dict:
        detail = await self._store.get_task_detail(task_id)
        if detail is None:
            return {"error": "not found"}
        return detail.model_dump()


class TaskPilotMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app: ASGIApp,
        db_path: str | Path = "./taskpilot.db",
        cleanup_interval: int | None = None,
        cleanup_max_age: int = 30 * 86400,
    ):
        super().__init__(app)
        self.db_path = str(db_path)
        self.cleanup_interval = cleanup_interval
        self.cleanup_max_age = cleanup_max_age
        self._store: TaskStore | None = None
        self._cleanup_task: asyncio.Task | None = None

        configure(db_path=self.db_path)

    async def _ensure_store(self) -> TaskStore:
        if self._store is None:
            self._store = TaskStore(self.db_path)
            await self._store.connect()
            if self.cleanup_interval:
                self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        return self._store

    async def _cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cleanup_interval)  # type: ignore[arg-type]
            try:
                cutoff = datetime.now(timezone.utc)
                from datetime import timedelta
                cutoff = cutoff - timedelta(seconds=self.cleanup_max_age)
                cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                store = await self._ensure_store()
                await store.cleanup(older_than=cutoff_str)
            except Exception:
                pass

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        store = await self._ensure_store()
        request.state.taskpilot = TaskPilotContext(store)
        response = await call_next(request)
        return response
