from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timezone
from typing import Optional

import typer
from rich.console import Console

from taskpilot._config import get_config
from taskpilot._types import TailEvent
from taskpilot.cli._output import STATUS_COLORS, STATUS_SYMBOLS
from taskpilot.store import TaskStore


async def _tail(
    db: str,
    poll_interval: float,
    status_filter: str | None,
    function: str | None,
    as_json: bool,
) -> None:
    store = TaskStore(db)
    await store.connect()

    console = Console(stderr=not as_json)
    if not as_json:
        console.print("[dim]Watching for task changes... (Ctrl+C to stop)[/dim]\n")

    last_check = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    try:
        while True:
            changes = await store.get_recent_changes(
                since=last_check,
                status=status_filter,
                function_name=function,
            )

            for task in changes:
                now_ts = task.updated_at[:8] if len(task.updated_at) >= 8 else task.updated_at
                time_part = task.updated_at[11:19] if len(task.updated_at) > 19 else task.updated_at

                event = TailEvent(
                    timestamp=task.updated_at,
                    task_id=task.task_id,
                    function_name=task.function_name,
                    status=task.status,
                    duration_ms=task.duration_ms,
                    error_message=task.error_message,
                    retry_attempt=task.retry_count if task.status == "retrying" else None,
                    retry_max=(
                        task.retry_config.get("retries")
                        if task.retry_config and task.status == "retrying"
                        else None
                    ),
                )

                if as_json:
                    print(json.dumps(event.model_dump(), default=str), flush=True)
                else:
                    symbol = STATUS_SYMBOLS.get(task.status, "?")
                    color = STATUS_COLORS.get(task.status, "white")
                    extra = ""
                    if task.duration_ms is not None and task.status == "succeeded":
                        extra = f"  ({task.duration_ms / 1000:.1f}s)"
                    elif task.error_message and task.status in ("failed", "dead"):
                        extra = f"  {task.error_message.split(chr(10))[0]}"
                    elif task.status == "retrying" and task.retry_config:
                        max_r = task.retry_config.get("retries", "?")
                        extra = f"  (attempt {task.retry_count}/{max_r})"

                    console.print(
                        f"{time_part} {symbol} [{color}]{task.status.upper():10s}[/] "
                        f"{task.function_name:30s} {task.task_id[:8]}{extra}"
                    )

            if changes:
                last_check = changes[-1].updated_at

            await asyncio.sleep(poll_interval)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await store.close()


def tail(
    poll_interval: float = typer.Option(1.0, "--poll-interval", help="Polling interval in seconds"),
    status: Optional[str] = typer.Option(None, "--status", help="Filter by status"),
    function: Optional[str] = typer.Option(None, "--function", help="Filter by function name"),
    json: bool = typer.Option(False, "--json", help="Output as NDJSON"),
    db: str = typer.Option(None, "--db", help="Database path"),
) -> None:
    """Live-stream task state changes."""
    db_path = get_config().resolve_db_path(db)
    asyncio.run(_tail(db_path, poll_interval, status, function, json))
