from __future__ import annotations

import asyncio
import uuid

import typer
from rich.console import Console

from taskpilot._config import get_config
from taskpilot._types import RetryResponse
from taskpilot.cli._output import output_json, print_error
from taskpilot.store import TaskStore


async def _retry(db: str, task_id: str, as_json: bool) -> int:
    store = TaskStore(db)
    await store.connect()
    try:
        if len(task_id) < 36:
            if len(task_id) < 6:
                print_error("Prefix must be at least 6 characters")
                return 1
            result = await store.get_task_by_prefix(task_id)
            if isinstance(result, list):
                if len(result) == 0:
                    print_error(f"No task found with prefix '{task_id}'")
                    return 2
                print_error(
                    f"Ambiguous prefix '{task_id}' matches {len(result)} tasks"
                )
                return 1
            task = result
        else:
            task = await store.get_task(task_id)

        if task is None:
            print_error(f"Task '{task_id}' not found")
            return 2

        if task.status not in ("failed", "dead"):
            print_error(
                f"Cannot retry task with status '{task.status}'. "
                "Only 'failed' or 'dead' tasks can be retried."
            )
            return 1

        new_id = str(uuid.uuid4())
        await store.insert_task(
            task_id=new_id,
            function_name=task.function_name,
            args_hash=task.args_hash,
            tags=task.tags,
            retry_config=task.retry_config,
            parent_task_id=task.task_id,
        )

        resp = RetryResponse(
            original_task_id=task.task_id,
            new_task_id=new_id,
            status="queued",
        )

        if as_json:
            output_json(resp)
            return 0

        console = Console()
        console.print(f"[green]Retry queued:[/green] {new_id}")
        console.print(f"  Original: {task.task_id}")
        console.print(f"  Function: {task.function_name}")
        return 0
    finally:
        await store.close()


def retry(
    task_id: str = typer.Argument(help="Task ID or unique prefix to retry"),
    json: bool = typer.Option(False, "--json", help="Output as JSON"),
    db: str = typer.Option(None, "--db", help="Database path"),
) -> None:
    """Re-queue a failed or dead task."""
    db_path = get_config().resolve_db_path(db)
    code = asyncio.run(_retry(db_path, task_id, json))
    if code != 0:
        raise typer.Exit(code=code)
