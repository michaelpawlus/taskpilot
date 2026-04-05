from __future__ import annotations

import asyncio
import sys

import typer
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from taskpilot._config import get_config
from taskpilot.cli._output import output_json, print_error, styled_status
from taskpilot.store import TaskStore


async def _show(db: str, task_id: str, as_json: bool) -> int:
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
            detail = await store.get_task_detail(result.task_id)
        else:
            detail = await store.get_task_detail(task_id)

        if detail is None:
            print_error(f"Task '{task_id}' not found")
            return 2

        if as_json:
            output_json(detail)
            return 0

        console = Console()
        console.print(f"\n[bold]Task {detail.task_id}[/bold]")
        console.print(f"  Function:  {detail.function_name}")
        console.print(f"  Status:    {styled_status(detail.status)}")
        console.print(f"  Created:   {detail.created_at}")
        if detail.started_at:
            console.print(f"  Started:   {detail.started_at}")
        if detail.completed_at:
            console.print(f"  Completed: {detail.completed_at}")
        if detail.duration_ms is not None:
            console.print(f"  Duration:  {detail.duration_ms}ms")
        console.print(f"  Args hash: {detail.args_hash}")
        console.print(f"  Retries:   {detail.retry_count}")
        if detail.tags:
            console.print(f"  Tags:      {', '.join(detail.tags)}")

        if detail.error_message:
            console.print(f"\n[red]Error:[/red] {detail.error_message}")
        if detail.error_traceback:
            console.print(Panel(detail.error_traceback, title="Traceback", border_style="red"))

        if detail.retries:
            console.print("\n[bold]Retry History:[/bold]")
            for r in detail.retries:
                console.print(
                    f"  Attempt {r.attempt}: {r.status} at {r.attempted_at} "
                    f"(delay {r.delay_seconds}s)"
                )
                if r.error_message:
                    console.print(f"    Error: {r.error_message}")

        if detail.result is not None:
            console.print(f"\n[bold]Result:[/bold] {detail.result}")

        console.print()
        return 0
    finally:
        await store.close()


def show(
    task_id: str = typer.Argument(help="Task ID or unique prefix (min 6 chars)"),
    json: bool = typer.Option(False, "--json", help="Output as JSON"),
    db: str = typer.Option(None, "--db", help="Database path"),
) -> None:
    """Show detailed info for a specific task."""
    db_path = get_config().resolve_db_path(db)
    code = asyncio.run(_show(db_path, task_id, json))
    if code != 0:
        raise typer.Exit(code=code)
