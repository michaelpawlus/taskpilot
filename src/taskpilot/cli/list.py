from __future__ import annotations

import asyncio
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from taskpilot._config import get_config
from taskpilot.cli._output import output_json, styled_status
from taskpilot.cli.status import _parse_duration
from taskpilot.store import TaskStore


async def _list_tasks(
    db: str,
    status_filter: str | None,
    since: str | None,
    function: str | None,
    tags: str | None,
    limit: int,
    offset: int,
    as_json: bool,
) -> None:
    store = TaskStore(db)
    await store.connect()
    try:
        since_ts = _parse_duration(since) if since else None
        result = await store.list_tasks(
            status=status_filter,
            function_name=function,
            since=since_ts,
            tags=tags,
            limit=limit,
            offset=offset,
        )

        if as_json:
            output_json(result)
            return

        console = Console()
        if not result.tasks:
            console.print("[dim]No tasks found.[/dim]")
            return

        table = Table(show_header=True, header_style="bold")
        table.add_column("ID", max_width=12)
        table.add_column("Function")
        table.add_column("Status")
        table.add_column("Created")
        table.add_column("Duration", justify="right")
        table.add_column("Retries", justify="right")

        for t in result.tasks:
            dur = f"{t.duration_ms}ms" if t.duration_ms is not None else "-"
            table.add_row(
                t.task_id[:8],
                t.function_name,
                styled_status(t.status),
                t.created_at[:19],
                dur,
                str(t.retry_count),
            )

        console.print(table)
        console.print(
            f"\nShowing {len(result.tasks)} of {result.total_matching} "
            f"(offset {result.offset})"
        )
    finally:
        await store.close()


def list_tasks(
    status: Optional[str] = typer.Option(None, "--status", help="Filter by status"),
    since: Optional[str] = typer.Option(None, "--since", help="Duration filter (e.g. 24h)"),
    function: Optional[str] = typer.Option(None, "--function", help="Filter by function name"),
    tags: Optional[str] = typer.Option(None, "--tags", help="Filter by tag text"),
    limit: int = typer.Option(50, "--limit", help="Max results"),
    offset: int = typer.Option(0, "--offset", help="Skip results"),
    json: bool = typer.Option(False, "--json", help="Output as JSON"),
    db: str = typer.Option(None, "--db", help="Database path"),
) -> None:
    """List tasks with optional filters."""
    db_path = get_config().resolve_db_path(db)
    asyncio.run(_list_tasks(db_path, status, since, function, tags, limit, offset, json))
