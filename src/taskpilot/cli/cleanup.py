from __future__ import annotations

import asyncio
import sys
from typing import Optional

import typer
from rich.console import Console

from taskpilot._config import get_config
from taskpilot.cli._output import output_json, print_error
from taskpilot.cli.status import _parse_duration
from taskpilot.store import TaskStore


async def _cleanup(
    db: str,
    older_than: str | None,
    status_filter: str | None,
    dry_run: bool,
    as_json: bool,
) -> None:
    store = TaskStore(db)
    await store.connect()
    try:
        older_than_ts = _parse_duration(older_than) if older_than else None

        if not as_json and not dry_run and sys.stdin.isatty():
            console = Console()
            preview = await store.cleanup(
                older_than=older_than_ts, status=status_filter, dry_run=True
            )
            if preview.deleted == 0:
                console.print("[dim]No tasks match the criteria.[/dim]")
                return
            confirm = typer.confirm(
                f"Delete {preview.deleted} task(s)?", default=False
            )
            if not confirm:
                console.print("[dim]Cancelled.[/dim]")
                return

        result = await store.cleanup(
            older_than=older_than_ts, status=status_filter, dry_run=dry_run
        )

        if as_json:
            output_json(result)
            return

        console = Console()
        if dry_run:
            console.print(f"[yellow]Dry run:[/yellow] {result.deleted} task(s) would be deleted")
        else:
            console.print(f"[green]Deleted:[/green] {result.deleted} task(s)")
    finally:
        await store.close()


def cleanup(
    older_than: Optional[str] = typer.Option(None, "--older-than", help="Duration (e.g. 30d)"),
    status: Optional[str] = typer.Option(None, "--status", help="Filter by status"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview without deleting"),
    json: bool = typer.Option(False, "--json", help="Output as JSON"),
    db: str = typer.Option(None, "--db", help="Database path"),
) -> None:
    """Delete old tasks from the database."""
    db_path = get_config().resolve_db_path(db)
    asyncio.run(_cleanup(db_path, older_than, status, dry_run, json))
