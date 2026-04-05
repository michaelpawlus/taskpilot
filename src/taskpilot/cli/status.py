from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from taskpilot._config import get_config
from taskpilot.cli._output import output_json, print_error, styled_status
from taskpilot.store import TaskStore


def _parse_duration(duration: str) -> str:
    """Convert duration like '24h', '7d', '30m' to ISO timestamp."""
    unit = duration[-1]
    value = int(duration[:-1])
    now = datetime.now(timezone.utc)
    if unit == "h":
        delta = timedelta(hours=value)
    elif unit == "d":
        delta = timedelta(days=value)
    elif unit == "m":
        delta = timedelta(minutes=value)
    elif unit == "s":
        delta = timedelta(seconds=value)
    else:
        raise typer.BadParameter(f"Unknown duration unit: {unit}")
    cutoff = now - delta
    return cutoff.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


async def _status(db: str, since: str | None, as_json: bool) -> None:
    store = TaskStore(db)
    await store.connect()
    try:
        since_ts = _parse_duration(since) if since else None
        summary = await store.get_status_summary(since=since_ts)
        if since:
            summary.since = since

        if as_json:
            output_json(summary)
            return

        console = Console()
        console.print(f"\n[bold]Task Status[/bold] (since {summary.since})\n")

        table = Table(show_header=True, header_style="bold")
        table.add_column("Status")
        table.add_column("Count", justify="right")

        for s, count in summary.by_status.items():
            table.add_row(styled_status(s), str(count))

        console.print(table)
        console.print(f"\nTotal: {summary.total}")
        if summary.avg_duration_ms is not None:
            console.print(f"Avg duration: {summary.avg_duration_ms}ms")
        if summary.failure_rate_pct is not None:
            console.print(f"Failure rate: {summary.failure_rate_pct}%")
        console.print()
    finally:
        await store.close()


def status(
    since: Optional[str] = typer.Option(None, help="Duration filter (e.g. 24h, 7d)"),
    json: bool = typer.Option(False, "--json", help="Output as JSON"),
    db: str = typer.Option(None, "--db", help="Database path"),
) -> None:
    """Show task status summary."""
    db_path = get_config().resolve_db_path(db)
    asyncio.run(_status(db_path, since, json))
