from __future__ import annotations

import json
import sys
from typing import Any

from rich.console import Console
from rich.table import Table

err_console = Console(stderr=True)

STATUS_COLORS = {
    "queued": "white",
    "running": "blue",
    "succeeded": "green",
    "failed": "red",
    "retrying": "yellow",
    "dead": "red bold",
}

STATUS_SYMBOLS = {
    "queued": ".",
    "running": "*",
    "succeeded": "v",
    "failed": "x",
    "retrying": "~",
    "dead": "!",
}


def output_json(data: Any) -> None:
    if hasattr(data, "model_dump"):
        print(json.dumps(data.model_dump(), default=str))
    else:
        print(json.dumps(data, default=str))


def styled_status(status: str) -> str:
    return f"[{STATUS_COLORS.get(status, 'white')}]{status.upper()}[/]"


def print_error(msg: str) -> None:
    err_console.print(f"[red]Error:[/red] {msg}")
