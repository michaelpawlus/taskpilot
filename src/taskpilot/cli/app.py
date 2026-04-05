from __future__ import annotations

import typer

from taskpilot.cli.cleanup import cleanup
from taskpilot.cli.list import list_tasks
from taskpilot.cli.retry import retry
from taskpilot.cli.show import show
from taskpilot.cli.status import status
from taskpilot.cli.tail import tail

app = typer.Typer(
    name="taskpilot",
    help="Background task tracking for FastAPI. Status, retries, CLI dashboard.",
    no_args_is_help=True,
)

app.command("status")(status)
app.command("list")(list_tasks)
app.command("show")(show)
app.command("retry")(retry)
app.command("tail")(tail)
app.command("cleanup")(cleanup)


if __name__ == "__main__":
    app()
