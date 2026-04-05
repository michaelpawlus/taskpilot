# Claude Code Notes -- taskpilot

## Running Tests

    .venv/bin/pytest

For verbose with coverage:

    .venv/bin/pytest -v --tb=short

## Project Overview

taskpilot is a drop-in FastAPI middleware and CLI that wraps BackgroundTasks with
automatic status tracking, retry logic, and a terminal dashboard. All state in
SQLite -- zero infrastructure.

## Architecture

- `src/taskpilot/_decorator.py` -- `@taskpilot.track` decorator. Core lifecycle.
- `src/taskpilot/store.py` -- `TaskStore` class. All SQLite operations via aiosqlite.
- `src/taskpilot/middleware.py` -- ASGI middleware. DB init, request context, cleanup loop.
- `src/taskpilot/cli/` -- Typer CLI. Each command in its own module.
- `src/taskpilot/api.py` -- Optional FastAPI router factory.

## CLI Commands

    taskpilot status [--since DURATION] [--json] [--db PATH]
    taskpilot list [--status STATUS] [--since DURATION] [--function NAME] [--tags TEXT] [--limit N] [--offset N] [--json] [--db PATH]
    taskpilot show TASK_ID [--json] [--db PATH]
    taskpilot retry TASK_ID [--json] [--db PATH]
    taskpilot tail [--poll-interval FLOAT] [--status STATUS] [--function NAME] [--json] [--db PATH]
    taskpilot cleanup [--older-than DURATION] [--status STATUS] [--dry-run] [--json] [--db PATH]
    taskpilot dashboard [--refresh FLOAT] [--db PATH]

All commands support `--json` (JSON to stdout, human text to stderr).
Exit codes: 0 success, 1 error, 2 not found.

## Key Design Decisions

1. SQLite only -- no Redis, no RabbitMQ. WAL mode for concurrent read/write.
2. Decorator wraps, doesn't replace -- works with FastAPI's BackgroundTasks.add_task().
3. CLI reads DB directly -- doesn't need the FastAPI app running (except retry enqueue).
4. Pydantic models for everything -- CLI JSON output is model.model_dump_json().

## Database

SQLite location: `--db` flag > `TASKPILOT_DB` env var > `./taskpilot.db`
Tables: taskpilot_tasks, taskpilot_task_retries, taskpilot_task_results, taskpilot_meta.

## Testing Conventions

- Every test uses a fresh tmp SQLite database (conftest.py tmp_db fixture)
- Middleware tests use httpx AsyncClient with test FastAPI app
- CLI tests use typer.testing.CliRunner
- Async tests use pytest-asyncio with auto mode
