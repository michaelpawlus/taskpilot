import json
import uuid

import pytest

from taskpilot.cli.app import app
from taskpilot.store import TaskStore


async def _seed(tmp_db, count=3):
    store = TaskStore(str(tmp_db))
    await store.connect()
    for i in range(count):
        tid = str(uuid.uuid4())
        await store.insert_task(task_id=tid, function_name="f", args_hash=f"sha256:{i}")
        if i == 0:
            await store.update_status(tid, "running")
            await store.update_status(tid, "succeeded", duration_ms=100)
    await store.close()


def test_status_json(cli_runner, tmp_db):
    import asyncio
    asyncio.run(_seed(tmp_db))

    result = cli_runner.invoke(app, ["status", "--json", "--db", str(tmp_db)])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["total"] == 3
    assert "by_status" in data


def test_status_human(cli_runner, tmp_db):
    import asyncio
    asyncio.run(_seed(tmp_db))

    result = cli_runner.invoke(app, ["status", "--db", str(tmp_db)])
    assert result.exit_code == 0


def test_status_empty_db(cli_runner, tmp_db):
    result = cli_runner.invoke(app, ["status", "--json", "--db", str(tmp_db)])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["total"] == 0
