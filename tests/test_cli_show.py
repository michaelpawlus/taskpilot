import json
import uuid

from taskpilot.cli.app import app
from taskpilot.store import TaskStore


async def _seed_one(tmp_db):
    store = TaskStore(str(tmp_db))
    await store.connect()
    task_id = str(uuid.uuid4())
    await store.insert_task(
        task_id=task_id, function_name="show_func", args_hash="sha256:abc",
        tags=["demo"],
    )
    await store.update_status(task_id, "running")
    await store.update_status(task_id, "succeeded", duration_ms=150)
    await store.close()
    return task_id


def test_show_full_id(cli_runner, tmp_db):
    import asyncio
    task_id = asyncio.run(_seed_one(tmp_db))

    result = cli_runner.invoke(app, ["show", task_id, "--json", "--db", str(tmp_db)])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["task_id"] == task_id
    assert data["status"] == "succeeded"


def test_show_prefix(cli_runner, tmp_db):
    import asyncio
    task_id = asyncio.run(_seed_one(tmp_db))

    result = cli_runner.invoke(
        app, ["show", task_id[:8], "--json", "--db", str(tmp_db)]
    )
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["task_id"] == task_id


def test_show_not_found(cli_runner, tmp_db):
    result = cli_runner.invoke(
        app, ["show", str(uuid.uuid4()), "--json", "--db", str(tmp_db)]
    )
    assert result.exit_code == 2


def test_show_short_prefix(cli_runner, tmp_db):
    result = cli_runner.invoke(
        app, ["show", "abc", "--json", "--db", str(tmp_db)]
    )
    assert result.exit_code == 1
