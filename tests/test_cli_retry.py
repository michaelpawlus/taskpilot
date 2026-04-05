import json
import uuid

from taskpilot.cli.app import app
from taskpilot.store import TaskStore


async def _seed_failed(tmp_db):
    store = TaskStore(str(tmp_db))
    await store.connect()
    task_id = str(uuid.uuid4())
    await store.insert_task(
        task_id=task_id, function_name="retry_func", args_hash="sha256:abc",
    )
    await store.update_status(task_id, "running")
    await store.update_status(task_id, "failed", error_message="boom")
    await store.close()
    return task_id


async def _seed_succeeded(tmp_db):
    store = TaskStore(str(tmp_db))
    await store.connect()
    task_id = str(uuid.uuid4())
    await store.insert_task(
        task_id=task_id, function_name="ok_func", args_hash="sha256:abc",
    )
    await store.update_status(task_id, "running")
    await store.update_status(task_id, "succeeded", duration_ms=50)
    await store.close()
    return task_id


def test_retry_failed_task(cli_runner, tmp_db):
    import asyncio
    task_id = asyncio.run(_seed_failed(tmp_db))

    result = cli_runner.invoke(
        app, ["retry", task_id, "--json", "--db", str(tmp_db)]
    )
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["original_task_id"] == task_id
    assert data["status"] == "queued"
    assert data["new_task_id"] != task_id


def test_retry_succeeded_rejected(cli_runner, tmp_db):
    import asyncio
    task_id = asyncio.run(_seed_succeeded(tmp_db))

    result = cli_runner.invoke(
        app, ["retry", task_id, "--json", "--db", str(tmp_db)]
    )
    assert result.exit_code == 1


def test_retry_not_found(cli_runner, tmp_db):
    result = cli_runner.invoke(
        app, ["retry", str(uuid.uuid4()), "--json", "--db", str(tmp_db)]
    )
    assert result.exit_code == 2
