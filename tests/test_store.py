import uuid

import pytest

from taskpilot.store import TaskStore


async def test_insert_and_get(store: TaskStore):
    task_id = str(uuid.uuid4())
    record = await store.insert_task(
        task_id=task_id,
        function_name="test_func",
        args_hash="sha256:abc123",
        tags=["test", "unit"],
    )
    assert record.task_id == task_id
    assert record.status == "queued"
    assert record.tags == ["test", "unit"]

    fetched = await store.get_task(task_id)
    assert fetched is not None
    assert fetched.function_name == "test_func"


async def test_get_by_prefix(store: TaskStore):
    task_id = str(uuid.uuid4())
    await store.insert_task(
        task_id=task_id, function_name="test_func", args_hash="sha256:abc"
    )
    result = await store.get_task_by_prefix(task_id[:8])
    assert not isinstance(result, list)
    assert result.task_id == task_id


async def test_get_by_prefix_not_found(store: TaskStore):
    result = await store.get_task_by_prefix("nonexist")
    assert isinstance(result, list) and len(result) == 0


async def test_get_by_prefix_ambiguous(store: TaskStore):
    prefix = "aaaa1111"
    id1 = prefix + str(uuid.uuid4())[8:]
    id2 = prefix + str(uuid.uuid4())[8:]
    await store.insert_task(task_id=id1, function_name="f1", args_hash="sha256:a")
    await store.insert_task(task_id=id2, function_name="f2", args_hash="sha256:b")
    result = await store.get_task_by_prefix(prefix)
    assert isinstance(result, list) and len(result) == 2


async def test_update_status(store: TaskStore):
    task_id = str(uuid.uuid4())
    await store.insert_task(
        task_id=task_id, function_name="f", args_hash="sha256:x"
    )
    await store.update_status(task_id, "running")
    task = await store.get_task(task_id)
    assert task.status == "running"
    assert task.started_at is not None

    await store.update_status(task_id, "succeeded", duration_ms=100)
    task = await store.get_task(task_id)
    assert task.status == "succeeded"
    assert task.completed_at is not None
    assert task.duration_ms == 100


async def test_list_tasks_with_filters(store: TaskStore):
    for i in range(5):
        await store.insert_task(
            task_id=str(uuid.uuid4()),
            function_name="func_a" if i < 3 else "func_b",
            args_hash=f"sha256:{i}",
        )
    await store.update_status(
        (await store.list_tasks(limit=1)).tasks[0].task_id, "succeeded"
    )

    result = await store.list_tasks(function_name="func_a")
    assert result.total_matching == 3

    result = await store.list_tasks(status="queued")
    assert result.total_matching == 4

    result = await store.list_tasks(limit=2)
    assert len(result.tasks) == 2
    assert result.total_matching == 5


async def test_insert_retry_and_get(store: TaskStore):
    task_id = str(uuid.uuid4())
    await store.insert_task(
        task_id=task_id, function_name="f", args_hash="sha256:x"
    )
    await store.insert_retry(
        task_id=task_id, attempt=1, status="failed",
        error_message="timeout", delay_seconds=2.0,
    )
    retries = await store.get_retries(task_id)
    assert len(retries) == 1
    assert retries[0].attempt == 1
    assert retries[0].error_message == "timeout"


async def test_store_and_get_result(store: TaskStore):
    task_id = str(uuid.uuid4())
    await store.insert_task(
        task_id=task_id, function_name="f", args_hash="sha256:x"
    )
    await store.store_result(task_id, '{"key": "value"}')
    result = await store.get_result(task_id)
    assert result == {"key": "value"}


async def test_get_result_not_found(store: TaskStore):
    result = await store.get_result("nonexistent")
    assert result is None


async def test_get_status_summary(store: TaskStore):
    for i in range(3):
        tid = str(uuid.uuid4())
        await store.insert_task(
            task_id=tid, function_name="f", args_hash=f"sha256:{i}"
        )
        if i == 0:
            await store.update_status(tid, "running")
            await store.update_status(tid, "succeeded", duration_ms=100)
        elif i == 1:
            await store.update_status(tid, "running")
            await store.update_status(tid, "failed", error_message="err")

    summary = await store.get_status_summary()
    assert summary.total == 3
    assert summary.by_status["succeeded"] == 1
    assert summary.by_status["failed"] == 1
    assert summary.by_status["queued"] == 1


async def test_cleanup(store: TaskStore):
    for i in range(3):
        await store.insert_task(
            task_id=str(uuid.uuid4()), function_name="f", args_hash=f"sha256:{i}"
        )

    result = await store.cleanup(dry_run=True)
    assert result.deleted == 3
    assert result.dry_run is True

    check = await store.list_tasks()
    assert check.total_matching == 3

    result = await store.cleanup()
    assert result.deleted == 3
    assert result.dry_run is False

    check = await store.list_tasks()
    assert check.total_matching == 0


async def test_cleanup_by_status(store: TaskStore):
    tid1 = str(uuid.uuid4())
    tid2 = str(uuid.uuid4())
    await store.insert_task(task_id=tid1, function_name="f", args_hash="sha256:a")
    await store.insert_task(task_id=tid2, function_name="f", args_hash="sha256:b")
    await store.update_status(tid1, "running")
    await store.update_status(tid1, "succeeded", duration_ms=50)

    result = await store.cleanup(status="succeeded")
    assert result.deleted == 1

    check = await store.list_tasks()
    assert check.total_matching == 1
    assert check.tasks[0].task_id == tid2


async def test_get_task_detail(store: TaskStore):
    task_id = str(uuid.uuid4())
    await store.insert_task(
        task_id=task_id, function_name="f", args_hash="sha256:x",
        tags=["a", "b"],
    )
    await store.update_status(task_id, "running")
    await store.insert_retry(
        task_id=task_id, attempt=1, status="failed",
        error_message="err", delay_seconds=1.0,
    )
    await store.store_result(task_id, '{"ok": true}')
    await store.update_status(task_id, "succeeded", duration_ms=200)

    detail = await store.get_task_detail(task_id)
    assert detail is not None
    assert detail.function_name == "f"
    assert detail.tags == ["a", "b"]
    assert len(detail.retries) == 1
    assert detail.result == {"ok": True}
