import asyncio
import uuid

import pytest

import taskpilot
from taskpilot.store import TaskStore


async def test_async_function_lifecycle(configured_db):
    @taskpilot.track()
    async def my_task(x: int):
        return x * 2

    await my_task(5)

    store = TaskStore(str(configured_db))
    await store.connect()
    result = await store.list_tasks(function_name="my_task")
    assert result.total_matching == 1
    assert result.tasks[0].status == "succeeded"
    await store.close()


async def test_sync_function_lifecycle(configured_db):
    @taskpilot.track()
    def my_sync_task(x: int):
        return x * 2

    await my_sync_task(5)

    store = TaskStore(str(configured_db))
    await store.connect()
    result = await store.list_tasks(function_name="my_sync_task")
    assert result.total_matching == 1
    assert result.tasks[0].status == "succeeded"
    await store.close()


async def test_exception_marks_failed(configured_db):
    @taskpilot.track()
    async def failing_task():
        raise ValueError("something broke")

    await failing_task()

    store = TaskStore(str(configured_db))
    await store.connect()
    result = await store.list_tasks(function_name="failing_task")
    assert result.total_matching == 1
    assert result.tasks[0].status == "failed"
    assert "something broke" in result.tasks[0].error_message
    await store.close()


async def test_retries_then_succeed(configured_db):
    call_count = 0

    @taskpilot.track(retries=3, backoff="none")
    async def flaky_task():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise RuntimeError("not yet")
        return "done"

    await flaky_task()

    store = TaskStore(str(configured_db))
    await store.connect()
    result = await store.list_tasks(function_name="flaky_task")
    assert result.total_matching == 1
    assert result.tasks[0].status == "succeeded"
    assert result.tasks[0].retry_count == 2

    retries = await store.get_retries(result.tasks[0].task_id)
    assert len(retries) == 2
    await store.close()


async def test_retries_exhausted_marks_dead(configured_db):
    @taskpilot.track(retries=2, backoff="none")
    async def always_fails():
        raise RuntimeError("nope")

    await always_fails()

    store = TaskStore(str(configured_db))
    await store.connect()
    result = await store.list_tasks(function_name="always_fails")
    assert result.total_matching == 1
    assert result.tasks[0].status == "dead"
    assert result.tasks[0].retry_count == 2

    retries = await store.get_retries(result.tasks[0].task_id)
    assert len(retries) == 2
    await store.close()


async def test_store_result_true(configured_db):
    @taskpilot.track(store_result=True)
    async def returning_task():
        return {"message_id": "abc123", "status": "sent"}

    await returning_task()

    store = TaskStore(str(configured_db))
    await store.connect()
    result = await store.list_tasks(function_name="returning_task")
    task_id = result.tasks[0].task_id
    stored = await store.get_result(task_id)
    assert stored == {"message_id": "abc123", "status": "sent"}
    await store.close()


async def test_store_result_false(configured_db):
    @taskpilot.track(store_result=False)
    async def no_store_task():
        return {"data": "ignored"}

    await no_store_task()

    store = TaskStore(str(configured_db))
    await store.connect()
    result = await store.list_tasks(function_name="no_store_task")
    task_id = result.tasks[0].task_id
    stored = await store.get_result(task_id)
    assert stored is None
    await store.close()


async def test_tags_stored(configured_db):
    @taskpilot.track(tags=["email", "onboarding"])
    async def tagged_task():
        pass

    await tagged_task()

    store = TaskStore(str(configured_db))
    await store.connect()
    result = await store.list_tasks(function_name="tagged_task")
    assert result.tasks[0].tags == ["email", "onboarding"]
    await store.close()


async def test_concurrent_execution(configured_db):
    @taskpilot.track()
    async def concurrent_task(n: int):
        await asyncio.sleep(0.01)
        return n

    await asyncio.gather(*[concurrent_task(i) for i in range(5)])

    store = TaskStore(str(configured_db))
    await store.connect()
    result = await store.list_tasks(function_name="concurrent_task")
    assert result.total_matching == 5
    succeeded = [t for t in result.tasks if t.status == "succeeded"]
    assert len(succeeded) == 5
    await store.close()
