import uuid

import pytest

from taskpilot.store import TaskStore


@pytest.fixture
def api_app(tmp_db):
    from fastapi import FastAPI

    from taskpilot.api import create_task_router

    app = FastAPI()
    router = create_task_router(db_path=str(tmp_db), prefix="/tasks")
    app.include_router(router)
    return app


@pytest.fixture
async def api_client(api_app):
    from httpx import ASGITransport, AsyncClient

    transport = ASGITransport(app=api_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def _seed_task(tmp_db, status="queued", function_name="test_func"):
    store = TaskStore(str(tmp_db))
    await store.connect()
    task_id = str(uuid.uuid4())
    await store.insert_task(
        task_id=task_id, function_name=function_name, args_hash="sha256:abc"
    )
    if status != "queued":
        await store.update_status(task_id, "running")
        if status in ("succeeded", "failed", "dead"):
            await store.update_status(
                task_id, status,
                duration_ms=100 if status == "succeeded" else None,
                error_message="err" if status in ("failed", "dead") else None,
            )
    await store.close()
    return task_id


async def test_list_tasks(api_client, tmp_db):
    await _seed_task(tmp_db)
    resp = await api_client.get("/tasks/")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_matching"] == 1


async def test_status_summary(api_client, tmp_db):
    await _seed_task(tmp_db)
    resp = await api_client.get("/tasks/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1


async def test_get_task(api_client, tmp_db):
    task_id = await _seed_task(tmp_db)
    resp = await api_client.get(f"/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["task_id"] == task_id


async def test_get_task_not_found(api_client):
    resp = await api_client.get(f"/tasks/{uuid.uuid4()}")
    assert resp.status_code == 404


async def test_retry_failed_task(api_client, tmp_db):
    task_id = await _seed_task(tmp_db, status="failed")
    resp = await api_client.post(f"/tasks/{task_id}/retry")
    assert resp.status_code == 200
    data = resp.json()
    assert data["original_task_id"] == task_id
    assert data["status"] == "queued"


async def test_retry_succeeded_task_rejected(api_client, tmp_db):
    task_id = await _seed_task(tmp_db, status="succeeded")
    resp = await api_client.post(f"/tasks/{task_id}/retry")
    assert resp.status_code == 400


async def test_cleanup_dry_run(api_client, tmp_db):
    await _seed_task(tmp_db)
    resp = await api_client.delete("/tasks/cleanup", params={"dry_run": True})
    assert resp.status_code == 200
    data = resp.json()
    assert data["deleted"] == 1
    assert data["dry_run"] is True
