import pytest

import taskpilot
from taskpilot.middleware import TaskPilotMiddleware
from taskpilot.store import TaskStore


@pytest.fixture
def test_app(tmp_db):
    from fastapi import FastAPI, Request

    app = FastAPI()
    app.add_middleware(TaskPilotMiddleware, db_path=str(tmp_db))

    @taskpilot.track(tags=["test"])
    async def background_work(value: int):
        return value * 2

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.post("/run")
    async def run_task(request: Request):
        task_id = await request.state.taskpilot.run(background_work, value=42)
        return {"task_id": task_id}

    @app.get("/task/{task_id}")
    async def get_task(task_id: str, request: Request):
        return await request.state.taskpilot.status(task_id)

    return app


@pytest.fixture
async def async_client(test_app):
    from httpx import ASGITransport, AsyncClient

    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_health_endpoint(async_client):
    resp = await async_client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_run_returns_task_id(async_client):
    resp = await async_client.post("/run")
    assert resp.status_code == 200
    data = resp.json()
    assert "task_id" in data
    assert len(data["task_id"]) == 36  # UUID


async def test_task_appears_in_db(async_client, tmp_db):
    resp = await async_client.post("/run")
    task_id = resp.json()["task_id"]

    store = TaskStore(str(tmp_db))
    await store.connect()
    task = await store.get_task(task_id)
    assert task is not None
    assert task.function_name == "background_work"
    await store.close()
