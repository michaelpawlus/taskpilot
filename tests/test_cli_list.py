import json
import uuid

from taskpilot.cli.app import app
from taskpilot.store import TaskStore


async def _seed(tmp_db, count=5):
    store = TaskStore(str(tmp_db))
    await store.connect()
    for i in range(count):
        await store.insert_task(
            task_id=str(uuid.uuid4()),
            function_name=f"func_{i % 2}",
            args_hash=f"sha256:{i}",
            tags=["tag_a"] if i % 2 == 0 else None,
        )
    await store.close()


def test_list_json(cli_runner, tmp_db):
    import asyncio
    asyncio.run(_seed(tmp_db))

    result = cli_runner.invoke(app, ["list", "--json", "--db", str(tmp_db)])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["total_matching"] == 5
    assert len(data["tasks"]) == 5


def test_list_with_function_filter(cli_runner, tmp_db):
    import asyncio
    asyncio.run(_seed(tmp_db))

    result = cli_runner.invoke(
        app, ["list", "--json", "--function", "func_0", "--db", str(tmp_db)]
    )
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["total_matching"] == 3


def test_list_with_limit(cli_runner, tmp_db):
    import asyncio
    asyncio.run(_seed(tmp_db))

    result = cli_runner.invoke(
        app, ["list", "--json", "--limit", "2", "--db", str(tmp_db)]
    )
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert len(data["tasks"]) == 2
    assert data["total_matching"] == 5


def test_list_empty(cli_runner, tmp_db):
    result = cli_runner.invoke(app, ["list", "--json", "--db", str(tmp_db)])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["total_matching"] == 0
