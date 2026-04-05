import json
import uuid

from taskpilot.cli.app import app
from taskpilot.store import TaskStore


async def _seed(tmp_db, count=3):
    store = TaskStore(str(tmp_db))
    await store.connect()
    for i in range(count):
        await store.insert_task(
            task_id=str(uuid.uuid4()), function_name="f", args_hash=f"sha256:{i}"
        )
    await store.close()


def test_cleanup_dry_run(cli_runner, tmp_db):
    import asyncio
    asyncio.run(_seed(tmp_db))

    result = cli_runner.invoke(
        app, ["cleanup", "--dry-run", "--json", "--db", str(tmp_db)]
    )
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["deleted"] == 3
    assert data["dry_run"] is True


def test_cleanup_actual(cli_runner, tmp_db):
    import asyncio
    asyncio.run(_seed(tmp_db))

    result = cli_runner.invoke(
        app, ["cleanup", "--json", "--db", str(tmp_db)]
    )
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["deleted"] == 3
    assert data["dry_run"] is False

    # Verify empty
    result2 = cli_runner.invoke(
        app, ["list", "--json", "--db", str(tmp_db)]
    )
    data2 = json.loads(result2.stdout)
    assert data2["total_matching"] == 0


def test_cleanup_empty_db(cli_runner, tmp_db):
    result = cli_runner.invoke(
        app, ["cleanup", "--dry-run", "--json", "--db", str(tmp_db)]
    )
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["deleted"] == 0
