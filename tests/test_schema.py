import aiosqlite

from taskpilot.schema import init_schema


async def test_schema_creates_tables(tmp_db):
    db = await aiosqlite.connect(str(tmp_db))
    db.row_factory = aiosqlite.Row
    await init_schema(db)

    async with db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ) as cursor:
        tables = [row["name"] for row in await cursor.fetchall()]

    assert "taskpilot_tasks" in tables
    assert "taskpilot_task_retries" in tables
    assert "taskpilot_task_results" in tables
    assert "taskpilot_meta" in tables
    await db.close()


async def test_schema_version(tmp_db):
    db = await aiosqlite.connect(str(tmp_db))
    db.row_factory = aiosqlite.Row
    await init_schema(db)

    async with db.execute(
        "SELECT value FROM taskpilot_meta WHERE key = 'schema_version'"
    ) as cursor:
        row = await cursor.fetchone()
        assert row["value"] == "1"

    await db.close()


async def test_schema_idempotent(tmp_db):
    db = await aiosqlite.connect(str(tmp_db))
    db.row_factory = aiosqlite.Row
    await init_schema(db)
    await init_schema(db)  # Should not raise
    await db.close()
