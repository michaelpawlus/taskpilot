import asyncio
import json
import uuid

import pytest

from taskpilot.store import TaskStore


async def test_tail_picks_up_changes(tmp_db):
    """Test that tail detects task changes via polling."""
    store = TaskStore(str(tmp_db))
    await store.connect()

    task_id = str(uuid.uuid4())
    await store.insert_task(
        task_id=task_id, function_name="tail_func", args_hash="sha256:abc"
    )

    changes = await store.get_recent_changes(
        since="2000-01-01T00:00:00.000Z",
        status=None,
        function_name=None,
    )
    assert len(changes) == 1
    assert changes[0].task_id == task_id

    await store.update_status(task_id, "running")
    last_ts = changes[0].updated_at
    changes2 = await store.get_recent_changes(since=last_ts)
    assert len(changes2) == 1
    assert changes2[0].status == "running"

    await store.close()
