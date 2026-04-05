from __future__ import annotations

SCHEMA_VERSION = "1"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS taskpilot_tasks (
    task_id TEXT PRIMARY KEY,
    parent_task_id TEXT,
    function_name TEXT NOT NULL,
    args_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    tags TEXT,
    retry_config TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    error_traceback TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    started_at TEXT,
    completed_at TEXT,
    duration_ms INTEGER,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    FOREIGN KEY (parent_task_id) REFERENCES taskpilot_tasks(task_id)
);

CREATE INDEX IF NOT EXISTS idx_tasks_status ON taskpilot_tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_function ON taskpilot_tasks(function_name);
CREATE INDEX IF NOT EXISTS idx_tasks_created ON taskpilot_tasks(created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_updated ON taskpilot_tasks(updated_at);
CREATE INDEX IF NOT EXISTS idx_tasks_parent ON taskpilot_tasks(parent_task_id);

CREATE TABLE IF NOT EXISTS taskpilot_task_retries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    status TEXT NOT NULL,
    error_message TEXT,
    error_traceback TEXT,
    delay_seconds REAL,
    attempted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    duration_ms INTEGER,
    FOREIGN KEY (task_id) REFERENCES taskpilot_tasks(task_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_retries_task ON taskpilot_task_retries(task_id);

CREATE TABLE IF NOT EXISTS taskpilot_task_results (
    task_id TEXT PRIMARY KEY,
    result_json TEXT NOT NULL,
    stored_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    FOREIGN KEY (task_id) REFERENCES taskpilot_tasks(task_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS taskpilot_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT OR IGNORE INTO taskpilot_meta (key, value) VALUES ('schema_version', '1');
"""


async def init_schema(db) -> None:
    """Initialize the database schema. Accepts an aiosqlite connection."""
    await db.executescript(SCHEMA_SQL)
    await db.commit()
