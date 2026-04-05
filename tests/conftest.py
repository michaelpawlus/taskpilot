from __future__ import annotations

import os
from pathlib import Path

import pytest

import taskpilot
from taskpilot.store import TaskStore


@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    return tmp_path / "test_taskpilot.db"


@pytest.fixture
async def store(tmp_db: Path) -> TaskStore:
    s = TaskStore(tmp_db)
    await s.connect()
    yield s
    await s.close()


@pytest.fixture
def configured_db(tmp_db: Path) -> Path:
    taskpilot.configure(db_path=str(tmp_db))
    return tmp_db


@pytest.fixture
def cli_runner():
    from typer.testing import CliRunner
    return CliRunner()
