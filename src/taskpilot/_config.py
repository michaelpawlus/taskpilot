from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable

from pydantic import BaseModel


class TaskPilotConfig(BaseModel):
    db_path: str = "./taskpilot.db"
    table_prefix: str = "taskpilot_"
    json_serializer: Callable[[Any], str] | None = None
    json_deserializer: Callable[[str], Any] | None = None
    default_retries: int = 0
    default_backoff: str = "none"

    model_config = {"arbitrary_types_allowed": True}

    def resolve_db_path(self, override: str | None = None) -> str:
        if override:
            return override
        env = os.environ.get("TASKPILOT_DB")
        if env:
            return env
        return self.db_path

    def serialize_json(self, obj: Any) -> str:
        if self.json_serializer:
            return self.json_serializer(obj)
        return json.dumps(obj, default=str)

    def deserialize_json(self, s: str) -> Any:
        if self.json_deserializer:
            return self.json_deserializer(s)
        return json.loads(s)


_config = TaskPilotConfig()


def get_config() -> TaskPilotConfig:
    return _config


def configure(
    db_path: str = "./taskpilot.db",
    table_prefix: str = "taskpilot_",
    json_serializer: Callable[[Any], str] | None = None,
    json_deserializer: Callable[[str], Any] | None = None,
    default_retries: int = 0,
    default_backoff: str = "none",
) -> None:
    global _config
    _config = TaskPilotConfig(
        db_path=db_path,
        table_prefix=table_prefix,
        json_serializer=json_serializer,
        json_deserializer=json_deserializer,
        default_retries=default_retries,
        default_backoff=default_backoff,
    )
