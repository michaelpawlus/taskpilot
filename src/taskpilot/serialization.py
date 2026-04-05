from __future__ import annotations

import hashlib
import json
from typing import Any


def hash_args(args: tuple, kwargs: dict[str, Any]) -> str:
    normalized = json.dumps(
        {"args": args, "kwargs": kwargs},
        sort_keys=True,
        default=str,
    )
    digest = hashlib.sha256(normalized.encode()).hexdigest()
    return f"sha256:{digest}"
