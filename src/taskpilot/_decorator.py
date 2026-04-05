from __future__ import annotations

import asyncio
import functools
import inspect
import json
import time
import traceback
import uuid
from typing import Any, Callable, Literal

from taskpilot._config import get_config
from taskpilot.backoff import calculate_delay
from taskpilot.serialization import hash_args
from taskpilot.store import TaskStore


def track(
    retries: int | None = None,
    backoff: Literal["none", "linear", "exponential"] | None = None,
    max_retry_delay: int = 300,
    store_result: bool = False,
    tags: list[str] | None = None,
) -> Callable:
    def decorator(func: Callable) -> Callable:
        resolved_retries = retries if retries is not None else get_config().default_retries
        resolved_backoff = backoff if backoff is not None else get_config().default_backoff

        retry_config = {
            "retries": resolved_retries,
            "backoff": resolved_backoff,
            "max_retry_delay": max_retry_delay,
        }

        is_async = inspect.iscoroutinefunction(func)

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            config = get_config()
            db_path = config.resolve_db_path()
            store = TaskStore(db_path)
            await store.connect()

            task_id = str(uuid.uuid4())
            args_h = hash_args(args, kwargs)

            try:
                await store.insert_task(
                    task_id=task_id,
                    function_name=func.__name__,
                    args_hash=args_h,
                    tags=tags,
                    retry_config=retry_config,
                )

                await store.update_status(task_id, "running")
                start = time.monotonic()

                attempt = 0
                last_error: Exception | None = None

                while True:
                    try:
                        if is_async:
                            result = await func(*args, **kwargs)
                        else:
                            loop = asyncio.get_event_loop()
                            result = await loop.run_in_executor(
                                None, functools.partial(func, *args, **kwargs)
                            )

                        elapsed_ms = int((time.monotonic() - start) * 1000)

                        await store.update_status(
                            task_id, "succeeded", duration_ms=elapsed_ms,
                            retry_count=attempt,
                        )

                        if store_result and result is not None:
                            result_json = config.serialize_json(result)
                            await store.store_result(task_id, result_json)

                        return result

                    except Exception as exc:
                        last_error = exc
                        tb = traceback.format_exc()
                        elapsed_ms = int((time.monotonic() - start) * 1000)

                        if attempt < resolved_retries:
                            delay = calculate_delay(
                                resolved_backoff, attempt + 1, max_retry_delay
                            )

                            await store.insert_retry(
                                task_id=task_id,
                                attempt=attempt + 1,
                                status="failed",
                                error_message=str(exc),
                                error_traceback=tb,
                                delay_seconds=delay,
                                duration_ms=elapsed_ms,
                            )

                            await store.update_status(
                                task_id, "retrying",
                                error_message=str(exc),
                                error_traceback=tb,
                                retry_count=attempt + 1,
                            )

                            if delay > 0:
                                await asyncio.sleep(delay)

                            attempt += 1
                            start = time.monotonic()
                            continue

                        await store.update_status(
                            task_id,
                            "dead" if resolved_retries > 0 else "failed",
                            error_message=str(exc),
                            error_traceback=tb,
                            duration_ms=elapsed_ms,
                            retry_count=attempt,
                        )
                        return None
            finally:
                await store.close()

        wrapper._taskpilot_tracked = True  # type: ignore[attr-defined]
        wrapper._taskpilot_retry_config = retry_config  # type: ignore[attr-defined]
        wrapper._taskpilot_tags = tags  # type: ignore[attr-defined]
        wrapper._taskpilot_store_result = store_result  # type: ignore[attr-defined]
        wrapper._taskpilot_func = func  # type: ignore[attr-defined]

        return wrapper

    return decorator
