from __future__ import annotations


def calculate_delay(
    strategy: str,
    attempt: int,
    max_retry_delay: int = 300,
) -> float:
    if strategy == "none":
        return 0.0
    if strategy == "linear":
        return float(attempt * 5)
    if strategy == "exponential":
        return float(min(2**attempt, max_retry_delay))
    raise ValueError(f"Unknown backoff strategy: {strategy}")
