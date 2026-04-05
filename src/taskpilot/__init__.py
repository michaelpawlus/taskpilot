"""taskpilot - Drop-in FastAPI background task tracking with SQLite."""

from taskpilot._config import configure
from taskpilot._decorator import track

__all__ = ["configure", "track"]
__version__ = "0.1.0"
