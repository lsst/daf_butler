from __future__ import annotations

from .core.timespan import Timespan


class Run:
    name: str
    collection_id: int
    origin: int
    timespan: Timespan
    host: str
    environment_id: int
