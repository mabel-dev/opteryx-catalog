from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class View:
    name: str
    definition: str
    properties: dict | None = None
    metadata: Any | None = None
