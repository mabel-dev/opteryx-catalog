from __future__ import annotations

from dataclasses import dataclass


@dataclass
class View:
    name: str
    definition: str
    properties: dict | None = None
