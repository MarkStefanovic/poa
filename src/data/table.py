from __future__ import annotations

import dataclasses

from src.data.column import Column

__all__ = ("Table",)


@dataclasses.dataclass(frozen=True, kw_only=True)
class Table:
    db_name: str
    schema_name: str | None
    table_name: str
    pk: tuple[str, ...]
    columns: frozenset[Column]
