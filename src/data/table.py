import dataclasses

from src.data.column import Column

__all__ = ("Table",)


@dataclasses.dataclass(frozen=True, kw_only=True)
class Table:
    schema_name: str
    table_name: str
    pk: tuple[str]
    columns: frozenset[Column]
