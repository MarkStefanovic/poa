import pydantic

from src.data.column import Column

__all__ = ("Table",)


@pydantic.dataclasses.dataclass(frozen=True, kw_only=True, config=pydantic.ConfigDict(strict=True))
class Table:
    db_name: str
    schema_name: str | None
    table_name: str
    pk: tuple[str, ...]
    columns: frozenset[Column]
