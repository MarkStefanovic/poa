import pydantic

from src.data.row import Row
from src.data.row_key import RowKey

__all__ = ("RowDiff",)


@pydantic.dataclasses.dataclass(frozen=True, kw_only=True, config=pydantic.ConfigDict(strict=True))
class RowDiff:
    added: dict[RowKey, Row]
    updated: dict[RowKey, tuple[Row, Row]]
    deleted: dict[RowKey, Row]
