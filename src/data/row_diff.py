import dataclasses

from src.data.row import Row
from src.data.row_key import RowKey

__all__ = ("RowDiff",)


@dataclasses.dataclass(frozen=True, kw_only=True)
class RowDiff:
    added: dict[RowKey, Row]
    updated: dict[RowKey, tuple[Row, Row]]
    deleted: dict[RowKey, Row]
