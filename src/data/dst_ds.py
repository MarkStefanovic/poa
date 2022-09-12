import abc
import typing

from src.data.row import Row
from src.data.row_key import RowKey
from src.data.sync_table_spec import SyncTableSpec

__all__ = ("DstDs",)


class DstDs(abc.ABC):
    @abc.abstractmethod
    def create(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def delete_rows(self, *, keys: set[RowKey]) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_rows(self, *, col_names: set[str] | None, after: dict[str, typing.Hashable] | None) -> list[Row]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_increasing_col_values(self) -> dict[str, typing.Hashable] | None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_row_count(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def get_sync_table_spec(self) -> SyncTableSpec:
        raise NotImplementedError

    @abc.abstractmethod
    def table_exists(self) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def truncate(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def upsert_rows(self, /, rows: typing.Iterable[Row]) -> None:
        raise NotImplementedError
