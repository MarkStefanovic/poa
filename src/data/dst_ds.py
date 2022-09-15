import abc
import typing

from src.data.row import Row
from src.data.row_key import RowKey
from src.data.table import Table

__all__ = ("DstDs",)


class DstDs(abc.ABC):
    @abc.abstractmethod
    def add_table_def(self, /, table: Table) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def add_increasing_col_indices(self, /, increasing_cols: set[str]) -> None:
        raise NotImplementedError

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
    def get_max_values(self, /, cols: set[str]) -> dict[str, typing.Hashable] | None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_row_count(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def get_table_def(self) -> Table:
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
