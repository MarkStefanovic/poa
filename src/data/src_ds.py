import abc
import typing

from src.data.error import Error
from src.data.row import Row
from src.data.row_key import RowKey
from src.data.table import Table

__all__ = ("SrcDs",)


class SrcDs(abc.ABC):
    @abc.abstractmethod
    def fetch_rows(
        self,
        *,
        col_names: set[str] | None,
        after: dict[str, typing.Hashable] | None,
    ) -> list[Row] | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_rows_by_key(
        self,
        *,
        col_names: typing.Iterable[str] | None,
        keys: typing.Iterable[RowKey],
    ) -> list[Row] | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def get_row_count(self) -> int | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def get_table(self) -> Table | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def table_exists(self) -> bool | Error:
        raise NotImplementedError
