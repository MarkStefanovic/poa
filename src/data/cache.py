import abc
import typing

from src.data.table import Table

__all__ = ("Cache",)


class Cache(abc.ABC):
    @abc.abstractmethod
    def add_increasing_col_value(self, *, col: str, value: typing.Hashable) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def add_key_cols(self, /, key_cols: typing.Iterable[str]) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def add_table_definition(self, /, table: Table) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def add_table_exists(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_latest_incremental_col_values(self) -> dict[str, typing.Hashable]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_key_cols(self) -> tuple[str] | None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_table_definition(self) -> Table | None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_table_exists(self) -> bool | None:
        raise NotImplementedError
