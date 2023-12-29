from __future__ import annotations

import abc
import typing

from src.data.error import Error
from src.data.check_result import CheckResult
from src.data.row import Row
from src.data.row_key import RowKey

__all__ = ("DstDs",)


class DstDs(abc.ABC):
    @abc.abstractmethod
    def add_check_result(self, /, result: CheckResult) -> None | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def add_increasing_col_indices(self, /, increasing_cols: typing.Iterable[str]) -> None | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def create(self) -> None | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def create_history_table(self) -> None | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def create_staging_table(self) -> None | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def delete_rows(self, *, keys: typing.Iterable[RowKey]) -> None | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def drop_table(self) -> None | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_rows(
        self,
        *,
        col_names: typing.Iterable[str] | None,
        after: dict[str, typing.Hashable] | None,
    ) -> list[Row] | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def get_max_values(
        self, /, col_names: typing.Iterable[str]
    ) -> dict[str, typing.Hashable] | None | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def get_row_count(self) -> int | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def table_exists(self) -> bool | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def truncate(self) -> None | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def update_history_table(self) -> None | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def upsert_rows_from_staging(self, /, rows: typing.Iterable[Row]) -> None | Error:
        raise NotImplementedError
