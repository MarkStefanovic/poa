import abc
import typing

from src.data.error import Error
from src.data.row import Row

__all__ = ("Cursor",)


class Cursor(abc.ABC):
    @abc.abstractmethod
    def execute(
        self,
        *,
        sql: str,
        params: typing.Iterable[typing.Hashable] | None,
    ) -> None | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def execute_many(
        self,
        *,
        sql: str,
        params: typing.Iterable[typing.Iterable[typing.Hashable]],
    ) -> None | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_one(
        self,
        *,
        sql: str,
        params: typing.Iterable[typing.Hashable] | None,
    ) -> Row | None | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_all(
        self,
        *,
        sql: str,
        params: typing.Iterable[typing.Hashable] | None,
    ) -> tuple[Row, ...] | Error:
        raise NotImplementedError
