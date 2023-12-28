import abc
import typing

import pydantic

from src.data.cursor import Cursor
from src.data.error import Error
from src.data.table import Table

__all__ = ("Cache",)


class Cache(abc.ABC, typing.Generic[Cursor]):
    @abc.abstractmethod
    def add_table(self, /, table: Table) -> None | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def get_table_def(
        self,
        *,
        db_name: str,
        schema_name: str,
        table_name: str,
    ) -> Table | None | Error:
        raise NotImplementedError
