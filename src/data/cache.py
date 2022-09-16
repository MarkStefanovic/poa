from __future__ import annotations

import abc

from src.data.table import Table

__all__ = ("Cache",)


class Cache(abc.ABC):
    @abc.abstractmethod
    def add_table_def(self, /, table: Table) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_table_def(self, *, db_name: str, schema_name: str | None, table_name: str) -> Table:
        raise NotImplementedError
