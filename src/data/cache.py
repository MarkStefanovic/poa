import abc

from src.data.error import Error
from src.data.table import Table

__all__ = ("Cache",)


class Cache(abc.ABC):
    @abc.abstractmethod
    def add_table(self, /, table: Table) -> None | Error:
        raise NotImplementedError

    @abc.abstractmethod
    def get_table_def(
        self,
        *,
        db_name: str | None,
        schema_name: str | None,
        table_name: str,
    ) -> Table | None | Error:
        raise NotImplementedError
