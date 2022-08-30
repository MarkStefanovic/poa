__all__ = ("PoaError", "TableDoesntExist")


class PoaError(Exception):
    """Base class for errors occurring in the poa codebase"""


class TableDoesntExist(PoaError):
    def __init__(self, *, schema_name: str, table_name: str):
        super().__init__(f"The table, {schema_name}.{table_name}, doesn't exist.")

        self._schema_name = schema_name
        self._table_name = table_name
