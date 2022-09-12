__all__ = ("PoaError", "TableDoesntExist")


class PoaError(Exception):
    """Base class for errors occurring in the poa codebase"""


class SyncTableSpecNotFound(PoaError):
    def __init__(self, db_name: str, schema_name: str | None, table_name: str):
        if schema_name:
            full_table_name = f"{db_name}::{schema_name}.{table_name}"
        else:
            full_table_name = f"{db_name}::{table_name}"

        super().__init__(f"A SyncTableSpec was not found for, {full_table_name}.")

        self._db_name = db_name
        self._schema_name = schema_name
        self._table_name = table_name


class TableDoesntExist(PoaError):
    def __init__(self, *, schema_name: str, table_name: str):
        if schema_name:
            full_table_name = f"{schema_name}.{table_name}"
        else:
            full_table_name = table_name

        super().__init__(f"The table, {full_table_name}, doesn't exist.")

        self._schema_name = schema_name
        self._table_name = table_name
