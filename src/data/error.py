from __future__ import annotations

__all__ = ("PoaError", "TableDoesntExist")


class PoaError(Exception):
    """Base class for errors occurring in the poa codebase"""


class CheckError(PoaError):
    """Error arising from the check service."""


class TableDoesntExist(PoaError):
    def __init__(self, *, schema_name: str | None, table_name: str):
        if schema_name:
            full_table_name = f"{schema_name}.{table_name}"
        else:
            full_table_name = table_name

        super().__init__(f"The table, {full_table_name}, doesn't exist.")


class UnrecognizedDatabaseAPI(PoaError):
    def __init__(self, *, api: str):
        super().__init__(f"The database api specified, {api}, was not recognized.")
