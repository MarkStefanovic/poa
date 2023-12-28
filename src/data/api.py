import enum

__all__ = ("API",)


class API(enum.Enum):
    HH = "hh"
    MSSQL = "mssql"
    PYODBC = "pyodbc"
    PSYCOPG = "psycopg"

    def __repr__(self) -> str:
        return f"API.{self.name}"

    def __str__(self) -> str:
        return self.value


if __name__ == "__main__":
    print(repr(API.PYODBC))
