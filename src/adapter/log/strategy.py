from src import data
from src.adapter.cursor_provider.pg import PgCursorProvider
from src.adapter.log.pg import PgLog

__all__ = ("create",)


def create(*, api: data.API, cursor_provider: data.CursorProvider) -> data.Log:
    if api.PSYCOPG2:
        assert isinstance(cursor_provider, PgCursorProvider), f"log.create expects a PgCursorProvider, but got a {type(cursor_provider)}."
        return PgLog(cursor_provider=cursor_provider)

    raise NotImplementedError(f"The Log interface has not been implemented for the {api} api.")
