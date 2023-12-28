from src import data
from src.adapter.cursor_provider.pg import PgCursorProvider
from src.adapter.log.pg import PgLog

__all__ = ("create",)


def create(*, db_config: data.DbConfig) -> data.Log | data.Error:
    try:
        if db_config.api == data.API.PSYCOPG:
            cursor_provider = PgCursorProvider(db_config=db_config)

            return PgLog(cursor_provider=cursor_provider)

        return data.Error.new(
            f"The Log interface has not been implemented for the {db_config.api} api.",
            db_config=db_config,
        )
    except Exception as e:
        return data.Error.new(str(e), db_config=db_config)
