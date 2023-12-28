import contextlib
import pathlib
import typing

import keyring
import psycopg

from src import data

__all__ = ("PgCursorProvider",)


class PgCursorProvider(data.CursorProvider[psycopg.Cursor]):
    def __init__(self, *, db_config: data.DbConfig):
        self._db_config: typing.Final[data.DbConfig] = db_config

    @contextlib.contextmanager
    def open(self) -> typing.Generator[psycopg.Cursor | data.Error, None, None]:
        # noinspection PyBroadException
        try:
            username = keyring.get_password("system", self._db_config.keyring_db_username_entry)
            password = keyring.get_password("system", self._db_config.keyring_db_password_entry)

            con: typing.Final[psycopg.Connection] = psycopg.connect(
                host=self._db_config.host,
                dbname=self._db_config.db_name,
                user=username,
                password=password,
            )
        except:  # noqa: E722
            yield data.Error.new("An error occurred while connecting to the database.")
        else:
            # noinspection PyBroadException
            try:
                with con.cursor() as cur:
                    cur.execute("SET SESSION idle_in_transaction_session_timeout = '15min';")
                    cur.execute("SET SESSION lock_timeout = '5min';")
                    cur.execute("SET SESSION TIME ZONE 'UTC';")
                    yield cur
            except BaseException:
                con.rollback()
            else:
                con.commit()
            finally:
                con.close()


if __name__ == "__main__":
    from src.adapter.config import load
    from src.adapter.fs import get_config_path

    fp = get_config_path()
    cfg = load(config_file=pathlib.Path(r"C:\bu\py\poa\assets\config.json"))
    if isinstance(cfg, data.Error):
        raise Exception(str(cfg))

    ds_cfg = next(c for c in cfg.databases if c.api == data.API.PSYCOPG)

    cp = PgCursorProvider(db_config=ds_cfg)

    with cp.open() as cr:
        if isinstance(cr, data.Error):
            raise Exception(str(cr))

        print(cr.execute("SELECT 1").fetchone())
