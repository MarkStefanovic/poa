import pathlib
import traceback

from loguru import logger

from src import data, adapter

__all__ = ("inspect",)


def inspect(
    *,
    src_db_name: str,
    src_schema_name: str | None,
    src_table_name: str,
    cache_db_name: str,
    pk: list[str],
    log_folder: pathlib.Path,
) -> None:
    try:
        if src_schema_name:
            prefix = f"inspect.{src_db_name}.{src_schema_name}.{src_table_name}."
        else:
            prefix = f"inspect.{src_db_name}.{src_table_name}."

        logger.add(log_folder / f"{prefix}.info.log", rotation="5 MB", retention="7 days", level="INFO")
        logger.add(log_folder / f"{prefix}.error.log", rotation="5 MB", retention="7 days", level="ERROR")

        logger.info(
            f"Starting inspect using the following parameters:\n  {src_db_name=!r}\n  {src_schema_name=!r}\n  "
            f"{src_table_name=!r}\n  {cache_db_name=!r}\n  {pk=!r}\n  log_folder={log_folder.resolve()!s}\n..."
        )

        config_file = adapter.fs.get_config_path()

        src_api = adapter.config.get_api(config_file=config_file, name=src_db_name)
        src_connection_str = adapter.config.get_connection_str(config_file=config_file, name=src_db_name)
        src_cursor_provider = adapter.cursor_provider.create(api=src_api, connection_str=src_connection_str)

        cache_api = adapter.config.get_api(config_file=config_file, name=cache_db_name)
        cache_connection_str = adapter.config.get_connection_str(config_file=config_file, name=cache_db_name)
        cache_cursor_provider = adapter.cursor_provider.create(api=cache_api, connection_str=cache_connection_str)

        _inspect(
            src_api=src_api,
            cache_api=cache_api,
            src_cursor_provider=src_cursor_provider,
            cache_cursor_provider=cache_cursor_provider,
            src_db_name=src_db_name,
            src_schema_name=src_schema_name,
            src_table_name=src_table_name,
            pk=tuple(pk),
        )
    except Exception as ie:
        logger.error(
            f"An error occurred while running inspect({src_db_name=!r}, {src_schema_name=!r}, "
            f"{src_table_name=!r}, {cache_db_name=!r}, {pk=!r}, log_folder='"
            f"{log_folder.resolve()!s}'): {ie!s}\n{traceback.format_exc()}"
        )
        raise


def _inspect(
    *,
    src_api: data.API,
    cache_api: data.API,
    src_cursor_provider: data.CursorProvider,
    cache_cursor_provider: data.CursorProvider,
    src_db_name: str,
    src_schema_name: str | None,
    src_table_name: str,
    pk: tuple[str, ...],
) -> data.Table:
    with src_cursor_provider.open() as src_cur:
        src_ds = adapter.src_ds.create(
            api=src_api,
            cur=src_cur,
            db_name=src_db_name,
            schema_name=src_schema_name,
            table_name=src_table_name,
            pk_cols=tuple(pk),
            after={},
        )

        with cache_cursor_provider.open() as cache_cur:
            cache = adapter.cache.create(api=cache_api, cur=cache_cur)
            if cached_src_table := cache.get_table_def(
                db_name=src_db_name,
                schema_name=src_schema_name,
                table_name=src_table_name,
            ):
                if cached_src_table.pk != tuple(pk):
                    raise Exception(
                        f"The cached primary key columns for {src_table_name}, {', '.join(cached_src_table.pk)} "
                        f"does not match the pk argument, {', '.join(pk)}."
                    )
                return cached_src_table
            else:
                src_table = src_ds.get_table()
                cache.add_table_def(table=src_table)
                return src_table
