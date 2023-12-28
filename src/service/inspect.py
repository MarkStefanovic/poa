import typing

from src import data, adapter

__all__ = ("inspect",)


def inspect(
    *,
    src_config: data.DbConfig,
    src_schema_name: str | None,
    src_table_name: str,
    dst_config: data.DbConfig,
    pk: typing.Iterable[str],
) -> data.Table | data.Error:
    try:
        cursor_provider = adapter.cursor_provider.create(db_config=src_config)
        if isinstance(cursor_provider, data.Error):
            return cursor_provider

        pk_cols = tuple(pk)

        with cursor_provider.open() as cur:
            src = adapter.src_ds.create(
                cur=cur,
                api=src_config.api,
                db_name=src_config.db_name,
                schema_name=src_schema_name,
                table_name=src_table_name,
                pk_cols=pk_cols,
                after=dict(),
            )
            if isinstance(src, data.Error):
                return src

            cache_db_cursor_provider = adapter.cursor_provider.create(db_config=dst_config)
            if isinstance(cache_db_cursor_provider, data.Error):
                return cache_db_cursor_provider

            with cache_db_cursor_provider.open() as cache_cur:
                cache = adapter.cache.create(cur=cache_cur, api=dst_config.api)
                if isinstance(cache, data.Error):
                    return cache

                cached_src_table = cache.get_table_def(
                    db_name=src_config.db_name,
                    schema_name=src_schema_name,
                    table_name=src_table_name,
                )
                if isinstance(cached_src_table, data.Error):
                    return cached_src_table

                if cached_src_table is None:
                    src_table = src.get_table()
                    cache.add_table(table=src_table)
                    return src_table

                if sorted(cached_src_table.pk) != sorted(pk_cols):
                    return data.Error.new(
                        f"The cached primary key columns for {src_table_name}, {', '.join(cached_src_table.pk)} "
                        f"does not match the pk argument, {', '.join(pk)}."
                    )
                return cached_src_table
    except Exception as e:
        return data.Error.new(
            str(e),
        )
