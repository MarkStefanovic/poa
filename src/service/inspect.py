from __future__ import annotations

from src import data, adapter


def inspect(
    src_api: data.API,
    cache_api: data.API,
    src_cursor_provider: data.CursorProvider,
    cache_cursor_provider: data.CursorProvider,
    src_db_name: str,
    src_schema_name: str | None,
    src_table_name: str,
    pk: tuple[str, ...],
) -> None:
    with src_cursor_provider.open() as src_cur:
        src_ds = adapter.src_ds.create(
            api=src_api,
            cur=src_cur,
            db_name=src_db_name,
            schema_name=src_schema_name,
            table_name=src_table_name,
            pk_cols=tuple(pk),
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
