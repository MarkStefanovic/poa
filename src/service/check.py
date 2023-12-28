import datetime

from src import data, adapter

__all__ = ("check",)


def check(
    *,
    src_db_config: data.DbConfig,
    src_schema_name: str | None,
    src_table_name: str,
    dst_db_config: data.DbConfig,
    dst_schema_name: str | None,
    dst_table_name: str,
    pk: list[str],
    after: dict[str, datetime.date],
    batch_ts: datetime.datetime,
) -> None | data.Error:
    try:
        log_cursor_provider = adapter.cursor_provider.create(db_config=dst_db_config)
        if isinstance(log_cursor_provider, data.Error):
            return log_cursor_provider

        log = adapter.log.create(db_config=dst_db_config)
        if isinstance(log, data.Error):
            return log

        src_cursor_provider = adapter.cursor_provider.create(db_config=src_db_config)
        if isinstance(src_cursor_provider, data.Error):
            return src_cursor_provider

        dst_cursor_provider = adapter.cursor_provider.create(db_config=dst_db_config)
        if isinstance(dst_cursor_provider, data.Error):
            return dst_cursor_provider

        with src_cursor_provider.open() as src_cur, dst_cursor_provider.open() as dst_cur:
            if isinstance(src_cursor_provider, data.Error):
                return src_cursor_provider

            if isinstance(dst_cursor_provider, data.Error):
                return dst_cursor_provider

            src_ds = adapter.src_ds.create(
                api=src_db_config.api,
                cur=src_cur,
                db_name=src_db_config.db_name,
                schema_name=src_schema_name,
                table_name=src_table_name,
                pk_cols=tuple(pk),
                after=after,
            )
            if isinstance(src_ds, data.Error):
                return src_ds

            cache = adapter.cache.create(cur=dst_cur, api=dst_db_config.api)
            if isinstance(cache, data.Error):
                return cache

            cached_src_table = cache.get_table_def(
                db_name=src_db_config.db_name,
                schema_name=src_schema_name,
                table_name=src_table_name,
            )
            if isinstance(cached_src_table, data.Error):
                return cached_src_table

            if cached_src_table:
                if cached_src_table.pk != tuple(pk):
                    return data.Error.new(
                        f"The cached primary key columns for {src_table_name}, "
                        f"{', '.join(cached_src_table.pk)} does not match the pk argument, {', '.join(pk)}.",
                        src_db_config=src_db_config,
                        src_db_name=src_db_config.db_name,
                        src_schema_name=src_schema_name,
                        src_table_name=src_table_name,
                        dst_db_config=dst_db_config,
                        dst_db_name=dst_db_config.db_name,
                        dst_schema_name=dst_schema_name,
                        dst_table_name=dst_table_name,
                        pk=tuple(pk),
                        after=tuple(after.items()),
                        batch_ts=batch_ts,
                    )

                src_table = cached_src_table
            else:
                src_table = src_ds.get_table()

                add_table_result = cache.add_table(table=src_table)
                if isinstance(add_table_result, data.Error):
                    return add_table_result

            dst_ds = adapter.dst_ds.create(
                api=dst_db_config.api,
                cur=dst_cur,
                dst_db_name=dst_db_config.db_name,
                dst_schema_name=dst_schema_name,
                dst_table_name=dst_table_name,
                src_table=src_table,
                batch_ts=batch_ts,
                after=after,
            )
            if isinstance(dst_ds, data.Error):
                return dst_ds

            result = _check(
                src_ds=src_ds,
                dst_ds=dst_ds,
                src_db_name=src_db_config.db_name,
                src_schema_name=src_schema_name,
                src_table_name=src_table_name,
                dst_db_name=dst_db_config.db_name,
                dst_schema_name=dst_schema_name,
                dst_table_name=dst_table_name,
                pk=tuple(pk),
            )
            if isinstance(result, data.Error):
                return result

            dst_ds.add_check_result(result)

        return None
    except Exception as e:
        return data.Error.new(
            str(e),
            src_db_config=src_db_config,
            src_db_name=src_db_config.db_name,
            src_schema_name=src_schema_name,
            src_table_name=src_table_name,
            dst_db_config=dst_db_config,
            dst_db_name=dst_db_config.db_name,
            dst_schema_name=dst_schema_name,
            dst_table_name=dst_table_name,
            pk=tuple(pk),
            after=tuple(after.items()),
            batch_ts=batch_ts,
        )


def _check(
    *,
    src_ds: data.SrcDs,
    dst_ds: data.DstDs,
    src_db_name: str,
    src_schema_name: str | None,
    src_table_name: str,
    dst_db_name: str,
    dst_schema_name: str | None,
    dst_table_name: str,
    pk: tuple[str, ...],
) -> data.CheckResult | data.Error:
    try:
        if not pk:
            return data.Error.new(
                "pk is required.",
                src_db_name=src_db_name,
                src_schema_name=src_schema_name,
                src_table_name=src_table_name,
                dst_db_name=dst_db_name,
                dst_schema_name=dst_schema_name,
                dst_table_name=dst_table_name,
                pk=pk,
            )

        start = datetime.datetime.now()

        src_row_ct = src_ds.get_row_count()
        dst_row_ct = dst_ds.get_row_count()

        src_keys = set(
            data.FrozenDict(row) for row in src_ds.fetch_rows(col_names=set(pk), after=None)
        )
        dst_keys = set(
            data.FrozenDict(row) for row in src_ds.fetch_rows(col_names=set(pk), after=None)
        )

        extra_keys = dst_keys - src_keys
        missing_keys = src_keys - dst_keys

        execution_millis = int((datetime.datetime.now() - start).total_seconds() * 1000)

        return data.CheckResult(
            src_db_name=src_db_name,
            src_schema_name=src_schema_name,
            src_table_name=src_table_name,
            dst_db_name=dst_db_name,
            dst_schema_name=dst_schema_name,
            dst_table_name=dst_table_name,
            src_rows=src_row_ct,
            dst_rows=dst_row_ct,
            extra_keys=frozenset(extra_keys),
            missing_keys=frozenset(missing_keys),
            execution_millis=execution_millis,
        )
    except Exception as e:
        raise data.Error.new(
            str(e),
            src_db_name=src_db_name,
            src_schema_name=src_schema_name,
            src_table_name=src_table_name,
            dst_db_name=dst_db_name,
            dst_schema_name=dst_schema_name,
            dst_table_name=dst_table_name,
            pk=pk,
        )
