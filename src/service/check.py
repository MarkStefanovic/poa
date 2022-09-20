from __future__ import annotations

import datetime

from src import data

__all__ = ("check",)


def check(
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
) -> data.CheckResult:
    try:
        assert pk, "pk is required."

        start = datetime.datetime.now()

        src_row_ct = src_ds.get_row_count()
        dst_row_ct = dst_ds.get_row_count()

        src_keys = set(data.FrozenDict(row) for row in src_ds.fetch_rows(col_names=set(pk), after=None))
        dst_keys = set(data.FrozenDict(row) for row in src_ds.fetch_rows(col_names=set(pk), after=None))

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
        raise data.error.CheckError(
            f"An error occurred while running check(src_ds=..., dst_ds=..., {src_db_name=!r}, "
            f"{src_schema_name=!r}, {src_table_name=!r}, {dst_db_name=!r}, {dst_schema_name=!r}, "
            f"{dst_table_name=!r}): {e!s}\n{e.__traceback__}"
        )
