import typing

from src import data

__all__ = ("PgLog",)


class PgLog(data.Log):
    def __init__(self, *, cursor_provider: data.CursorProvider):
        self._cursor_provider: typing.Final[data.CursorProvider] = cursor_provider

    def delete_old_logs(self, *, days_to_keep: int) -> None | data.Error:
        try:
            with self._cursor_provider.open() as cur:
                if isinstance(cur, data.Error):
                    return cur

                return cur.execute(
                    sql="CALL poa.delete_old_logs (p_days_to_keep := %s)",
                    params=(days_to_keep,),
                )
        except Exception as e:
            return data.Error.new(str(e), days_to_keep=days_to_keep)

    def error(self, /, error_message: str) -> None | data.Error:
        try:
            with self._cursor_provider.open() as cur:
                if isinstance(cur, data.Error):
                    return cur

                cur.execute(
                    sql="CALL poa.log_error (p_error_message := %s)",
                    params=(error_message,),
                )
        except Exception as e:
            return data.Error.new(str(e), error_message=error_message)

    def sync_failed(self, *, sync_id: int, reason: str) -> None | data.Error:
        try:
            with self._cursor_provider.open() as cur:
                if isinstance(cur, data.Error):
                    return cur

                cur.execute(
                    sql="CALL poa.sync_failed (p_sync_id := %s, p_error_message := %s)",
                    parms=(sync_id, reason),
                )
        except Exception as e:
            return data.Error.new(str(e), sync_id=sync_id, reason=reason)

    def sync_skipped(self, *, sync_id: int, reason: str) -> None | data.Error:
        try:
            with self._cursor_provider.open() as cur:
                if isinstance(cur, data.Error):
                    return cur

                cur.execute(
                    sql="CALL poa.sync_skipped(p_sync_id := %s, p_skip_reason := %s)",
                    params=(sync_id, reason),
                )
        except Exception as e:
            return data.Error.new(str(e), sync_id=sync_id, reason=reason)

    def sync_started(
        self,
        *,
        src_db_name: str,
        src_schema_name: str | None,
        src_table_name: str,
        incremental: bool,
    ) -> int | data.Error:
        try:
            with self._cursor_provider.open() as cur:
                if isinstance(cur, data.Error):
                    return cur

                row = cur.fetch_one(
                    """
                    SELECT * FROM poa.sync_started (
                        p_src_db_name := %s
                    ,   p_src_schema_name := %s
                    ,   p_src_table_name := %s
                    ,   p_incremental := %s
                    ) AS sync_id
                    """,
                    (
                        src_db_name,
                        src_schema_name,
                        src_table_name,
                        incremental,
                    ),
                )
                if isinstance(row, data.Error):
                    return row

                return typing.cast(int, row["sync_id"])
        except Exception as e:
            return data.Error.new(
                str(e),
                src_db_name=src_db_name,
                src_schema_name=src_schema_name,
                src_table_name=src_table_name,
                incremental=incremental,
            )

    def sync_succeeded(
        self,
        *,
        sync_id: int,
        rows_added: int,
        rows_deleted: int,
        rows_updated: int,
        execution_millis: int,
    ) -> None | data.Error:
        try:
            with self._cursor_provider.open() as cur:
                if isinstance(cur, data.Error):
                    return cur

                cur.execute(
                    sql="""
                        CALL poa.sync_succeeded(
                            p_sync_id := %s
                        ,   p_rows_added := %s
                        ,   p_rows_deleted := %s
                        ,   p_rows_updated := %s
                        ,   p_execution_millis := %s
                        )
                    """,
                    params=(
                        sync_id,
                        rows_added,
                        rows_deleted,
                        rows_updated,
                        execution_millis,
                    ),
                )
        except Exception as e:
            return data.Error.new(
                str(e),
                sync_id=sync_id,
                rows_added=rows_added,
                rows_deleted=rows_deleted,
                rows_updated=rows_updated,
                execution_millis=execution_millis,
            )
