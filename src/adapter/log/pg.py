from __future__ import annotations

import traceback

from loguru import logger

from src import data

__all__ = ("PgLog",)


class PgLog(data.Log):
    def __init__(self, *, cursor_provider: data.CursorProvider):
        self._cursor_provider = cursor_provider

    def delete_old_logs(self, *, days_to_keep: int) -> None:
        try:
            with self._cursor_provider.open() as cur:
                cur.execute(
                    "CALL poa.delete_old_logs (p_days_to_keep := %(days_to_keep)s);",
                    {"days_to_keep": days_to_keep},
                )
        except Exception as e:
            self.error(
                f"An error occurred while running delete_old_logs({days_to_keep=!r}): "
                f"{e!s}\n{traceback.format_exc()}"
            )
            raise

    def error(self, /, error_message: str) -> None:
        try:
            with self._cursor_provider.open() as cur:
                cur.execute(
                    "CALL poa.log_error (p_error_message := %(error_message)s)",
                    {"error_message": error_message},
                )
        except Exception as e:
            logger.exception(e)
            raise

    def sync_failed(self, *, sync_id: int, reason: str) -> None:
        try:
            with self._cursor_provider.open() as cur:
                cur.execute(
                    "CALL poa.sync_failed (p_sync_id := %(sync_id)s, p_error_message := %(reason)s)",
                    {"sync_id": sync_id, "reason": reason},
                )
        except Exception as e:
            self.error(
                f"An error occurred while running sync_failed({sync_id=!r}, {reason=!r}): "
                f"{e!s}\n{traceback.format_exc()}"
            )
            raise

    def sync_skipped(self, *, sync_id: int, reason: str) -> None:
        try:
            with self._cursor_provider.open() as cur:
                cur.execute(
                    "CALL poa.sync_skipped(p_sync_id := %(sync_id)s, p_skip_reason := %(reason)s);",
                    {"sync_id": sync_id, "reason": reason},
                )
        except Exception as e:
            self.error(
                f"An error occurred while running sync_skipped({sync_id=!r}, {reason=!r}): "
                f"{e!s}\n{traceback.format_exc()}"
            )
            raise

    def sync_started(
        self,
        *,
        src_db_name: str,
        src_schema_name: str | None,
        src_table_name: str,
        incremental: bool,
    ) -> int:
        try:
            with self._cursor_provider.open() as cur:
                cur.execute(
                    """
                    SELECT * FROM poa.sync_started (
                        p_src_db_name := %(src_db_name)s
                    ,   p_src_schema_name := %(src_schema_name)s
                    ,   p_src_table_name := %(src_table_name)s
                    ,   p_incremental := %(incremental)s
                    );
                    """,
                    {
                        "src_db_name": src_db_name,
                        "src_schema_name": src_schema_name,
                        "src_table_name": src_table_name,
                        "incremental": incremental,
                    },
                )
                return cur.fetchone()["sync_started"]
        except Exception as e:
            self.error(
                f"An error occurred while running sync_started({src_db_name=!r}, {src_schema_name=!r}, "
                f"{src_table_name=!r}, {incremental=!r}): {e!s}\n{traceback.format_exc()}"
            )
            raise

    def sync_succeeded(
        self,
        *,
        sync_id: int,
        rows_added: int,
        rows_deleted: int,
        rows_updated: int,
        execution_millis: int,
    ) -> None:
        try:
            with self._cursor_provider.open() as cur:
                cur.execute(
                    """
                    CALL poa.sync_succeeded(
                        p_sync_id := %(sync_id)s
                    ,   p_rows_added := %(rows_added)s
                    ,   p_rows_deleted := %(rows_deleted)s
                    ,   p_rows_updated := %(rows_updated)s
                    ,   p_execution_millis := %(execution_millis)s
                    );
                    """,
                    {
                        "sync_id": sync_id,
                        "rows_added": rows_added,
                        "rows_deleted": rows_deleted,
                        "rows_updated": rows_updated,
                        "execution_millis": execution_millis,
                    },
                )
        except Exception as e:
            self.error(
                f"An error occurred while running sync_succeeded({sync_id=!r}, {execution_millis=!r}): "
                f"{e!s}\n{traceback.format_exc()}"
            )
            raise
