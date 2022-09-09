from loguru import logger

from src import data

__all__ = ("PgLog",)


class PgLog(data.Log):
    def __init__(self, *, cursor_provider: data.CursorProvider):
        self._cursor_provider = cursor_provider

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
            logger.exception(e)
            raise

    def sync_skipped(self, *, sync_id: int, reason: str) -> None:
        pass

    def sync_started(self, *, src_ds_name: str, src_schema_name: str | None, src_table_name: str) -> int:
        pass

    def sync_succeeded(self, *, sync_id: int, execution_millis: int) -> None:
        pass
