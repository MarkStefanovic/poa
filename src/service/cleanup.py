from loguru import logger

from src import adapter, data

__all__ = ("cleanup",)


def cleanup(
    *,
    dst_config: data.DbConfig,
    days_logs_to_keep: int = 3,
) -> None | data.Error:
    try:
        log = adapter.log.create(db_config=dst_config)
        if isinstance(log, data.Error):
            return log

        return log.delete_old_logs(days_to_keep=days_logs_to_keep)
    except Exception as e:
        logger.error(f"An error occurred while running cleanup: {e!s}")

        return data.Error.new(f"An error occurred while running service.cleanup: {e!s}")
