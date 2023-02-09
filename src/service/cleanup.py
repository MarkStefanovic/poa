import pathlib
import traceback

from loguru import logger

from src import adapter

__all__ = ("cleanup",)


def cleanup(*, db_name: str, log_folder: pathlib.Path, days_logs_to_keep: int = 3) -> None:
    try:
        logger.add(log_folder / f"cleanup.{db_name}.info.log", rotation="5 MB", retention="7 days", level="INFO")
        logger.add(log_folder / f"cleanup.{db_name}.error.log", rotation="5 MB", retention="7 days", level="ERROR")

        config_file = adapter.fs.get_config_path()

        dst_api = adapter.config.get_api(config_file=config_file, name=db_name)
        dst_connection_str = adapter.config.get_connection_str(config_file=config_file, name=db_name)

        logger_cursor_provider = adapter.cursor_provider.create(api=dst_api, connection_str=dst_connection_str)

        days_logs_to_keep = adapter.config.get_days_logs_to_keep(config_file=config_file)
        log = adapter.log.create(api=dst_api, cursor_provider=logger_cursor_provider)

        try:
            log.delete_old_logs(days_to_keep=days_logs_to_keep)
        except Exception as e1:
            log.error(
                f"An error occurred while running service.cleanup({db_name=!r}, log_folder=..., {days_logs_to_keep=!r}): "
                f"{e1!s}\n{traceback.format_exc()}"
            )
            raise
    except Exception as e2:
        logger.error(
            f"An error occurred while running cleanup({db_name=!r}, log_folder=..., {days_logs_to_keep=!r}): "
            f"{e2!s}\n{traceback.format_exc()}"
        )
        raise
