from src import data

__all__ = ("cleanup",)


def cleanup(*, cursor_provider: data.CursorProvider, days_logs_to_keep: int) -> None:
    pass
