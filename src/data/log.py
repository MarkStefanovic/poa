import abc

__all__ = ("Log",)


class Log(abc.ABC):
    @abc.abstractmethod
    def error(self, /, error_message: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def sync_failed(self, *, sync_id: int, reason: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def sync_skipped(self, *, sync_id: int, reason: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def sync_started(self, *, src_ds_name: str, src_schema_name: str | None, src_table_name: str) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def sync_succeeded(self, *, sync_id: int, execution_millis: int) -> None:
        raise NotImplementedError
