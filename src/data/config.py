import pydantic

from src.data.db_config import DbConfig

__all__ = ("Config",)


@pydantic.dataclasses.dataclass(frozen=True, kw_only=True)
class Config:
    seconds_between_cleanups: pydantic.PositiveInt
    days_logs_to_keep: pydantic.PositiveInt
    batch_size: pydantic.PositiveInt
    databases: tuple[DbConfig, ...]

    def db(self, /, db_id: str) -> DbConfig | None:
        return next((db for db in self.databases if db.db_id == db_id), None)

    def __repr__(self) -> str:
        return (
            f"Config(seconds_between_cleanups={self.seconds_between_cleanups}, "
            f"days_logs_to_keep={self.days_logs_to_keep}, batch_size={self.batch_size}, "
            f"datasources={self.databases})"
        )
