import pydantic

from src import data

__all__ = ("DbConfig",)


@pydantic.dataclasses.dataclass(frozen=True, kw_only=True, config=pydantic.ConfigDict(strict=True))
class DbConfig:
    db_id: str
    api: data.API
    host: str | None
    db_name: str | None
    keyring_db_username_entry: str | None
    keyring_db_password_entry: str | None
    connection_string: pydantic.SecretStr | None

    def __repr__(self) -> str:
        return f"DbConfig(db_id={self.db_id!r}, api={self.api!r})"
