import pydantic

from src.data.data_type import DataType

__all__ = ("Column",)


@pydantic.dataclasses.dataclass(frozen=True, kw_only=True, config=pydantic.ConfigDict(strict=True))
class Column:
    name: str
    data_type: DataType
    nullable: bool
    length: int | None
    precision: int | None
    scale: int | None
