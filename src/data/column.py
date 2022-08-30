import dataclasses

from src.data.data_type import DataType

__all__ = ("Column",)


@dataclasses.dataclass(frozen=True)
class Column:
    name: str
    data_type: DataType
    nullable: bool
    length: int | None
    precision: int | None
    scale: int | None
