import enum

__all__ = ("DataType",)


# noinspection PyArgumentList
class DataType(enum.Enum):
    BigFloat = enum.auto()
    BigInt = enum.auto()
    Bool = enum.auto()
    Date = enum.auto()
    Decimal = enum.auto()
    Float = enum.auto()
    Int = enum.auto()
    Text = enum.auto()
    Timestamp = enum.auto()
    TimestampTZ = enum.auto()
    UUID = enum.auto()
