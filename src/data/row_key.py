import typing

from frozendict import frozendict  # type: ignore

__all__ = ("RowKey",)


RowKey: typing.TypeAlias = frozendict
