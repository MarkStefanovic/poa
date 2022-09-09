import abc
import typing

__all__ = ("CursorProvider", "CursorType")

CursorType = typing.TypeVar("CursorType")


class CursorProvider(abc.ABC, typing.Generic[CursorType]):
    @abc.abstractmethod
    def open(self) -> typing.Generator[CursorType, None, None]:
        raise NotImplementedError
