import abc
import contextlib
import typing

from src.data.cursor import Cursor

__all__ = ("CursorProvider",)


class CursorProvider(abc.ABC, typing.Generic[Cursor]):
    @contextlib.contextmanager
    @abc.abstractmethod
    def open(self) -> typing.Generator[Cursor, None, None]:
        raise NotImplementedError
