import abc
import contextlib
import typing

__all__ = ("CursorProvider",)


class CursorProvider(abc.ABC):
    @contextlib.contextmanager
    @abc.abstractmethod
    def open(self) -> typing.Generator[typing.Any, None, None]:
        raise NotImplementedError
