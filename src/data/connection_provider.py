import abc
import typing

__all__ = ("ConnectionProvider", "ConnectionType")

ConnectionType = typing.TypeVar("ConnectionType")


class ConnectionProvider(abc.ABC, typing.Generic[ConnectionType]):
    @abc.abstractmethod
    def connect(self) -> typing.Generator[ConnectionType, None, None]:
        raise NotImplementedError
