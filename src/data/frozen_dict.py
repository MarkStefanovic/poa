import typing

__all__ = ("FrozenDict",)


class FrozenDict(dict[str, typing.Hashable]):
    def __hash__(self) -> int:  # type: ignore
        return hash(frozenset(self.items()))
