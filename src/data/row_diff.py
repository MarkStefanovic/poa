import dataclasses

import typing

__all__ = ("RowDiff",)


@dataclasses.dataclass(frozen=True)
class RowDiff:
    added: list[dict[str, typing.Hashable]]
    changed: list[tuple[dict[str, typing.Hashable], dict[str, typing.Hashable]]]
    deleted: list[dict[str, typing.Hashable]]
