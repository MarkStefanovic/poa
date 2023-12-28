import dataclasses

import pydantic

from src.data.frozen_dict import FrozenDict

__all__ = ("CheckResult",)


@pydantic.dataclasses.dataclass(frozen=True, kw_only=True, config=pydantic.ConfigDict(strict=True))
class CheckResult:
    src_db_name: str
    src_schema_name: str | None
    src_table_name: str
    dst_db_name: str
    dst_schema_name: str | None
    dst_table_name: str
    src_rows: int | None
    dst_rows: int | None
    missing_keys: frozenset[FrozenDict] | None
    extra_keys: frozenset[FrozenDict] | None
    execution_millis: int
