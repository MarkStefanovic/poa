import dataclasses

__all__ = ("SyncTableSpec",)


@dataclasses.dataclass(frozen=True, kw_only=True)
class SyncTableSpec:
    schema_name: str | None
    table_name: str
    compare_cols: set[str]
    skip_if_row_counts_match: bool
    increasing_cols: set[str]
