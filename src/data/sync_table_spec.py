import dataclasses

__all__ = ("SyncTableSpec",)


@dataclasses.dataclass(frozen=True, kw_only=True)
class SyncTableSpec:
    db_name: str
    schema_name: str | None
    table_name: str
    compare_cols: set[str] | None
    increasing_cols: set[str] | None
    skip_if_row_counts_match: bool
