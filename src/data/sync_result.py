from __future__ import annotations

import typing

import pydantic

__all__ = ("SyncResult",)


@pydantic.dataclasses.dataclass(frozen=True, kw_only=True, config=pydantic.ConfigDict(strict=True))
class SyncResult:
    error_message: str | None
    execution_millis: int | None
    rows_added: int
    rows_deleted: int
    rows_updated: int
    skip_reason: str | None
    status: typing.Literal["failed", "skipped", "succeeded"]

    @staticmethod
    def failed(*, error_message: str) -> SyncResult:
        return SyncResult(
            error_message=error_message,
            execution_millis=None,
            rows_added=0,
            rows_deleted=0,
            rows_updated=0,
            skip_reason=None,
            status="failed",
        )

    @staticmethod
    def skipped(*, reason: str) -> SyncResult:
        return SyncResult(
            error_message=None,
            execution_millis=None,
            rows_added=0,
            rows_deleted=0,
            rows_updated=0,
            skip_reason=reason,
            status="skipped",
        )

    @staticmethod
    def succeeded(
        *,
        rows_added: int,
        rows_deleted: int,
        rows_updated: int,
        execution_millis: int,
    ) -> SyncResult:
        return SyncResult(
            error_message=None,
            execution_millis=execution_millis,
            rows_added=rows_added,
            rows_deleted=rows_deleted,
            rows_updated=rows_updated,
            skip_reason=None,
            status="succeeded",
        )
