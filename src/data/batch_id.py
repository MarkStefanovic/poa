import uuid

import pydantic

__all__ = ("BatchId",)


@pydantic.dataclasses.dataclass(frozen=True)
class BatchId:
    value: str = pydantic.Field(
        default_factory=lambda: uuid.uuid4().hex,
        min_length=32,
        max_length=32,
        strict=True,
    )
