from __future__ import annotations

import dataclasses
import inspect
import pathlib
import typing

__all__ = ("Error",)


@dataclasses.dataclass(frozen=True, kw_only=True)
class Error(Exception):
    file: str
    fn: str
    fn_args: dict[str, typing.Hashable]
    error_message: str

    @staticmethod
    def new(error_message: str, **kwargs: typing.Hashable) -> Error:
        try:
            frame = inspect.stack()[1].frame
            fn_name = frame.f_code.co_name
            filename = pathlib.Path(frame.f_code.co_filename).with_suffix("").name
        except:  # noqa
            return Error(
                file="",
                fn="",
                fn_args={},
                error_message="An error occurred while inspecting the first frame to create a new Context.",
            )

        if kwargs:
            fn_args: dict[str, typing.Hashable] = kwargs
        else:
            fn_args = {}

        return Error(
            file=filename,
            fn=fn_name,
            fn_args=fn_args,
            error_message=error_message,
        )

    def __str__(self) -> str:
        file_name = pathlib.Path(self.file).with_suffix("").name

        ctx = "\n     " + "\n    ".join(
            f"{arg_name}: {arg_value!s}" for arg_name, arg_value in sorted(self.fn_args.items())
        )

        return (
            f"\nError [\n  msg: {self.error_message}\n  src: {file_name}.{self.fn}\n  ctx:{ctx}\n]"
        )


def example(x: int) -> float | Error:
    try:
        return 5 / x
    except Exception as e:
        return Error.new(str(e), x=1)


if __name__ == "__main__":
    r = example(0)
    print(r)

    r2 = example(3)
    print(r2)
