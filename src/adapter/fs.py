import functools
import os
import pathlib
import sys
from src import data

__all__ = (
    "get_config_path",
    "get_log_folder",
)


@functools.lru_cache
def _root_dir() -> pathlib.Path | data.Error:
    if getattr(sys, "frozen", False):
        path = pathlib.Path(os.path.dirname(sys.executable))

        if not path.exists():
            return data.Error.new(
                "os.path.dirname(sys.executable) returned an invalid path for a frozen executable."
            )

        return path
    else:
        try:
            return next(p for p in pathlib.Path(__file__).parents if (p / "src").exists())
        except StopIteration:
            return data.Error.new(f"src not found in path, {__file__}.")


@functools.lru_cache
def get_config_path() -> pathlib.Path | data.Error:
    try:
        root = _root_dir()
        if isinstance(root, data.Error):
            return root

        return root / "assets" / "config.json"
    except Exception as e:
        return data.Error.new(str(e))


@functools.lru_cache
def get_log_folder() -> pathlib.Path | data.Error:
    try:
        root = _root_dir()
        if isinstance(root, data.Error):
            return root

        folder = root / "logs"
        folder.mkdir(exist_ok=True)
        return folder
    except Exception as e:
        return data.Error.new(str(e))
