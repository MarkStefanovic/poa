import functools
import os
import pathlib
import sys

__all__ = ("config_path", "log_folder")


@functools.lru_cache(maxsize=1)
def _root_dir() -> pathlib.Path:
    if getattr(sys, "frozen", False):
        path = pathlib.Path(os.path.dirname(sys.executable))
        assert path is not None
        return path
    else:
        try:
            return next(p for p in pathlib.Path(__file__).parents if p.name == "poa")
        except StopIteration:
            raise Exception(f"poa not found in path, {__file__}.")


@functools.lru_cache(maxsize=1)
def get_config_path() -> pathlib.Path:
    return _root_dir() / "assets" / "config.json"


@functools.lru_cache(maxsize=1)
def get_log_folder() -> pathlib.Path:
    folder = _root_dir() / "logs"
    folder.mkdir(exist_ok=True)
    return folder
