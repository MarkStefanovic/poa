from __future__ import annotations

import functools
import json
import pathlib
import typing

import loguru

from src import data

__all__ = (
    "get_api",
    "get_connection_str",
    "get_days_logs_to_keep",
    "get_seconds_between_cleanups",
)


@functools.lru_cache(maxsize=100)
def get_api(*, config_file: pathlib.Path, name: str) -> data.API:
    api_str = str(_load(config_file=config_file)["ds"][name]["api"])
    if api_str == "hh":
        return data.API.HH
    elif api_str == "pyodbc":
        return data.API.PYODBC
    elif api_str == "psycopg2":
        return data.API.PSYCOPG2
    else:
        raise data.error.UnrecognizedDatabaseAPI(api=api_str)


@functools.lru_cache(maxsize=100)
def get_connection_str(*, config_file: pathlib.Path, name: str) -> str:
    return str(_load(config_file=config_file)["ds"][name]["connection-string"])


def get_days_logs_to_keep(*, config_file: pathlib.Path) -> int:
    return typing.cast(int, _load(config_file=config_file)["days-logs-to-keep"])


@functools.lru_cache(maxsize=1)
def get_seconds_between_cleanups(*, config_file: pathlib.Path) -> int:
    return typing.cast(int, _load(config_file=config_file)["get-seconds-between-cleanups"])


@functools.lru_cache(maxsize=1)
def _load(*, config_file: pathlib.Path) -> dict[str, typing.Any]:
    loguru.logger.info(f"Loading config file at {config_file.resolve()!s}...")

    assert config_file.exists(), f"The config file specified, {config_file.resolve()!s}, does not exist."

    with config_file.open("r") as fh:
        return json.load(fh)
