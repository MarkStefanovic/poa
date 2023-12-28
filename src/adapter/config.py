import functools
import json
import pathlib
import typing

from src import data

__all__ = ("load",)


@functools.lru_cache(maxsize=1)
def load(*, config_file: pathlib.Path) -> data.Config | data.Error:
    # noinspection PyBroadException
    try:
        if not config_file.exists():
            return data.Error.new(
                f"The config file specified, {config_file.resolve()!s}, does not exist.",
                config_file=config_file,
            )

        with config_file.open("r") as fh:
            d = typing.cast(dict[str, typing.Any], json.load(fh))

        if "seconds-between-cleanups" not in d.keys():
            return data.Error.new("config file is missing an entry for 'seconds-between-cleanups'.")

        seconds_between_cleanups: typing.Final[int] = int(d["seconds-between-cleanups"])

        if "days-logs-to-keep" not in d.keys():
            return data.Error.new("config file is missing an entry for 'days-logs-to-keep'.")

        days_logs_to_keep: typing.Final[int] = int(d["days-logs-to-keep"])

        if "batch-size" not in d.keys():
            return data.Error.new("config file is missing an entry for 'batch-size'.")

        batch_size: typing.Final[int] = int(d["batch-size"])

        if "databases" not in d.keys():
            return data.Error.new("config file is missing an entry for 'databases'.")

        databases: list[data.DbConfig] = []
        for datasource_dict in d["databases"]:
            database = _parse_datasource_dict(datasource_dict)
            if isinstance(database, data.Error):
                return database

            databases.append(database)

        return data.Config(
            seconds_between_cleanups=seconds_between_cleanups,
            days_logs_to_keep=days_logs_to_keep,
            batch_size=batch_size,
            databases=tuple(databases),
        )
    except:  # noqa: E722
        # import traceback

        # print(traceback.format_exc())

        return data.Error.new(
            "An error occurred while loading the config file.",
            config_file=config_file,
        )


def _parse_datasource_dict(datasource_dict: dict[str, typing.Any], /) -> data.DbConfig | data.Error:
    # noinspection PyBroadException
    try:
        if "name" not in datasource_dict.keys():
            return data.Error.new("datasource entry in config file is missing an entry for 'name'.")

        name: typing.Final[str] = datasource_dict["name"]

        if "api" not in datasource_dict.keys():
            return data.Error.new("datasource entry in config file is missing an entry for 'api'.")

        # noinspection PyBroadException
        try:
            api: typing.Final[data.API] = data.API(datasource_dict["api"])
        except:  # noqa: E722
            return data.Error.new(
                f"could not convert api entry, {datasource_dict['api']!r}, to a data.API instance."
            )

        if "host" not in datasource_dict.keys():
            return data.Error.new("datasource entry in config file is missing an entry for 'host'.")

        host: typing.Final[str | None] = datasource_dict["host"]

        if "db-name" not in datasource_dict.keys():
            return data.Error.new(
                "datasource entry in config file is missing an entry for 'db-name'."
            )

        db_name: typing.Final[str | None] = datasource_dict["db-name"]

        if "keyring-db-username-entry" not in datasource_dict.keys():
            return data.Error.new(
                "datasource entry in config file is missing an entry for 'keyring-db-username-entry'."
            )

        keyring_db_username_entry: typing.Final[str | None] = datasource_dict[
            "keyring-db-username-entry"
        ]

        if "keyring-db-password-entry" not in datasource_dict.keys():
            return data.Error.new(
                "datasource entry in config file is missing an entry for 'keyring-db-password-entry'."
            )

        keyring_db_password_entry: typing.Final[str | None] = datasource_dict[
            "keyring-db-password-entry"
        ]

        if "connection-string" not in datasource_dict.keys():
            return data.Error.new(
                "datasource entry in config file is missing an entry for 'connection-string'."
            )

        connection_string: typing.Final[str | None] = datasource_dict["connection-string"]

        if connection_string is None:
            if (
                host is None
                or db_name is None
                or keyring_db_username_entry is None
                or keyring_db_password_entry is None
            ):
                return data.Error.new(
                    "If connection-string is null, then host, db_name, keyring-db-username-entry, and "
                    "keyring-db-password-entry must be provided."
                )

        return data.DbConfig(
            ds_name=name,
            api=api,
            host=host,
            db_name=db_name,
            keyring_db_username_entry=keyring_db_username_entry,
            keyring_db_password_entry=keyring_db_password_entry,
            connection_string=connection_string,
        )
    except:  # noqa: E722
        return data.Error.new("An error occurred while parsing datasource from json.")


if __name__ == "__main__":
    print(load(config_file=pathlib.Path(r"C:\bu\py\poa\assets\config.json")))
