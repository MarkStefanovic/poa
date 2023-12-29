import datetime
import typing

__all__ = ("combine_filters",)

T = typing.TypeVar("T")


def combine_filters(
    *,
    ds_filter: dict[str, datetime.date] | None,
    query_filter: dict[str, typing.Any] | None,
) -> dict[str, typing.Any]:
    if ds_filter is None:
        return _sort_dict_by_key(query_filter)

    if query_filter is None:
        return _sort_dict_by_key(ds_filter)

    result: dict[str, typing.Any] = ds_filter.copy()
    for field, after in query_filter.items():
        ds_after = ds_filter.get(field)

        if ds_after is None:
            if after is not None:
                result[field] = after
        else:
            query_after_dt = _date_to_datetime(after)
            ds_after_dt = _date_to_datetime(ds_after)

            if (
                query_after_dt is not None
                and ds_after_dt is not None
                and query_after_dt > ds_after_dt
            ):
                result[field] = query_after_dt
            else:
                result[field] = ds_after_dt

    return _sort_dict_by_key(result)


def _sort_dict_by_key(d: dict[str, T] | None) -> dict[str, T]:
    if d:
        return dict(sorted(d.items()))  # noqa
    return {}


def _date_to_datetime(d: datetime.date | datetime.datetime | None, /) -> datetime.datetime | None:
    if d is None:
        return d
    elif isinstance(d, datetime.datetime):
        return d
    elif isinstance(d, datetime.date):
        return datetime.datetime(d.year, d.month, d.day)
    else:
        raise TypeError(f"Expected a date or datetime, but got {d!r}.")
