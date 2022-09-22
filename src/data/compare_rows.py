import typing

from src.data.frozen_dict import FrozenDict
from src.data.row import Row
from src.data.row_diff import RowDiff
from src.data.row_key import RowKey

__all__ = ("compare_rows",)


def compare_rows(
    *,
    src_rows: typing.Iterable[Row],
    dst_rows: typing.Iterable[Row],
    key_cols: typing.Iterable[str],
) -> RowDiff:
    indexed_src_rows = _index_rows(key_cols=key_cols, rows=src_rows)
    indexed_dst_rows = _index_rows(key_cols=key_cols, rows=dst_rows)
    return _compare_rows(indexed_src_rows=indexed_src_rows, indexed_dst_rows=indexed_dst_rows)


def _index_rows(*, key_cols: typing.Iterable[str], rows: typing.Iterable[Row]) -> dict[RowKey, Row]:
    return {
        FrozenDict({col: row[col] for col in key_cols}): row
        for row in rows
    }


def _compare_rows(*, indexed_src_rows: dict[RowKey, Row], indexed_dst_rows: dict[RowKey, Row]) -> RowDiff:
    added: dict[RowKey, Row] = {}
    updated: dict[RowKey, tuple[Row, Row]] = {}
    for key, src_row in indexed_src_rows.items():
        dst_row = indexed_dst_rows.get(key)
        if dst_row is None:
            added[key] = src_row
        else:
            if any(1 for col_name, value in src_row.items() if value != dst_row[col_name]):
                updated[key] = (src_row, dst_row)

    deleted = {
        dst_key: dst_row
        for dst_key, dst_row in indexed_dst_rows.items()
        if dst_key not in indexed_src_rows.keys()
    }

    return RowDiff(added=added, updated=updated, deleted=deleted)


if __name__ == '__main__':
    s = [
        {"id": 1, "first_name": "Steve", "last_name": "Smith", "age": None},
        {"id": 2, "first_name": "Amy", "last_name": "Apples", "age": 38}
    ]
    d = [
        {"id": 1, "first_name": "Steve", "last_name": "Smith", "age": 28},
        {"id": 3, "first_name": "null", "last_name": "void", "age": None},
    ]
    rd = compare_rows(src_rows=s, dst_rows=d, key_cols=("id",))
    print(rd)

    # ix = _index_rows(
    #     key_cols=["first_name", "last_name"],
    #     rows=[
    #         {"id": 1, "first_name": "Steve", "last_name": "Smith", "age": 28},
    #         {"id": 3, "first_name": "null", "last_name": "void", "age": None},
    #     ],
    # )
    # print(ix)
