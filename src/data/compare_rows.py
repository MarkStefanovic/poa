import typing

from src.data.row_diff import RowDiff

__all__ = ("get_row_diff",)

Key: typing.TypeAlias = tuple[typing.Hashable, ...]
Row: typing.TypeAlias = dict[str, typing.Hashable]


def get_row_diff(src: dict[Key, Row], dst: dict[Key, Row], /) -> RowDiff:
    added: list[Row] = []
    changed: list[tuple[Row, Row]] = []
    for key, src_row in src.items():
        dst_row = dst.get(key)
        if dst_row is None:
            added.append(src_row)
        else:
            if any(1 for col_name, value in src_row.items() if value != dst_row[col_name]):
                changed.append((src_row, dst_row))

    deleted = [dst_row for dst_key, dst_row in dst.items() if dst_key not in src]

    return RowDiff(added=added, changed=changed, deleted=deleted)


if __name__ == '__main__':
    s = {
        ("a", 1): {"first_name": "Steve", "last_name": "Smith", "age": None},
        ("b", 2): {"first_name": "Amy", "last_name": "Apples", "age": 38}
    }
    d = {
        ("a", 1): {"first_name": "Steve", "last_name": "Smith", "age": 28},
        ("c", None): {"first_name": "null", "last_name": "void", "age": None},
    }
    rd = get_row_diff(s, d)
    print(rd)
