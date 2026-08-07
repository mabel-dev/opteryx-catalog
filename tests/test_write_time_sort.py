"""Write-time sorting: `_sort_for_write` should physically sort a freshly
written Morsel by the dataset's configured sort order, since compaction only
clusters files once they're large enough for sort-aware merges -- a small
append/overwrite would otherwise never be internally sorted."""

from opteryx_catalog.catalog.dataset import RelationSchema
from opteryx_catalog.catalog.dataset import SchemaColumn
from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.metadata import DatasetMetadata


def _make_unsorted_table():
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel

    timestamps = [50, 10, 40, 20, 30]
    values = [f"value_{t}" for t in timestamps]

    m = Morsel()
    m.append_vector("timestamp", vector_from_sequence(timestamps, dtype="INTEGER"))
    m.append_vector("value", vector_from_sequence(values, dtype="VARCHAR"))
    return m


def _make_dataset(sort_orders):
    meta = DatasetMetadata(
        dataset_identifier="tests_temp.sort_on_write",
        location="mem://ws/tests_temp/sort_on_write",
        schema=RelationSchema(
            name="tests_temp.sort_on_write",
            columns=[
                SchemaColumn(name="timestamp", type="INTEGER", id=1),
                SchemaColumn(name="value", type="VARCHAR", id=2),
            ],
        ),
        properties={},
    )
    meta.sort_orders = sort_orders
    return SimpleDataset(identifier="tests_temp.sort_on_write", _metadata=meta)


def test_sort_for_write_sorts_by_configured_column():
    ds = _make_dataset([{"order-id": 1, "fields": [{"name": "timestamp", "direction": "asc"}]}])
    table = _make_unsorted_table()

    sorted_table, sort_column, sort_descending = ds._sort_for_write(table)

    assert sort_column == "timestamp"
    assert sort_descending is False
    assert sorted_table.column(b"timestamp").to_pylist() == [10, 20, 30, 40, 50]


def test_sort_for_write_descending():
    ds = _make_dataset([{"order-id": 1, "fields": [{"name": "timestamp", "direction": "desc"}]}])
    table = _make_unsorted_table()

    sorted_table, sort_column, sort_descending = ds._sort_for_write(table)

    assert sort_column == "timestamp"
    assert sort_descending is True
    assert sorted_table.column(b"timestamp").to_pylist() == [50, 40, 30, 20, 10]


def test_sort_for_write_positional_sort_order():
    ds = _make_dataset([0])  # positional index into columns -> "timestamp"
    table = _make_unsorted_table()

    sorted_table, sort_column, _ = ds._sort_for_write(table)

    assert sort_column == "timestamp"
    assert sorted_table.column(b"timestamp").to_pylist() == [10, 20, 30, 40, 50]


def test_sort_for_write_no_sort_order_configured_is_noop():
    ds = _make_dataset([])
    table = _make_unsorted_table()

    result_table, sort_column, sort_descending = ds._sort_for_write(table)

    assert sort_column is None
    assert sort_descending is False
    assert result_table is table


def test_sort_for_write_unresolvable_column_is_noop():
    ds = _make_dataset(
        [{"order-id": 1, "fields": [{"name": "does_not_exist", "direction": "asc"}]}]
    )
    table = _make_unsorted_table()

    result_table, sort_column, _ = ds._sort_for_write(table)

    assert sort_column is None
    assert result_table is table
