# Sort-order resolution.
#
# Lifted out of `compaction.py` unchanged when compaction moved to the engine
# (docs/COMPACTION_ENGINE_EXECUTION_DESIGN.md, D-8). These two are NOT
# compaction-specific and never were: `SimpleDataset._write_table_and_build_entry`
# uses them for write-time sorting, so the catalog still needs them after the
# compactor is gone. Keeping them here is what lets `compaction.py` be deleted
# outright rather than kept alive for two functions.

from __future__ import annotations


def normalize_sort_order(sort_orders) -> dict | None:
    """Reduce a ``sort_orders`` value to the primary sort key in canonical form.

    ``sort_orders`` has been written in two incompatible shapes:

    * positional ints — ``[0]`` — an index into the schema's columns (used by
      the tests and by production ``ops.*`` datasets); and
    * Iceberg-style dicts — ``[{"order-id": 1, "fields": [{"name": "id",
      "direction": "asc"}]}]`` — name-based with a direction (written by
      ``scripts/create_dataset.py``). The old code treated ``sort_orders[0]`` as
      an int unconditionally, so the dict shape raised an uncaught ``TypeError``
      (``dict >= int``) out of ``compact()``.

    Returns ``{"name", "field_id", "index", "ascending"}`` for the primary
    (first) sort key, with the unused resolution keys set to ``None``, or
    ``None`` when nothing usable can be extracted (caller falls back to brute).
    Resolution precedence downstream is field_id → name → index.
    """
    try:
        if not sort_orders:
            return None
        entry = sort_orders[0]

        # Positional int index.
        if isinstance(entry, bool):
            return None  # bool is an int subclass; never a valid column index
        if isinstance(entry, int):
            return {"name": None, "field_id": None, "index": entry, "ascending": True}

        # Bare column name.
        if isinstance(entry, str):
            return {"name": entry, "field_id": None, "index": None, "ascending": True}

        if isinstance(entry, dict):
            # Iceberg sort-order object: unwrap to its first field. Also accept a
            # bare field dict ({"name": ..., "direction": ...}).
            field = entry
            fields = entry.get("fields")
            if isinstance(fields, (list, tuple)) and fields:
                field = fields[0]
            if not isinstance(field, dict):
                return None

            name = field.get("name")
            # Iceberg identifies the source column by "source-id" (a field id);
            # accept it as field_id when present.
            field_id = field.get("source-id")
            if field_id is None:
                field_id = field.get("field-id")
            direction = str(field.get("direction", "asc")).lower()
            ascending = direction != "desc"

            if name is None and field_id is None:
                return None
            return {
                "name": name,
                "field_id": field_id,
                "index": None,
                "ascending": ascending,
            }
    except (AttributeError, KeyError, TypeError, ValueError):
        # Reading an arbitrarily-shaped sort-order document: a missing key, a
        # non-dict where a dict was expected, a direction that will not stringify.
        # Any of those means "no usable sort order", which callers handle by
        # falling back to a brute-force merge.
        return None
    return None


def resolve_sort_column(sort_order: dict, columns):
    """Resolve a canonical sort key (from ``normalize_sort_order``) against
    schema ``columns``.

    Precedence: field_id → name → positional index. ``columns`` entries may
    be objects with ``.name``/``.id`` or dicts with ``"name"``/``"id"``.
    Returns ``(column_name, field_id, index)`` where ``index`` is the
    column's schema position (used to read positional min/max stats when a
    manifest entry carries no field_ids). ``column_name`` is None when the
    key cannot be resolved (caller falls back to brute/unsorted).

    Shared by write-time sorting (``SimpleDataset._write_table_and_build_entry``)
    and the engine's compaction planner, so the two stay consistent about how a
    stored sort order maps to a column.
    """

    def col_name(c):
        return getattr(c, "name", None) or (c.get("name") if isinstance(c, dict) else None)

    def col_id(c):
        cid = getattr(c, "id", None)
        if cid is None and isinstance(c, dict):
            cid = c.get("id")
        return cid

    target_fid = sort_order.get("field_id")
    target_name = sort_order.get("name")
    target_index = sort_order.get("index")

    sort_index = None
    if target_fid is not None:
        sort_index = next((i for i, c in enumerate(columns) if col_id(c) == target_fid), None)
    if sort_index is None and target_name is not None:
        sort_index = next((i for i, c in enumerate(columns) if col_name(c) == target_name), None)
    if sort_index is None and target_index is not None and 0 <= target_index < len(columns):
        sort_index = target_index

    if sort_index is None:
        return None, None, None
    sort_col = columns[sort_index]
    return col_name(sort_col), col_id(sort_col), sort_index
