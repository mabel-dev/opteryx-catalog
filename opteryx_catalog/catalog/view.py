from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class View:
    name: str
    definition: str
    properties: dict | None = None
    metadata: Any | None = None

    def schema(self, schema_id: str | None = None):
        """The columns this view produces, or None if they were never recorded.

        The same call, with the same duck-typed return, as
        :meth:`SimpleDataset.schema` - a caller asks a relation for its schema
        without first having to know whether it is a dataset or a view. Without
        it, a `hasattr(ds, "schema")` probe answered False for EVERY view, so
        every view was described as a single placeholder column named `id`.

        A view's columns belong to the STATEMENT that produces them, so this is
        the shape of the view's CURRENT statement - `load_view` read it from the
        same document as that statement's SQL. `schema_id` is accepted to match
        the dataset signature and ignored: a view has no schema documents to
        choose between, only statement versions.

        None means the current statement was registered before columns were
        recorded. It does NOT mean the view produces no columns, which cannot
        happen.
        """
        return getattr(self.metadata, "schema", None)
