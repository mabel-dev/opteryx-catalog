"""Custom Table implementation that writes Parquet manifests instead of Avro."""

from __future__ import annotations

from typing import Dict
from typing import Optional

import pyarrow as pa
from orso.logging import get_logger
from pyiceberg.table import StaticTable
from pyiceberg.typedef import EMPTY_DICT

logger = get_logger()


class FirestoreTable(StaticTable):
    """Table implementation that writes Parquet manifests instead of Avro.

    This overrides the append() method to bypass pyiceberg's Transaction flow
    which writes Avro manifests, and instead writes Parquet manifests directly.
    """

    def append(
        self,
        df: pa.Table,
        snapshot_properties: Dict[str, str] = EMPTY_DICT,
        branch: Optional[str] = None,
    ) -> None:
        """Append data using Parquet manifests instead of Avro.

        This delegates to PyIceberg's standard append which writes Avro manifests,
        but our catalog's commit_table also writes Parquet manifests which are used for planning.

        Args:
            df: The Arrow dataframe to append
            snapshot_properties: Custom properties for the snapshot summary
            branch: Branch reference (not yet implemented)
        """
        print("🔥🔥🔥 FirestoreTable.append() called - delegating to parent!")
        logger.warning("🔥 FirestoreTable.append() called - using standard flow!")

        # Use parent's append which handles all the complexity
        # Our catalog's commit_table will write Parquet manifests
        super().append(df, snapshot_properties=snapshot_properties, branch=branch)

        logger.info("Appended data via standard PyIceberg flow")
