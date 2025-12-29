"""Custom Table implementation that writes Parquet manifests instead of Avro."""

from __future__ import annotations

import uuid
from typing import Dict
from typing import Optional

import pyarrow as pa
from orso.logging import get_logger
from pyiceberg.io.pyarrow import _check_pyarrow_schema_compatible
from pyiceberg.manifest import DataFile
from pyiceberg.manifest import ManifestEntry
from pyiceberg.manifest import ManifestEntryStatus
from pyiceberg.table import DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE
from pyiceberg.table import Config
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

        Args:
            df: The Arrow dataframe to append
            snapshot_properties: Custom properties for the snapshot summary
            branch: Branch reference (not yet implemented)
        """
        print("🔥🔥🔥 FirestoreTable.append() called!")
        logger.warning("🔥 FirestoreTable.append() called!")

        if not isinstance(df, pa.Table):
            raise ValueError(f"Expected PyArrow table, got: {df}")

        if df.shape[0] == 0:
            logger.debug("Empty dataframe, skipping append")
            return

        # Validate schema compatibility
        downcast_ns_timestamp_to_us = (
            Config().get_bool(DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE) or False
        )
        _check_pyarrow_schema_compatible(
            self.metadata.schema(),
            provided_schema=df.schema,
            downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
            format_version=self.metadata.format_version,
        )

        # Generate commit UUID and write data file ourselves
        import pyarrow.parquet as pq

        commit_uuid = uuid.uuid4()
        data_file_name = f"00000-0-{commit_uuid}.parquet"
        data_file_path = f"{self.metadata.location}/data/{data_file_name}"

        # Write Parquet data file directly
        output_file = self.io.new_output(data_file_path)
        with output_file.create() as f:
            pq.write_table(df, f, compression="snappy")

        # Get file stats for manifest entry
        file_size = len(output_file) if hasattr(output_file, "__len__") else 0
        if file_size == 0:
            # Read it back to get size
            input_file = self.io.new_input(data_file_path)
            file_size = len(input_file)

        # Create DataFile entry
        data_file = DataFile(
            content=0,  # DATA
            file_path=data_file_path,
            file_format="PARQUET",
            partition={},
            record_count=df.shape[0],
            file_size_in_bytes=file_size,
            column_sizes={},
            value_counts={},
            null_value_counts={},
            nan_value_counts={},
            lower_bounds={},
            upper_bounds={},
            key_metadata=None,
            split_offsets=[4],  # Standard Parquet split
            equality_ids=None,
            sort_order_id=None,
        )

        logger.debug(
            f"Wrote data file: {data_file_path} ({file_size} bytes, {df.shape[0]} records)"
        )

        # Build manifest entries for our custom commit flow
        from pyiceberg_firestore_gcs.parquet_manifest import ManifestOptimizationConfig
        from pyiceberg_firestore_gcs.parquet_manifest import entry_to_dict

        snapshot_id = self.metadata.new_snapshot_id()

        entry = ManifestEntry(
            status=ManifestEntryStatus.ADDED,
            snapshot_id=snapshot_id,
            sequence_number=None,
            file_sequence_number=None,
            data_file=data_file,
        )

        # Convert to dict for our Parquet manifest writer
        config = ManifestOptimizationConfig()
        entries = [entry_to_dict(entry, self.metadata.schema(), config)]

        # Commit via catalog's custom append method (bypasses standard commit flow)
        logger.debug(f"Calling catalog.commit_append() with {len(entries)} entries")
        try:
            self.catalog.commit_append(
                table=self,
                snapshot_id=snapshot_id,
                manifest_entries=entries,
                added_records=df.shape[0],
                added_files_size=file_size,
                snapshot_properties=snapshot_properties,
            )
            logger.info(f"Appended 1 file with {df.shape[0]} records to {self.identifier}")
        except AttributeError as e:
            logger.error(f"commit_append() not found on catalog: {e}")
            raise
        except Exception as e:
            logger.error(f"commit_append() failed: {e}")
            raise
