# pyiceberg-firestore-gcs

A Firestore + Google Cloud Storage (GCS) backed implementation of a
lightweight catalog interface. This package provides an opinionated
catalog implementation for storing table metadata documents in Firestore and
consolidated Parquet manifests in GCS.

**Important:** This library is *modelled after* Apache Iceberg but is **not
compatible** with Iceberg; it is a separate implementation with different
storage conventions and metadata layout. This library is the catalog and
metastore used by [opteryx.app](https://opteryx.app/) and uses **Firestore** as the primary
metastore and **GCS** for data and manifest storage.

---

## Features ✅

- Firestore-backed catalog and collection storage
- GCS-based table metadata storage; export/import utilities available for artifact conversion
- Table creation, registration, listing, loading, renaming, and deletion
- Commit operations that write updated metadata to GCS and persist references in Firestore
- Simple, opinionated defaults (e.g., default GCS location derived from catalog properties)
- Lightweight schema handling (supports pyarrow schemas)

## Quick start 💡

1. Ensure you have GCP credentials available to the environment. Typical approaches:
   - Set `GOOGLE_APPLICATION_CREDENTIALS` to a service account JSON key file, or
   - Use `gcloud auth application-default login` for local development.

2. Install locally (or publish to your package repo):

```bash
python -m pip install -e .
```

3. Create a `FirestoreCatalog` and use it in your application:

```python
from pyiceberg_firestore_gcs import create_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import IntegerType, StringType

catalog = create_catalog(
	"my_catalog",
	firestore_project="my-gcp-project",
	gcs_bucket="my-default-bucket",
)

# Create a collection
catalog.create_collection("example_collection")

# Create a simple PyIceberg schema
schema = Schema(
	NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
	NestedField(field_id=2, name="name", field_type=StringType(), required=False),
)

# Create a new dataset (metadata written to a GCS path derived from the bucket property)
table = catalog.create_dataset(("example_collection", "users"), schema)

# Or register a table if you already have a metadata JSON in GCS
catalog.register_table(("example_namespace", "events"), "gs://my-bucket/path/to/events/metadata/00000001.json")

# Load a table
tbl = catalog.load_dataset(("example_namespace", "users"))
print(tbl.metadata)
```

## Configuration and environment 🔧

- GCP authentication: Use `GOOGLE_APPLICATION_CREDENTIALS` or Application Default Credentials
- `firestore_project` and `firestore_database` can be supplied when creating the catalog
- `gcs_bucket` is recommended to allow `create_dataset` to write metadata automatically; otherwise pass `location` explicitly to `create_dataset`
 - The catalog writes consolidated Parquet manifests and does not write manifest-list artifacts in the hot path. Use the provided export/import utilities for artifact conversion when necessary.

Example environment variables:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
export GOOGLE_CLOUD_PROJECT="my-gcp-project"
```

### Manifest format

This catalog writes consolidated Parquet manifests for fast query planning and stores table metadata in Firestore. Manifests and data files are stored in GCS. If you need different artifact formats, use the provided export/import utilities to convert manifests outside the hot path.

## API overview 📚

The package exports a factory helper `create_catalog` and the `FirestoreCatalog` class.

Key methods include:
- `create_collection(collection, properties={}, exists_ok=False)`
- `drop_namespace(namespace)`
- `list_namespaces()`
- `create_dataset(identifier, schema, location=None, partition_spec=None, sort_order=None, properties={})`
- `register_table(identifier, metadata_location)`
- `load_dataset(identifier)`
- `list_datasets(namespace)`
- `drop_dataset(identifier)`
- `rename_table(from_identifier, to_identifier)`
- `commit_table(table, requirements, updates)`
- `create_view(identifier, sql, schema=None, author=None, description=None, properties={})`
- `load_view(identifier)`
- `list_views(namespace)`
- `view_exists(identifier)`
- `drop_view(identifier)`
- `update_view_execution_metadata(identifier, row_count=None, execution_time=None)`

### Views 👁️

Views are SQL queries stored in the catalog that can be referenced like tables. Each view includes:
- **SQL statement**: The query that defines the view
- **Schema**: The expected result schema (optional but recommended)
- **Metadata**: Author, description, creation/update timestamps
- **Execution history**: Last run time, row count, execution time

Example usage:
```python
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import IntegerType, StringType

# Create a schema for the view
schema = Schema(
    NestedField(field_id=1, name="user_id", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="username", field_type=StringType(), required=False),
)

# Create a view
view = catalog.create_view(
    identifier=("my_namespace", "active_users"),
    sql="SELECT user_id, username FROM users WHERE active = true",
    schema=schema,
    author="data_team",
    description="View of all active users in the system"
)

# Load a view
view = catalog.load_view(("my_namespace", "active_users"))
print(f"SQL: {view.sql}")
print(f"Schema: {view.metadata.schema}")

# Update execution metadata after running the view
catalog.update_view_execution_metadata(
    ("my_namespace", "active_users"),
    row_count=1250,
    execution_time=0.45
)
```

Notes about behavior:
- `create_dataset` will try to infer a default GCS location using the provided `gcs_bucket` property if `location` is omitted.
- `register_table` validates that the provided `metadata_location` points to an existing GCS blob.
- Views are stored as Firestore documents with complete metadata including SQL, schema, authorship, and execution history.
- Table transactions are intentionally unimplemented.

## Development & Linting 🧪

This package includes a small `Makefile` target to run linting and formatting tools (`ruff`, `isort`, `pycln`).

Install dev tools and run linters with:

```bash
python -m pip install --upgrade pycln isort ruff
make lint
```

Running tests (if you add tests):

```bash
python -m pytest
```

## Compaction 🔧

This catalog supports small file compaction to improve query performance. See [COMPACTION.md](COMPACTION.md) for detailed design documentation.

### Quick Start

```python
from pyiceberg_firestore_gcs import create_catalog
from pyiceberg_firestore_gcs.compaction import compact_table, get_compaction_stats

catalog = create_catalog(...)

# Check if compaction is needed
table = catalog.load_dataset(("namespace", "dataset_name"))
stats = get_compaction_stats(table)
print(f"Small files: {stats['small_file_count']}")

# Run compaction
result = compact_table(catalog, ("namespace", "table_name"))
print(f"Compacted {result.files_rewritten} files")
```

### Configuration

Control compaction behavior via table properties:

```python
table = catalog.create_dataset(
    identifier=("namespace", "table_name"),
    schema=schema,
    properties={
        "compaction.enabled": "true",
        "compaction.min-file-count": "10",
        "compaction.max-small-file-size-bytes": "33554432",  # 32 MB
        "write.target-file-size-bytes": "134217728"  # 128 MB
    }
)
```

## Limitations & KNOWN ISSUES ⚠️

- No support for dataset-level transactions. `create_dataset_transaction` raises `NotImplementedError`.
- The catalog stores metadata location references in Firestore; purging metadata files from GCS is not implemented.
- This is an opinionated implementation intended for internal or controlled environments. Review for production constraints before use in multi-tenant environments.

## Contributing 🤝

Contributions are welcome. Please follow these steps:

1. Fork the repository and create a feature branch.
2. Run and pass linting and tests locally.
3. Submit a PR with a clear description of the change.

Please add unit tests and docs for new behaviors.

---

If you'd like, I can also add usage examples that show inserting rows using PyIceberg readers/writers, or add CI testing steps to the repository. ✅
