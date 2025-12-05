# pyiceberg-firestore-gcs

A Firestore + Google Cloud Storage (GCS) backed implementation of the PyIceberg
catalog interface. This package provides a straightforward, opinionated
catalog implementation for storing table metadata documents in Firestore while
storing the Iceberg table metadata JSON in GCS.

This project is intended to be used as a catalog component for PyIceberg in
GCP-based environments.

---

## Features ✅

- Firestore-backed catalog and namespace storage
- GCS-based Iceberg table metadata storage
- Table creation, registration, listing, loading, renaming, and deletion
- Commit operations that write updated metadata to GCS and persist references in Firestore
- Simple, opinionated defaults (e.g., default GCS location derived from catalog properties)
- Lightweight schema handling compatible with PyIceberg (supports pyarrow schemas and PyIceberg Schema)

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

# Create a namespace
catalog.create_namespace("example_namespace")

# Create a simple PyIceberg schema
schema = Schema(
	NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
	NestedField(field_id=2, name="name", field_type=StringType(), required=False),
)

# Create a new table (metadata written to a GCS path derived from the bucket property)
table = catalog.create_table(("example_namespace", "users"), schema)

# Or register a table if you already have a metadata JSON in GCS
catalog.register_table(("example_namespace", "events"), "gs://my-bucket/path/to/events/metadata/00000001.json")

# Load a table
tbl = catalog.load_table(("example_namespace", "users"))
print(tbl.metadata)
```

## Configuration and environment 🔧

- GCP authentication: Use `GOOGLE_APPLICATION_CREDENTIALS` or Application Default Credentials
- `firestore_project` and `firestore_database` can be supplied when creating the catalog
- `gcs_bucket` is recommended to allow `create_table` to write metadata automatically; otherwise pass `location` explicitly to `create_table`

Example environment variables:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
export GOOGLE_CLOUD_PROJECT="my-gcp-project"
```

## API overview 📚

The package exports a factory helper `create_catalog` and the `FirestoreCatalog` class.

Key methods include:
- `create_namespace(namespace, properties={}, exists_ok=False)`
- `drop_namespace(namespace)`
- `list_namespaces()`
- `create_table(identifier, schema, location=None, partition_spec=None, sort_order=None, properties={})`
- `register_table(identifier, metadata_location)`
- `load_table(identifier)`
- `list_tables(namespace)`
- `drop_table(identifier)`
- `rename_table(from_identifier, to_identifier)`
- `commit_table(table, requirements, updates)`

Notes about behavior:
- `create_table` will try to infer a default GCS location using the provided `gcs_bucket` property if `location` is omitted.
- `register_table` validates that the provided `metadata_location` points to an existing GCS blob.
- Views and table transactions are intentionally unimplemented.

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

## Limitations & KNOWN ISSUES ⚠️

- No support for table-level transactions. `create_table_transaction` raises `NotImplementedError`.
- Views are not implemented — attempting to use view-related APIs will either return an empty list or raise `NoSuchTableError`.
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
