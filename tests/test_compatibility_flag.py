import pytest

pytest.skip(
    "iceberg_compatible flag removed — use export_to_iceberg/import_from_iceberg for Avro interoperability",
    allow_module_level=True,
)
