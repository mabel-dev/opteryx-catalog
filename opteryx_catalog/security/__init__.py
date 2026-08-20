"""Secret-handling helpers for the catalog.

`security.kms` is the envelope-encryption module shared by workspace catalog
bindings (binding.py / WORKSPACE_CATALOG_RESOLUTION.md) and external-table
credentials (the external-tables plan). Dependencies are optional - install
`opteryx-catalog[kms]` - and imported lazily inside the functions, following
the same pattern as the `s3` and `alerts` extras.
"""
