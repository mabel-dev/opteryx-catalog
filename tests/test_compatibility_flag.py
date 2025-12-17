"""Test the iceberg_compatible flag functionality.

This test verifies that:
1. The flag can be set at catalog level (defaults to True)
2. The flag can be set at table level
3. When catalog flag is True, all tables are forced to be compatible
4. When catalog flag is False, tables can override to True
"""

import unittest
from unittest.mock import patch

from pyiceberg_firestore_gcs import FirestoreCatalog


class TestCompatibilityFlag(unittest.TestCase):
    """Test iceberg_compatible flag behavior."""

    def test_catalog_default_iceberg_compatible_true(self):
        """Test that catalog defaults to iceberg_compatible=True."""
        with patch("pyiceberg_firestore_gcs.firestore_catalog._get_firestore_client"):
            catalog = FirestoreCatalog(
                catalog_name="test_catalog",
                firestore_project="test-project",
                gcs_bucket="test-bucket",
            )
            self.assertTrue(catalog.iceberg_compatible)

    def test_catalog_explicit_iceberg_compatible_false(self):
        """Test that catalog can be set to iceberg_compatible=False."""
        with patch("pyiceberg_firestore_gcs.firestore_catalog._get_firestore_client"):
            catalog = FirestoreCatalog(
                catalog_name="test_catalog",
                firestore_project="test-project",
                gcs_bucket="test-bucket",
                iceberg_compatible=False,
            )
            self.assertFalse(catalog.iceberg_compatible)

    def test_catalog_explicit_iceberg_compatible_true(self):
        """Test that catalog can be explicitly set to iceberg_compatible=True."""
        with patch("pyiceberg_firestore_gcs.firestore_catalog._get_firestore_client"):
            catalog = FirestoreCatalog(
                catalog_name="test_catalog",
                firestore_project="test-project",
                gcs_bucket="test-bucket",
                iceberg_compatible=True,
            )
            self.assertTrue(catalog.iceberg_compatible)

    def test_is_iceberg_compatible_catalog_true_forces_table_true(self):
        """Test that when catalog is iceberg_compatible=True, all tables are forced to be compatible."""
        with patch("pyiceberg_firestore_gcs.firestore_catalog._get_firestore_client"):
            catalog = FirestoreCatalog(
                catalog_name="test_catalog",
                firestore_project="test-project",
                gcs_bucket="test-bucket",
                iceberg_compatible=True,
            )

            # Even if table tries to set iceberg_compatible=False, it should be True
            result = catalog._is_iceberg_compatible({"iceberg_compatible": "false"})
            self.assertTrue(result)

            # With no table property, should be True
            result = catalog._is_iceberg_compatible({})
            self.assertTrue(result)

    def test_is_iceberg_compatible_catalog_false_allows_table_override(self):
        """Test that when catalog is iceberg_compatible=False, tables inherit False but can override to True."""
        with patch("pyiceberg_firestore_gcs.firestore_catalog._get_firestore_client"):
            catalog = FirestoreCatalog(
                catalog_name="test_catalog",
                firestore_project="test-project",
                gcs_bucket="test-bucket",
                iceberg_compatible=False,
            )

            # Table can explicitly override to True
            result = catalog._is_iceberg_compatible({"iceberg_compatible": "true"})
            self.assertTrue(result)

            # Table inherits catalog setting (False) when no property set
            result = catalog._is_iceberg_compatible({})
            self.assertFalse(result)

            # Table can be explicitly False
            result = catalog._is_iceberg_compatible({"iceberg_compatible": "false"})
            self.assertFalse(result)

    def test_is_iceberg_compatible_various_boolean_formats(self):
        """Test that _is_iceberg_compatible handles various boolean string formats."""
        with patch("pyiceberg_firestore_gcs.firestore_catalog._get_firestore_client"):
            catalog = FirestoreCatalog(
                catalog_name="test_catalog",
                firestore_project="test-project",
                gcs_bucket="test-bucket",
                iceberg_compatible=False,
            )

            # Test various True formats
            for true_val in ["true", "True", "TRUE", "1", "yes", "Yes", "YES"]:
                result = catalog._is_iceberg_compatible({"iceberg_compatible": true_val})
                self.assertTrue(result, f"Expected True for '{true_val}'")

            # Test various False formats
            for false_val in ["false", "False", "FALSE", "0", "no", "No", "NO"]:
                result = catalog._is_iceberg_compatible({"iceberg_compatible": false_val})
                self.assertFalse(result, f"Expected False for '{false_val}'")


if __name__ == "__main__":
    unittest.main()
