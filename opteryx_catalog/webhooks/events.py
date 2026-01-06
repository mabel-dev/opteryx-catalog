"""Event definitions and payload builders for webhook notifications.

This module provides helper functions to create standardized payloads
for different types of catalog events.
"""

from __future__ import annotations

from typing import Any
from typing import Optional


def dataset_created_payload(
    schema: Any,
    location: Optional[str] = None,
    properties: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Build payload for dataset creation event.

    Args:
        schema: Dataset schema (arrow or pyiceberg schema)
        location: GCS location of the dataset
        properties: Additional dataset properties

    Returns:
        Payload dictionary with dataset metadata
    """
    payload = {
        "location": location,
        "properties": properties or {},
    }

    # Include schema information if available
    try:
        if hasattr(schema, "names"):  # PyArrow schema
            payload["schema"] = {
                "fields": [
                    {"name": name, "type": str(schema.field(name).type)} for name in schema.names
                ]
            }
    except Exception:
        pass

    return payload


def dataset_deleted_payload() -> dict[str, Any]:
    """Build payload for dataset deletion event."""
    return {}


def dataset_updated_payload(
    description: Optional[str] = None,
    properties: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Build payload for dataset update event.

    Args:
        description: New description
        properties: Updated properties

    Returns:
        Payload dictionary with updated fields
    """
    return {
        "description": description,
        "properties": properties or {},
    }


def dataset_commit_payload(
    snapshot_id: int,
    sequence_number: int,
    record_count: int,
    file_count: int,
) -> dict[str, Any]:
    """Build payload for dataset commit (append) event.

    Args:
        snapshot_id: New snapshot ID
        sequence_number: Sequence number of the commit
        record_count: Number of records added
        file_count: Number of files added

    Returns:
        Payload dictionary with commit metadata
    """
    return {
        "snapshot_id": snapshot_id,
        "sequence_number": sequence_number,
        "record_count": record_count,
        "file_count": file_count,
    }


def collection_created_payload(
    properties: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Build payload for collection creation event.

    Args:
        properties: Collection properties

    Returns:
        Payload dictionary with collection metadata
    """
    return {
        "properties": properties or {},
    }


def view_created_payload(
    definition: str,
    properties: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Build payload for view creation event.

    Args:
        definition: SQL definition of the view
        properties: Additional view properties

    Returns:
        Payload dictionary with view metadata
    """
    return {
        "definition": definition,
        "properties": properties or {},
    }


def view_deleted_payload() -> dict[str, Any]:
    """Build payload for view deletion event."""
    return {}


def view_updated_payload(
    description: Optional[str] = None,
    properties: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Build payload for view update event.

    Args:
        description: New description
        properties: Updated properties

    Returns:
        Payload dictionary with updated fields
    """
    return {
        "description": description,
        "properties": properties or {},
    }


def view_executed_payload(
    execution_time_ms: Optional[int] = None,
    row_count: Optional[int] = None,
    error: Optional[str] = None,
) -> dict[str, Any]:
    """Build payload for view execution event.

    Args:
        execution_time_ms: Execution time in milliseconds
        row_count: Number of rows returned
        error: Error message if execution failed

    Returns:
        Payload dictionary with execution metadata
    """
    payload = {}
    if execution_time_ms is not None:
        payload["execution_time_ms"] = execution_time_ms
    if row_count is not None:
        payload["row_count"] = row_count
    if error is not None:
        payload["error"] = error
    return payload
