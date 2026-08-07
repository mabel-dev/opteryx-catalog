"""Event definitions and payload builders for webhook notifications.

This module provides helper functions to create standardized payloads
for different types of catalog events.
"""

from __future__ import annotations

from typing import Any


def dataset_created_payload(
    schema: Any,
    location: str | None = None,
    properties: dict[str, Any] | None = None,
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


def dataset_deleted_payload(
    location: str | None = None,
    dropped_by: str | None = None,
) -> dict[str, Any]:
    """Build payload for dataset deletion event.

    Args:
        location: GCS location whose files are now awaiting reclamation
        dropped_by: identity that dropped the dataset

    Returns:
        Payload dictionary describing the deletion
    """
    return {
        "location": location,
        "dropped_by": dropped_by,
    }


def dataset_renamed_payload(
    old_identifier: str,
    new_identifier: str,
    old_location: str | None = None,
    new_location: str | None = None,
    renamed_by: str | None = None,
) -> dict[str, Any]:
    """Build payload for dataset rename event.

    Both identifiers and both locations are carried because a rename may move
    the dataset between collections and always moves its files: a consumer
    tracking either the catalog name or the storage prefix needs the old and
    new value to follow it.

    Args:
        old_identifier: 'collection.dataset' the dataset was addressed by
        new_identifier: 'collection.dataset' it is addressed by now
        old_location: GCS prefix its files were under, now awaiting reclamation
        new_location: GCS prefix its files are under now
        renamed_by: identity that renamed the dataset

    Returns:
        Payload dictionary describing the rename
    """
    return {
        "old_identifier": old_identifier,
        "new_identifier": new_identifier,
        "old_location": old_location,
        "new_location": new_location,
        "renamed_by": renamed_by,
    }


def dataset_updated_payload(
    description: str | None = None,
    properties: dict[str, Any] | None = None,
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
    properties: dict[str, Any] | None = None,
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
    properties: dict[str, Any] | None = None,
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


def view_deleted_payload(dropped_by: str | None = None) -> dict[str, Any]:
    """Build payload for view deletion event.

    Args:
        dropped_by: identity that dropped the view

    Returns:
        Payload dictionary describing the deletion
    """
    return {"dropped_by": dropped_by}


def view_updated_payload(
    description: str | None = None,
    properties: dict[str, Any] | None = None,
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


def workspace_deleted_payload(dropped_by: str | None = None) -> dict[str, Any]:
    """Build payload for workspace soft-delete event.

    Args:
        dropped_by: identity that soft-deleted the workspace

    Returns:
        Payload dictionary describing the soft-delete
    """
    return {"dropped_by": dropped_by}


def workspace_restored_payload(restored_by: str | None = None) -> dict[str, Any]:
    """Build payload for workspace restore event.

    Args:
        restored_by: identity that restored the workspace

    Returns:
        Payload dictionary describing the restore
    """
    return {"restored_by": restored_by}


def workspace_locked_payload(locked_by: str | None = None) -> dict[str, Any]:
    """Build payload for workspace lock event.

    Args:
        locked_by: identity that locked the workspace

    Returns:
        Payload dictionary describing the lock
    """
    return {"locked_by": locked_by}


def workspace_unlocked_payload(unlocked_by: str | None = None) -> dict[str, Any]:
    """Build payload for workspace unlock event.

    Args:
        unlocked_by: identity that unlocked the workspace

    Returns:
        Payload dictionary describing the unlock
    """
    return {"unlocked_by": unlocked_by}


def view_executed_payload(
    execution_time_ms: int | None = None,
    row_count: int | None = None,
    error: str | None = None,
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
