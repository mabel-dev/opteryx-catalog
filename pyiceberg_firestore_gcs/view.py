"""View model for storing SQL view metadata in Firestore."""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from typing import Any
from typing import Dict
from typing import Optional

import orjson
from pyiceberg.catalog import Identifier
from pyiceberg.schema import Schema


class ViewAlreadyExistsError(Exception):
    """Raised when trying to create a view that already exists."""

    pass


@dataclass
class ViewMetadata:
    """Metadata for a SQL view stored in Firestore.

    This class stores all the metadata needed to track and execute a SQL view,
    including the SQL statement, authorship, execution history, and schema information.
    """

    # Core view definition
    sql_text: str
    """The SQL statement that defines the view"""

    schema: Optional[Schema] = None
    """The schema of the view result set"""

    # Authorship and description
    author: Optional[str] = None
    """The username or identifier of the person who created the view"""

    description: Optional[str] = None
    """Human-readable description of what the view does"""

    description_author: Optional[str] = None
    """The username or identifier of the person who wrote the description"""

    # Timestamps
    created_at: Optional[datetime] = None
    """When the view was created"""

    updated_at: Optional[datetime] = None
    """When the view was last modified"""

    last_run_at: Optional[datetime] = None
    """When the view was last executed"""

    # Execution statistics
    last_row_count: Optional[int] = None
    """Number of rows returned when the view was last executed"""

    # Query planning metadata
    properties: Dict[str, Any] = field(default_factory=dict)
    """Additional properties for query planning and optimization"""

    # Catalog metadata
    view_version: str = "1"
    """Version of the view metadata format"""

    # Workspace (catalog) name for permissions and scoping
    workspace: Optional[str] = None
    """Name of the workspace (catalog) this view belongs to"""

    def to_dict(self) -> Dict[str, Any]:
        """Convert the metadata to a dictionary for Firestore storage."""
        result: Dict[str, Any] = {
            "sql_text": self.sql_text,
            "view_version": self.view_version,
        }
        if self.workspace is not None:
            result["workspace"] = self.workspace

        if self.schema is not None:
            # Store schema as JSON
            result["schema"] = self.schema.model_dump_json()

        if self.author is not None:
            result["author"] = self.author

        if self.description is not None:
            result["description"] = self.description

        if self.description_author is not None:
            result["description_author"] = self.description_author

        if self.created_at is not None:
            result["created_at"] = self.created_at

        if self.updated_at is not None:
            result["updated_at"] = self.updated_at

        if self.last_run_at is not None:
            result["last_run_at"] = self.last_run_at

        if self.last_row_count is not None:
            result["last_row_count"] = self.last_row_count

        if self.properties:
            result["properties"] = self.properties

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ViewMetadata:
        """Create a ViewMetadata instance from a Firestore document."""
        schema = None
        if "schema" in data and data["schema"]:
            # Deserialize schema from JSON
            schema_dict = orjson.loads(data["schema"])
            schema = Schema.model_validate(schema_dict)

        return cls(
            sql_text=data["sql_text"],
            schema=schema,
            author=data.get("author"),
            description=data.get("description"),
            description_author=data.get("description_author"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            last_run_at=data.get("last_run_at"),
            last_row_count=data.get("last_row_count"),
            properties=data.get("properties", {}),
            view_version=data.get("view_version", "1"),
            workspace=data.get("workspace"),
        )


@dataclass
class View:
    """Represents a SQL view in the catalog.

    A view is a stored SQL query that can be referenced like a table.
    This class provides access to the view's metadata and SQL definition.
    """

    identifier: Identifier
    """The catalog identifier (namespace, view_name) for this view"""

    metadata: ViewMetadata
    """The view metadata including SQL, schema, and execution history"""

    catalog_name: str
    """Name of the catalog this view belongs to"""

    def _is_valid_tuple_identifier(self) -> bool:
        """Check if identifier is a valid tuple with at least 2 elements."""
        return isinstance(self.identifier, tuple) and len(self.identifier) >= 2

    @property
    def name(self) -> str:
        """Return the view name."""
        if self._is_valid_tuple_identifier():
            return self.identifier[1]
        return str(self.identifier)

    @property
    def namespace(self) -> str:
        """Return the namespace."""
        if self._is_valid_tuple_identifier():
            return self.identifier[0]
        return ""

    @property
    def sql(self) -> str:
        """Return the SQL statement that defines the view."""
        return self.metadata.sql_text

    def __repr__(self) -> str:
        return f"View(identifier={self.identifier}, catalog={self.catalog_name})"
