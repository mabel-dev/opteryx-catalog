"""Webhook system for Opteryx Catalog events.

This module provides webhook notification capabilities for key catalog events.
Webhooks can be delivered either directly via HTTP or asynchronously via
Google Cloud Tasks.

Configuration:
    OPTERYX_WEBHOOK_DOMAIN: Base domain for webhook delivery (e.g., router.opteryx.app)
    OPTERYX_WEBHOOK_QUEUE: Cloud Tasks queue path for async delivery
                           Format: projects/PROJECT/locations/LOCATION/queues/QUEUE
                           If not set, webhooks are sent directly via HTTP

Example:
    export OPTERYX_WEBHOOK_DOMAIN=router.opteryx.app
    export OPTERYX_WEBHOOK_QUEUE=projects/my-project/locations/us-central1/queues/webhooks

Webhook Endpoint:
    All webhooks are sent to: https://{OPTERYX_WEBHOOK_DOMAIN}/event
"""

from __future__ import annotations

import json
import os
import time
from typing import Any
from typing import Optional

import requests


class WebhookManager:
    """Manages webhook delivery for catalog events.

    Supports two delivery modes:
    1. Direct HTTP POST (when OPTERYX_WEBHOOK_QUEUE is not set)
    2. Cloud Tasks async delivery (when OPTERYX_WEBHOOK_QUEUE is set)
    """

    def __init__(
        self,
        domain: Optional[str] = None,
        queue_path: Optional[str] = None,
        timeout: int = 10,
    ):
        """Initialize the webhook manager.

        Args:
            domain: Base domain for webhooks (e.g., 'hook.opteryx.app')
                   Falls back to OPTERYX_WEBHOOK_DOMAIN env var
            queue_path: Cloud Tasks queue path for async delivery
                       Falls back to OPTERYX_WEBHOOK_QUEUE env var
            timeout: HTTP timeout in seconds for direct delivery
        """
        self.domain = domain or os.getenv("OPTERYX_WEBHOOK_DOMAIN")
        self.queue_path = queue_path or os.getenv("OPTERYX_WEBHOOK_QUEUE")
        self.timeout = timeout
        self.enabled = bool(self.domain)

        # Initialize Cloud Tasks client only if needed
        self._tasks_client = None
        if self.enabled and self.queue_path:
            try:
                from google.cloud import tasks_v2

                self._tasks_client = tasks_v2.CloudTasksClient()
            except ImportError:
                # Cloud Tasks not available, fall back to direct HTTP
                self._tasks_client = None

    def send(
        self,
        action: str,
        workspace: str,
        collection: str,
        resource_type: str,
        resource_name: str,
        payload: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Send a webhook notification.

        Args:
            action: Action type (create, delete, update, commit, execute)
            workspace: Workspace name
            collection: Collection name
            resource_type: Type of resource (dataset, view, collection)
            resource_name: Name of the resource
            payload: Additional data to include in the webhook body

        Returns:
            True if webhook was sent successfully (or queued), False otherwise
        """
        if not self.enabled:
            return False

        # Simple endpoint URL
        url = f"https://{self.domain}/event"

        # Build the payload
        body = self._build_payload(
            action=action,
            workspace=workspace,
            collection=collection,
            resource_type=resource_type,
            resource_name=resource_name,
            additional=payload or {},
        )

        # Deliver via Cloud Tasks or direct HTTP
        if self._tasks_client and self.queue_path:
            return self._send_via_cloud_tasks(url, body)
        else:
            return self._send_direct(url, body)

    def _build_payload(
        self,
        action: str,
        workspace: str,
        collection: str,
        resource_type: str,
        resource_name: str,
        additional: dict[str, Any],
    ) -> dict[str, Any]:
        """Build the webhook payload.

        Returns a standardized payload with event metadata and additional data.
        """
        return {
            "event": {
                "action": action,
                "workspace": workspace,
                "collection": collection,
                "resource_type": resource_type,
                "resource_name": resource_name,
                "timestamp": int(time.time() * 1000),  # milliseconds
            },
            "data": additional,
        }

    def _send_direct(self, url: str, payload: dict[str, Any]) -> bool:
        """Send webhook directly via HTTP POST.

        Args:
            url: Full webhook URL
            payload: JSON payload

        Returns:
            True if successful (2xx response), False otherwise
        """
        try:
            response = requests.post(
                url,
                json=payload,
                timeout=self.timeout,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "opteryx-catalog-webhook/1.0",
                },
            )
            return response.status_code >= 200 and response.status_code < 300
        except Exception:
            # Log errors in production; for now, silently fail
            return False

    def _send_via_cloud_tasks(self, url: str, payload: dict[str, Any]) -> bool:
        """Send webhook asynchronously via Cloud Tasks.

        Args:
            url: Full webhook URL
            payload: JSON payload

        Returns:
            True if task was created successfully, False otherwise
        """
        if not self._tasks_client:
            # Fall back to direct delivery if client unavailable
            return self._send_direct(url, payload)

        try:
            from google.cloud import tasks_v2

            # Create the task
            task = tasks_v2.Task(
                http_request=tasks_v2.HttpRequest(
                    http_method=tasks_v2.HttpMethod.POST,
                    url=url,
                    headers={
                        "Content-Type": "application/json",
                        "User-Agent": "opteryx-catalog-webhook/1.0",
                    },
                    body=json.dumps(payload).encode(),
                )
            )

            # Queue the task
            self._tasks_client.create_task(
                request=tasks_v2.CreateTaskRequest(
                    parent=self.queue_path,
                    task=task,
                )
            )
            return True
        except Exception:
            # Log errors in production; for now, silently fail
            return False


# Global webhook manager instance
_webhook_manager: Optional[WebhookManager] = None


def get_webhook_manager() -> WebhookManager:
    """Get or create the global webhook manager instance."""
    global _webhook_manager
    if _webhook_manager is None:
        _webhook_manager = WebhookManager()
    return _webhook_manager


def send_webhook(
    action: str,
    workspace: str,
    collection: str,
    resource_type: str,
    resource_name: str,
    payload: Optional[dict[str, Any]] = None,
) -> bool:
    """Convenience function to send a webhook via the global manager."""
    manager = get_webhook_manager()
    return manager.send(action, workspace, collection, resource_type, resource_name, payload)
