"""Tests for the webhook system."""

import os
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest


def test_webhook_manager_disabled_without_domain():
    """Test that webhook manager is disabled when no domain is configured."""
    from opteryx_catalog.webhooks import WebhookManager

    # Clear any existing env vars
    os.environ.pop("OPTERYX_WEBHOOK_DOMAIN", None)
    os.environ.pop("OPTERYX_WEBHOOK_QUEUE", None)

    manager = WebhookManager()
    assert not manager.enabled

    # Should return False without making any HTTP calls
    result = manager.send(
        action="create",
        workspace="test",
        collection="test",
        resource_type="dataset",
        resource_name="test",
    )
    assert result is False


def test_webhook_manager_direct_http():
    """Test that webhooks are sent via direct HTTP when queue is not configured."""
    from opteryx_catalog.webhooks import WebhookManager

    with patch("opteryx_catalog.webhooks.requests.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        manager = WebhookManager(domain="router.example.com", queue_path=None)
        assert manager.enabled
        assert manager._tasks_client is None

        result = manager.send(
            action="create",
            workspace="test-workspace",
            collection="test-collection",
            resource_type="dataset",
            resource_name="test-dataset",
            payload={"location": "gs://bucket/path"},
        )

        assert result is True
        mock_post.assert_called_once()

        # Verify the call arguments
        call_args = mock_post.call_args
        assert call_args.args[0] == "https://router.example.com/event"
        assert call_args.kwargs["json"]["event"]["action"] == "create"
        assert call_args.kwargs["json"]["event"]["resource_type"] == "dataset"
        assert call_args.kwargs["json"]["event"]["resource_name"] == "test-dataset"
        assert call_args.kwargs["json"]["data"]["location"] == "gs://bucket/path"


def test_webhook_manager_payload_building():
    """Test that webhook payloads are built correctly."""
    from opteryx_catalog.webhooks import WebhookManager

    manager = WebhookManager(domain="hook.example.com")

    payload = manager._build_payload(
        action="update",
        workspace="ws",
        collection="col",
        resource_type="dataset",
        resource_name="ds",
        additional={"description": "New description"},
    )

    assert payload["event"]["action"] == "update"
    assert payload["event"]["workspace"] == "ws"
    assert payload["event"]["collection"] == "col"
    assert payload["event"]["resource_type"] == "dataset"
    assert payload["event"]["resource_name"] == "ds"
    assert "timestamp" in payload["event"]
    assert payload["data"]["description"] == "New description"


def test_webhook_http_failure_returns_false():
    """Test that HTTP failures return False without raising exceptions."""
    from opteryx_catalog.webhooks import WebhookManager

    with patch("opteryx_catalog.webhooks.requests.post") as mock_post:
        # Simulate HTTP error
        mock_post.side_effect = Exception("Connection failed")

        manager = WebhookManager(domain="router.example.com")
        result = manager.send(
            action="create",
            workspace="test",
            collection="test",
            resource_type="dataset",
            resource_name="test",
        )

        assert result is False


def test_send_webhook_convenience_function():
    """Test the convenience send_webhook function."""
    from opteryx_catalog.webhooks import send_webhook

    with patch("opteryx_catalog.webhooks.requests.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        os.environ["OPTERYX_WEBHOOK_DOMAIN"] = "router.example.com"
        os.environ.pop("OPTERYX_WEBHOOK_QUEUE", None)

        # Reset the global manager to pick up new env vars
        import opteryx_catalog.webhooks as webhook_module

        webhook_module._webhook_manager = None

        result = send_webhook(
            action="create",
            workspace="test",
            collection="test",
            resource_type="dataset",
            resource_name="test",
            payload={"snapshot_id": 123},
        )

        assert result is True
        mock_post.assert_called_once()

        # Clean up
        os.environ.pop("OPTERYX_WEBHOOK_DOMAIN", None)


def test_event_payload_builders():
    """Test the event payload builder functions."""
    from opteryx_catalog.webhooks.events import dataset_commit_payload
    from opteryx_catalog.webhooks.events import dataset_created_payload
    from opteryx_catalog.webhooks.events import view_created_payload
    from opteryx_catalog.webhooks.events import view_executed_payload

    # Test dataset created
    payload = dataset_created_payload(
        schema=None, location="gs://bucket/path", properties={"key": "value"}
    )
    assert payload["location"] == "gs://bucket/path"
    assert payload["properties"]["key"] == "value"

    # Test dataset commit
    payload = dataset_commit_payload(
        snapshot_id=123, sequence_number=5, record_count=1000, file_count=2
    )
    assert payload["snapshot_id"] == 123
    assert payload["sequence_number"] == 5
    assert payload["record_count"] == 1000
    assert payload["file_count"] == 2

    # Test view created
    payload = view_created_payload(definition="SELECT * FROM table", properties={})
    assert payload["definition"] == "SELECT * FROM table"

    # Test view executed
    payload = view_executed_payload(execution_time_ms=1500, row_count=100)
    assert payload["execution_time_ms"] == 1500
    assert payload["row_count"] == 100


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
