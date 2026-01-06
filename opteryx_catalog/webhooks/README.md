# Webhook System

The Opteryx Catalog includes a webhook notification system that triggers HTTP requests on key catalog events.

## Configuration

The webhook system is configured using environment variables:

### OPTERYX_WEBHOOK_DOMAIN
The base domain where webhooks should be delivered.

```bash
export OPTERYX_WEBHOOK_DOMAIN=router.opteryx.app
```

### OPTERYX_WEBHOOK_QUEUE (Optional)
The Cloud Tasks queue path for asynchronous webhook delivery. If not set, webhooks are sent directly via HTTP POST.

```bash
export OPTERYX_WEBHOOK_QUEUE=projects/my-project/locations/us-central1/queues/webhooks
```

If you use Cloud Tasks, install the optional dependency:
```bash
pip install opteryx-catalog[webhooks]
```

## Webhook Events

Webhooks are triggered for the following events:

### Dataset Events
- **create** - New dataset created

### View Events
- **create** - New view created

## Webhook Endpoint

All webhooks are sent to a single endpoint:

```
https://router.opteryx.app/event
```

## Webhook Payload Format

All webhooks include a standardized JSON payload:

```json
{
  "event": {
    "action": "create",
    "workspace": "my-workspace",
    "collection": "my-collection",
    "resource_type": "dataset",
    "resource_name": "my-dataset",
    "timestamp": 1704556800000
  },
  "data": {
    "location": "gs://bucket/path",
    "properties": {},
    "schema": {
      "fields": [...]
    }
  }
}
```

## Implementation Details

### Direct HTTP Delivery
When `OPTERYX_WEBHOOK_QUEUE` is not set, webhooks are sent synchronously via HTTP POST with a 10-second timeout. Failed webhooks are silently ignored to prevent disrupting catalog operations.

### Cloud Tasks Delivery
When `OPTERYX_WEBHOOK_QUEUE` is configured, webhooks are queued in Google Cloud Tasks for asynchronous delivery. This provides:
- Automatic retries with exponential backoff
- Rate limiting
- Better reliability
- Non-blocking catalog operations

Cloud Tasks handles delivery failures and retries according to the queue configuration.

## Disabling Webhooks

Webhooks are automatically disabled if `OPTERYX_WEBHOOK_DOMAIN` is not set. No configuration changes are needed to disable the feature.

## Example Webhook Handler

Here's a simple Flask handler for processing webhooks:

```python
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/event', methods=['POST'])
def handle_webhook():
    payload = request.json
    
    action = payload['event']['action']
    resource_type = payload['event']['resource_type']
    resource_name = payload['event']['resource_name']
    workspace = payload['event']['workspace']
    collection = payload['event']['collection']
    
    print(f"Received {action} event for {resource_type} {resource_name}")
    print(f"Workspace: {workspace}, Collection: {collection}")
    print(f"Timestamp: {payload['event']['timestamp']}")
    print(f"Data: {payload['data']}")
    
    # Process the event
    # ...
    
    return jsonify({"status": "ok"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
```
