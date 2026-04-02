from prometheus_client import Counter, Gauge, Histogram, start_http_server
from typing import Optional

# --- Counters (only increase) ---
webhook_received_total = Counter(
    'webhook_received_total',
    'Total webhooks received',
    ['event_type']  # Label to break down by event type
)

webhook_delivered_total = Counter(
    'webhook_delivered_total',
    'Total webhooks successfully delivered',
    ['event_type']
)

webhook_failed_total = Counter(
    'webhook_failed_total',
    'Total webhooks failed',
    ['event_type', 'reason']  # Reason: timeout, connection_error, etc.
)

webhook_retry_total = Counter(
    'webhook_retry_total',
    'Total webhook retries',
    ['event_type', 'attempt_number']
)

# --- Gauges (can go up/down) ---
queue_depth_gauge = Gauge(
    'webhook_queue_depth',
    'Current number of webhooks in delivery queue'
)

redis_up_gauge = Gauge(
    'webhook_redis_up',
    'Status of Redis connection (1 for up, 0 for down)'
)

active_workers_gauge = Gauge(
    'webhook_active_workers',
    'Number of active delivery workers'
)

# --- Histograms (distribution) ---
delivery_latency_histogram = Histogram(
    'webhook_delivery_latency_seconds',
    'Time taken to deliver webhook',
    ['event_type'],
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0]  # Latency buckets in seconds
)

def start_metrics_server(port: int = 8001):
    """
    Start Prometheus metrics HTTP server.
    Metrics available at http://localhost:8001/metrics
    """
    start_http_server(port)
    print(f"📊 Metrics server started on port {port}")