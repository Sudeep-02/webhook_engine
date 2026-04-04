from locust import HttpUser, task, between
import uuid


class WebhookUser(HttpUser):
    wait_time = between(0.1, 0.5)

    @task(3)
    def create_event(self):
        # FIX: Ensure 'payload' is a DICT, not a STRING
        self.client.post(
            "/events/",
            json={
                "event_type": "payment.success",
                "payload": {"amount": 100, "currency": "USD"},
                "idempotency_key": str(uuid.uuid4()),
            },
            name="/events/ (Create)",
        )

    @task(1)
    def health_check(self):
        self.client.get("/health", name="/health")
