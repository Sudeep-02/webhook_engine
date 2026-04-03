import httpx
import hmac
import hashlib
import json
from typing import Optional, Dict, Tuple


class WebhookDeliveryService:
    TIMEOUT_SECONDS = 10.0
    USER_AGENT = "WebhookEngine/2.0"

    @classmethod
    def sign_payload(cls, payload: dict, secret: str) -> str:
        # Standardize JSON string for consistent hashing
        payload_str = json.dumps(payload, sort_keys=True)
        signature = hmac.new(
            secret.encode("utf-8"), payload_str.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        return f"sha256={signature}"

    @classmethod
    async def deliver(
        cls, url: str, payload: Dict, secret: str, event_type: str
    ) -> Tuple[int, Optional[dict], Optional[str], Optional[dict]]:
        """
        Returns: (status_code, response_json, error_message, response_headers)
        """
        signature = cls.sign_payload(payload, secret)
        headers = {
            "Content-Type": "application/json",
            "User-Agent": cls.USER_AGENT,
            "X-Webhook-Signature": signature,
            "X-Webhook-Event": event_type,
        }

        try:
            async with httpx.AsyncClient(timeout=cls.TIMEOUT_SECONDS) as client:
                response = await client.post(url, json=payload, headers=headers)

                try:
                    resp_data = response.json()
                except Exception:
                    resp_data = {"raw": response.text[:1000]}

                return (response.status_code, resp_data, None, dict(response.headers))

        except httpx.HTTPError as e:
            return (0, None, str(e), None)
