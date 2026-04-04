import httpx
import hmac
import hashlib
import json
from typing import Optional, Dict, Tuple

class WebhookDeliveryService:
    TIMEOUT_CONNECT = 5.0   # Time to resolve DNS & establish TCP
    TIMEOUT_READ = 10.0     # Time to wait for response body
    USER_AGENT = "WebhookEngine/2.0"
    
    # Lazy-loaded shared client for connection pooling
    _client: Optional[httpx.AsyncClient] = None

    @classmethod
    def sign_payload(cls, payload: dict, secret: str) -> str:
        payload_str = json.dumps(payload, sort_keys=True)
        signature = hmac.new(
            secret.encode("utf-8"), payload_str.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        return f"sha256={signature}"

    @classmethod
    async def _get_client(cls) -> httpx.AsyncClient:
        """Initialize a shared client with connection pooling & tuned timeouts."""
        if cls._client is None or cls._client.is_closed:
            limits = httpx.Limits(max_connections=1000, max_keepalive_connections=200)
            
            # ✅ FIX: Set ALL four timeout parameters explicitly
            timeout = httpx.Timeout(
                connect=5.0,   # DNS + TCP handshake
                read=10.0,     # Wait for response body
                write=10.0,    # Send request body (important for large payloads)
                pool=10.0      # Wait for available connection from pool
            )
            
            cls._client = httpx.AsyncClient(
                timeout=timeout,
                limits=limits,
                headers={"User-Agent": cls.USER_AGENT},
                # http2=True,
            )
        return cls._client

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
            "X-Webhook-Signature": signature,
            "X-Webhook-Event": event_type,
        }

        client = await cls._get_client()

        try:
            response = await client.post(url, json=payload, headers=headers)

            # Safely parse JSON
            try:
                resp_data = response.json()
            except (json.JSONDecodeError, UnicodeDecodeError):
                # Fallback for non-JSON or corrupted responses
                resp_data = None

            return (response.status_code, resp_data, None, dict(response.headers))

        except httpx.ConnectError as e:
            # DNS resolution failure, connection refused, SSL handshake fail
            return (0, None, f"Connection error: {str(e)}", None)
            
        except httpx.TimeoutException as e:
            # Request timed out (DNS, connect, or read)
            return (0, None, f"Timeout error: {str(e)}", None)
            
        except httpx.HTTPError as e:
            # Fallback for other HTTP/transport errors
            return (0, None, f"HTTP transport error: {str(e)}", None)

    @classmethod
    async def close(cls):
        """Gracefully close the shared HTTP client (call on app shutdown)."""
        if cls._client and not cls._client.is_closed:
            await cls._client.aclose()
            cls._client = None