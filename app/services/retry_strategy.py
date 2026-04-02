import random
import logging
import time
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Optional

logger = logging.getLogger("webhook_engine")

class RetryStrategy:
    MAX_RETRIES = 5
    BASE_DELAY = 1.0
    MAX_DELAY = 300.0
    
    @classmethod
    def get_retry_after(cls, response_headers: Optional[dict]) -> Optional[float]:
        """
        Parses Retry-After header. Handles both:
        1. Seconds (e.g., '30')
        2. HTTP-Date (e.g., 'Fri, 31 Dec 2026 23:59:59 GMT')
        """
        if not response_headers:
            return None
            
        # Case-insensitive header fetch
        retry_after = response_headers.get("Retry-After") or response_headers.get("retry-after")
        if not retry_after:
            return None

        try:
            # Attempt 1: Parse as seconds (Integer/Float)
            return float(retry_after)
        except (ValueError, TypeError):
            try:
                # Attempt 2: Parse as HTTP-Date string
                target_date = parsedate_to_datetime(retry_after)
                now = datetime.now(target_date.tzinfo)
                # Calculate difference in seconds
                delay = (target_date - now).total_seconds()
                return max(0.0, delay)
            except Exception:
                logger.warning(f"Could not parse Retry-After header: {retry_after}")
                return None

    @classmethod
    def calculate_delay(cls, attempt: int, response_headers: Optional[dict] = None) -> float:
        """
        Calculates how long to wait before the next delivery attempt.
        Priority: 1. Retry-After Header | 2. Full Jitter Exponential Backoff
        """
        # 1. Respect server-sent instructions first
        header_delay = cls.get_retry_after(response_headers)
        if header_delay is not None:
            # We cap it at MAX_DELAY so a single server can't stall our worker for days
            return min(header_delay, cls.MAX_DELAY)

        # 2. Fallback to Full Jitter Exponential Backoff (if no header or invalid)
        if attempt >= cls.MAX_RETRIES:
            return 0.0
        
        # Exponential backoff: 2, 4, 8, 16, 32...
        backoff_limit = min(cls.MAX_DELAY, cls.BASE_DELAY * (2 ** attempt))
        
        # Full Jitter: random range between 0 and backoff_limit
        # This is the most efficient way to de-synchronize overlapping retries
        return max(1.0, random.uniform(0, backoff_limit))

    @classmethod
    def should_retry(cls, attempt: int, http_status: int) -> bool:
        """
        Logic gate to determine if an event stays in the queue or is dropped.
        """
        if attempt >= cls.MAX_RETRIES:
            logger.warning(f"Max retries ({cls.MAX_RETRIES}) reached. Abandoning event.")
            return False
        
        # Retryable statuses:
        # 429 = Too Many Requests (Rate Limited)
        # 5xx = Server-side errors
        # 0   = Network timeouts / Connection dropped
        if http_status == 429 or http_status >= 500 or http_status == 0:
            return True
            
        # Terminal Client Errors (400, 401, 403, 404)
        # We don't retry these because the payload or URL is wrong and won't change.
        if 400 <= http_status < 500:
            logger.error(f"Terminal Failure: HTTP {http_status}. Dropping event.")
            return False

        return False