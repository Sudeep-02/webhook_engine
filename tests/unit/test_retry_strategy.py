import pytest
from datetime import datetime, timezone
from app.services.retry_strategy import RetryStrategy

class TestRetryStrategy:
    
    # --- Tests for get_retry_after ---
    
    def test_get_retry_after_seconds(self):
        """Should parse simple integer seconds."""
        headers = {"Retry-After": "30"}
        assert RetryStrategy.get_retry_after(headers) == 30.0

    def test_get_retry_after_http_date(self, mocker):
        """Should parse HTTP-Date and return difference from 'now'."""
        # Mock 'now' to a fixed point: 2026-04-03 12:00:00
        fixed_now = datetime(2026, 4, 3, 12, 0, 0, tzinfo=timezone.utc)
        mock_dt = mocker.patch("app.services.retry_strategy.datetime")
        mock_dt.now.return_value = fixed_now
        
        # Target is 1 hour in the future
        future_date = "Fri, 03 Apr 2026 13:00:00 GMT"
        headers = {"retry-after": future_date}
        
        delay = RetryStrategy.get_retry_after(headers)
        assert delay == 3600.0

    def test_get_retry_after_invalid(self):
        """Should return None for garbage values."""
        assert RetryStrategy.get_retry_after({"Retry-After": "invalid"}) is None
        assert RetryStrategy.get_retry_after(None) is None
        
    def test_get_retry_after_none_input(self):
        """Tests Line 22: When response_headers is None."""
        # We pass None directly to the function
        result = RetryStrategy.get_retry_after(None)
        
        # It should hit the first 'if' and return None immediately
        assert result is None

    def test_get_retry_after_missing_key(self):
        """Tests Line 26: Dictionary exists but doesn't have the retry key."""
        # The dictionary is NOT empty, but it doesn't have what we need
        headers = {"Content-Type": "application/json", "Server": "nginx"}
        
        result = RetryStrategy.get_retry_after(headers)
        
        # It should pass Line 22, but hit the 'if not retry_after' on Line 26
        assert result is None
    
    def test_calculate_delay_past_max_retries(self):
        """Covers Line 63: When attempt is already at or above MAX_RETRIES."""
        # If attempt is 5 (MAX) or 6, it should return 0.0 immediately
        delay = RetryStrategy.calculate_delay(attempt=5)
        assert delay == 0.0
        

    # --- Tests for calculate_delay ---

    def test_calculate_delay_prefers_header(self):
        """Header should override backoff logic."""
        headers = {"Retry-After": "10"}
        # Even on attempt 1, it should take the 10 from the header
        delay = RetryStrategy.calculate_delay(attempt=1, response_headers=headers)
        assert delay == 10.0

    def test_calculate_delay_caps_at_max(self):
        """Should never exceed MAX_DELAY even if header asks for it."""
        headers = {"Retry-After": "9999"}
        delay = RetryStrategy.calculate_delay(attempt=1, response_headers=headers)
        assert delay == RetryStrategy.MAX_DELAY

    def test_calculate_delay_jitter_bounds(self):
        """Jitter should be between 1.0 and the exponential limit."""
        # For attempt 3: 2^3 = 8. BASE_DELAY is 1.0. Limit is 8.0.
        for _ in range(100): # Run multiple times to check randomness
            delay = RetryStrategy.calculate_delay(attempt=3)
            assert 1.0 <= delay <= 8.0

    # --- Tests for should_retry ---

    @pytest.mark.parametrize("status,expected", [
        (429, True),  # Rate limit
        (500, True),  # Server Error
        (503, True),  # Service Unavailable
        (0, True),    # Network issues
        (400, False), # Bad Request
        (404, False), # Not Found
        (200, False), # Success (shouldn't be retrying anyway)
    ])
    def test_should_retry_status_codes(self, status, expected):
        assert RetryStrategy.should_retry(attempt=1, http_status=status) is expected

    def test_should_retry_max_attempts(self):
        """Should stop retrying once MAX_RETRIES is hit."""
        assert RetryStrategy.should_retry(RetryStrategy.MAX_RETRIES, 500) is False