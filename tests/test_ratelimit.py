"""Tests for the rate limiting module."""

import pytest
import asyncio
from pyqueue.ratelimit import RateLimiter


class TestRateLimiter:
    """Tests for RateLimiter."""
    
    @pytest.fixture
    def rate_limiter(self):
        """Create a rate limiter for testing."""
        return RateLimiter(default_rate=10.0, default_burst=5)
    
    @pytest.mark.asyncio
    async def test_first_request_allowed(self, rate_limiter):
        """Test that first request is allowed."""
        allowed = await rate_limiter.check_rate_limit("client1")
        assert allowed is True
    
    @pytest.mark.asyncio
    async def test_burst_requests_allowed(self, rate_limiter):
        """Test that burst requests within limit are allowed."""
        # Should allow up to burst size
        for _ in range(5):
            allowed = await rate_limiter.check_rate_limit("client1")
            assert allowed is True
    
    @pytest.mark.asyncio
    async def test_over_burst_denied(self, rate_limiter):
        """Test that requests over burst are denied."""
        # Use all burst
        for _ in range(5):
            await rate_limiter.check_rate_limit("client1")
        
        # Next request should be denied (no time to refill)
        allowed = await rate_limiter.check_rate_limit("client1")
        assert allowed is False
    
    @pytest.mark.asyncio
    async def test_different_clients_separate_buckets(self, rate_limiter):
        """Test that different clients have separate rate limits."""
        # Exhaust client1's bucket
        for _ in range(5):
            await rate_limiter.check_rate_limit("client1")
        
        # client2 should still have full bucket
        allowed = await rate_limiter.check_rate_limit("client2")
        assert allowed is True
    
    @pytest.mark.asyncio
    async def test_tokens_refill_over_time(self, rate_limiter):
        """Test that tokens refill over time."""
        # Use all tokens
        for _ in range(5):
            await rate_limiter.check_rate_limit("client1")
        
        # Should be denied now
        assert await rate_limiter.check_rate_limit("client1") is False
        
        # Wait for refill (at 10 tokens/sec, 0.2s should give ~2 tokens)
        await asyncio.sleep(0.2)
        
        # Should be allowed again
        allowed = await rate_limiter.check_rate_limit("client1")
        assert allowed is True

