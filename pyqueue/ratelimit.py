"""Rate limiting using token bucket algorithm."""

import time
import asyncio
from typing import Dict, Optional
from collections import defaultdict


class RateLimiter:
    """Token bucket rate limiter per producer."""
    
    def __init__(self, default_rate: float = 10.0, default_burst: int = 20):
        self.rate = default_rate
        self.burst = default_burst
        self.buckets: Dict[str, dict] = {}
        self.lock = None
    
    async def _ensure_lock(self):
        """Ensure lock is initialized (must be called from async context)."""
        if self.lock is None:
            self.lock = asyncio.Lock()
    
    async def check_rate_limit(self, producer_id: str, current_time: Optional[float] = None) -> bool:
        """Check if producer is within rate limit.
        
        Returns: True if allowed, False if rate limited
        """
        if current_time is None:
            current_time = time.time()
        
        await self._ensure_lock()
        async with self.lock:
            if producer_id not in self.buckets:
                self.buckets[producer_id] = {
                    "tokens": float(self.burst),
                    "last_update": current_time
                }
            
            bucket = self.buckets[producer_id]
            
            elapsed = current_time - bucket["last_update"]
            tokens_to_add = elapsed * self.rate
            bucket["tokens"] = min(self.burst, bucket["tokens"] + tokens_to_add)
            bucket["last_update"] = current_time
            
            if bucket["tokens"] >= 1.0:
                bucket["tokens"] -= 1.0
                return True
            
            return False
