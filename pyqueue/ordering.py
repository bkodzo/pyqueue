"""Message ordering guarantees."""

import asyncio
from typing import Dict, Optional
from collections import defaultdict


class OrderingManager:
    """Manages message ordering with sequence numbers."""
    
    def __init__(self):
        self.sequence_numbers: Dict[str, int] = defaultdict(int)
        self.partition_keys: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.lock = None
    
    async def _ensure_lock(self):
        """Ensure lock is initialized (must be called from async context)."""
        if self.lock is None:
            self.lock = asyncio.Lock()
    
    async def get_next_sequence(self, queue_name: str, partition_key: Optional[str] = None) -> int:
        """Get next sequence number for a queue or partition."""
        await self._ensure_lock()
        async with self.lock:
            if partition_key:
                self.partition_keys[queue_name][partition_key] += 1
                return self.partition_keys[queue_name][partition_key]
            else:
                self.sequence_numbers[queue_name] += 1
                return self.sequence_numbers[queue_name]
