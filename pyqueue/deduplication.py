"""Task deduplication using idempotency keys."""

import hashlib
from typing import Dict, Set, Optional


class DeduplicationStore:
    """Stores seen tasks to prevent duplicates."""
    
    def __init__(self, ttl_seconds: int = 3600):
        self.seen_tasks: Dict[str, float] = {}
        self.ttl_seconds = ttl_seconds
    
    def is_duplicate(self, key: str, current_time: float) -> bool:
        """Check if task with this key was already seen."""
        self._cleanup_expired(current_time)
        return key in self.seen_tasks
    
    def mark_seen(self, key: str, current_time: float):
        """Mark a task as seen."""
        self.seen_tasks[key] = current_time
    
    def _cleanup_expired(self, current_time: float):
        """Remove expired entries."""
        expired = [
            key for key, timestamp in self.seen_tasks.items()
            if current_time - timestamp > self.ttl_seconds
        ]
        for key in expired:
            del self.seen_tasks[key]
    
    def generate_key(self, payload: str, idempotency_key: Optional[str] = None) -> str:
        """Generate deduplication key from payload or idempotency key."""
        if idempotency_key:
            return f"idempotency:{idempotency_key}"
        return f"content:{hashlib.sha256(payload.encode('utf-8')).hexdigest()}"

