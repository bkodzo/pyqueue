"""Backpressure handling for queue depth limits."""

from typing import Dict, Optional
from .config import Config


class BackpressureManager:
    """Manages backpressure and queue depth limits."""
    
    def __init__(self, config: Config):
        self.enabled = config.get("queues", "enable_backpressure", True)
        self.default_max_depth = config.get("queues", "default_max_depth", 10000)
        self.queue_limits: Dict[str, int] = {}
        self.queue_depths: Dict[str, int] = {}
    
    def set_queue_limit(self, queue_name: str, max_depth: int):
        """Set maximum depth for a queue."""
        self.queue_limits[queue_name] = max_depth
    
    def get_queue_limit(self, queue_name: str) -> int:
        """Get maximum depth for a queue."""
        return self.queue_limits.get(queue_name, self.default_max_depth)
    
    def update_depth(self, queue_name: str, depth: int):
        """Update current depth for a queue."""
        self.queue_depths[queue_name] = depth
    
    def can_accept(self, queue_name: str) -> tuple[bool, Optional[str]]:
        """Check if queue can accept more tasks. Returns (can_accept, reason)."""
        if not self.enabled:
            return True, None
        
        limit = self.get_queue_limit(queue_name)
        current_depth = self.queue_depths.get(queue_name, 0)
        
        if current_depth >= limit:
            return False, f"Queue '{queue_name}' is at capacity ({current_depth}/{limit})"
        
        return True, None
    
    def get_pressure_level(self, queue_name: str) -> float:
        """Get backpressure level (0.0 to 1.0) for a queue."""
        if not self.enabled:
            return 0.0
        
        limit = self.get_queue_limit(queue_name)
        if limit == 0:
            return 1.0
        
        current_depth = self.queue_depths.get(queue_name, 0)
        return min(current_depth / limit, 1.0)

