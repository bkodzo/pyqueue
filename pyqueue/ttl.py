"""Message TTL (Time To Live) management."""

import time
from typing import Optional, Dict, List, TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .broker import Task


class TTLManager:
    """Manages message TTL and expiration."""
    
    def __init__(self, default_ttl: Optional[int] = None):
        self.default_ttl = default_ttl
        self.queue_ttls: Dict[str, Optional[int]] = {}
    
    def set_queue_ttl(self, queue_name: str, ttl_seconds: Optional[int]):
        """Set TTL for a specific queue."""
        self.queue_ttls[queue_name] = ttl_seconds
    
    def get_queue_ttl(self, queue_name: str) -> Optional[int]:
        """Get TTL for a queue."""
        return self.queue_ttls.get(queue_name, self.default_ttl)
    
    def is_expired(self, task: 'Task', current_time: float) -> bool:
        """Check if a task has expired."""
        if not hasattr(task, 'ttl') or task.ttl is None:
            queue_ttl = self.get_queue_ttl(task.queue_name)
            if queue_ttl is None:
                return False
            task.ttl = queue_ttl
        
        if not hasattr(task, 'created_at'):
            return False
        
        age = current_time - task.created_at
        return age > task.ttl
    
    def filter_expired(self, tasks: List['Task'], current_time: float) -> tuple[List['Task'], List['Task']]:
        """Filter expired tasks from a list. Returns (valid_tasks, expired_tasks)."""
        valid = []
        expired = []
        
        for task in tasks:
            if self.is_expired(task, current_time):
                expired.append(task)
            else:
                valid.append(task)
        
        return valid, expired

