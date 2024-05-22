"""Metrics collection for PyQueue broker."""

import time
import asyncio
from typing import Dict
from collections import deque


class Metrics:
    """Track and report system metrics."""
    
    def __init__(self):
        self.lock = None
        self.start_time = time.time()
        
        self.tasks_pushed = 0
        self.tasks_completed = 0
        self.tasks_expired = 0
        self.tasks_failed = 0
        
        self.recent_tasks = deque(maxlen=100)
        self.processing_times = deque(maxlen=100)
    
    async def _ensure_lock(self):
        """Ensure lock is initialized (must be called from async context)."""
        if self.lock is None:
            self.lock = asyncio.Lock()
    
    async def record_push(self, task_id: str):
        """Record a task push."""
        await self._ensure_lock()
        async with self.lock:
            self.tasks_pushed += 1
            self.recent_tasks.append(("push", task_id, time.time()))
    
    async def record_completion(self, task_id: str, processing_time: float):
        """Record a task completion."""
        await self._ensure_lock()
        async with self.lock:
            self.tasks_completed += 1
            self.processing_times.append(processing_time)
            self.recent_tasks.append(("complete", task_id, time.time()))
    
    async def record_expiration(self, task_id: str):
        """Record a task expiration."""
        await self._ensure_lock()
        async with self.lock:
            self.tasks_expired += 1
            self.recent_tasks.append(("expire", task_id, time.time()))
    
    async def record_failure(self, task_id: str):
        """Record a task failure."""
        await self._ensure_lock()
        async with self.lock:
            self.tasks_failed += 1
            self.recent_tasks.append(("fail", task_id, time.time()))
    
    async def get_stats(self, ready_count: int, processing_count: int) -> Dict:
        """Get current statistics."""
        await self._ensure_lock()
        async with self.lock:
            uptime = time.time() - self.start_time
            
            avg_processing_time = 0.0
            if self.processing_times:
                avg_processing_time = sum(self.processing_times) / len(self.processing_times)
            
            tasks_per_sec = 0.0
            if uptime > 0:
                tasks_per_sec = self.tasks_completed / uptime
            
            return {
                "uptime_seconds": round(uptime, 2),
                "tasks_pushed": self.tasks_pushed,
                "tasks_completed": self.tasks_completed,
                "tasks_expired": self.tasks_expired,
                "tasks_failed": self.tasks_failed,
                "ready_queue_depth": ready_count,
                "processing_queue_depth": processing_count,
                "avg_processing_time_seconds": round(avg_processing_time, 3),
                "tasks_per_second": round(tasks_per_sec, 2)
            }
