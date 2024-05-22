"""Tests for the Task class."""

import pytest
import time
from pyqueue.broker import Task


class TestTask:
    """Tests for Task class."""
    
    def test_task_creation(self):
        """Test basic task creation."""
        task = Task("task-1", "payload data")
        assert task.task_id == "task-1"
        assert task.payload == "payload data"
        assert task.priority == Task.PRIORITY_NORMAL
        assert task.queue_name == "default"
        assert task.retry_count == 0
    
    def test_task_with_priority(self):
        """Test task with priority."""
        high = Task("task-high", "data", priority=Task.PRIORITY_HIGH)
        normal = Task("task-normal", "data", priority=Task.PRIORITY_NORMAL)
        low = Task("task-low", "data", priority=Task.PRIORITY_LOW)
        
        assert high.priority == 0
        assert normal.priority == 1
        assert low.priority == 2
    
    def test_task_ordering_by_priority(self):
        """Test that tasks are ordered by priority."""
        high = Task("high", "data", priority=Task.PRIORITY_HIGH)
        normal = Task("normal", "data", priority=Task.PRIORITY_NORMAL)
        low = Task("low", "data", priority=Task.PRIORITY_LOW)
        
        # High priority should be less than (come before) lower priorities
        assert high < normal
        assert normal < low
        assert high < low
    
    def test_task_ordering_by_sequence(self):
        """Test that tasks with same priority are ordered by sequence."""
        task1 = Task("task1", "data", priority=Task.PRIORITY_NORMAL, sequence=1)
        task2 = Task("task2", "data", priority=Task.PRIORITY_NORMAL, sequence=2)
        
        assert task1 < task2
    
    def test_task_with_queue_name(self):
        """Test task with custom queue name."""
        task = Task("task-1", "data", queue_name="custom-queue")
        assert task.queue_name == "custom-queue"
    
    def test_task_with_ttl(self):
        """Test task with TTL."""
        task = Task("task-1", "data", ttl=3600)
        assert task.ttl == 3600
    
    def test_task_with_idempotency_key(self):
        """Test task with idempotency key."""
        task = Task("task-1", "data", idempotency_key="unique-key")
        assert task.idempotency_key == "unique-key"
    
    def test_task_with_schedule(self):
        """Test task with schedule."""
        schedule = {"type": "interval", "seconds": 60}
        task = Task("task-1", "data", schedule=schedule)
        assert task.schedule == schedule
    
    def test_task_created_at(self):
        """Test that created_at is set."""
        before = time.time()
        task = Task("task-1", "data")
        after = time.time()
        
        assert before <= task.created_at <= after

