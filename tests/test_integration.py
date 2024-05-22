"""Integration tests for end-to-end workflows."""

import pytest
import time
import threading
from pyqueue.broker import Broker
from pyqueue.producer import Producer
from pyqueue.worker import Worker


@pytest.fixture
def broker():
    """Create a test broker."""
    broker = Broker(host='localhost', port=0, visibility_timeout=5)
    broker.start()
    time.sleep(0.1)
    yield broker
    broker.stop()


def test_producer_worker_integration(broker):
    """Test producer and worker working together."""
    producer = Producer(host=broker.host, port=broker.port, use_pool=False)
    worker = Worker(host=broker.host, port=broker.port, use_pool=False)
    
    task_id = producer.push("integration test task")
    assert task_id is not None
    
    result = worker.pull_and_process()
    assert result is True


def test_batch_operations(broker):
    """Test batch push and pull operations."""
    producer = Producer(host=broker.host, port=broker.port, use_pool=False)
    
    tasks = [
        {"payload": f"task_{i}", "priority": "NORMAL"}
        for i in range(5)
    ]
    
    result = producer.push_batch(tasks)
    assert result is not None
    assert len(result.get("task_ids", [])) == 5


def test_transactions(broker):
    """Test transaction commit and rollback."""
    producer = Producer(host=broker.host, port=broker.port, use_pool=False)
    
    tx_id = producer.begin_transaction()
    assert tx_id is not None
    
    task_id1 = producer.push("task1", transaction_id=tx_id)
    task_id2 = producer.push("task2", transaction_id=tx_id)
    
    success = producer.commit_transaction(tx_id)
    assert success is True
    
    time.sleep(0.1)
    
    with broker.queue_lock:
        assert len(broker.queues["default"]) == 2

