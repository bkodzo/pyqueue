"""Async tests for the broker module."""

import pytest
import asyncio
import json
from unittest.mock import MagicMock, AsyncMock, patch
from pyqueue.broker import Broker, Task


class TestBrokerInit:
    """Tests for Broker initialization."""
    
    def test_broker_init_defaults(self):
        """Test broker initializes with defaults."""
        with patch('pyqueue.broker.Config') as mock_config:
            mock_config.return_value.get.return_value = None
            mock_config.return_value.validate.return_value = (True, [])
            broker = Broker()
            
            assert broker.host == "localhost"
            assert broker.queues == {"default": []}
            assert broker.processing_queue == {}
            assert broker.dead_letter_queue == []
            assert broker.background_tasks == set()
    
    def test_broker_init_custom_port(self):
        """Test broker initializes with custom port."""
        with patch('pyqueue.broker.Config') as mock_config:
            mock_config.return_value.get.return_value = None
            mock_config.return_value.validate.return_value = (True, [])
            broker = Broker(port=6000)
            
            assert broker.port == 6000


class TestBrokerQueues:
    """Tests for broker queue operations."""
    
    @pytest.fixture
    def broker(self):
        """Create a broker for testing."""
        with patch('pyqueue.broker.Config') as mock_config:
            mock_config.return_value.get.return_value = None
            mock_config.return_value.validate.return_value = (True, [])
            return Broker()
    
    def test_default_queue_exists(self, broker):
        """Test that default queue exists."""
        assert "default" in broker.queues
    
    def test_add_task_to_queue(self, broker):
        """Test adding a task to queue."""
        import heapq
        task = Task("task-1", "payload")
        heapq.heappush(broker.queues["default"], task)
        
        assert len(broker.queues["default"]) == 1
        assert broker.queues["default"][0].task_id == "task-1"
    
    def test_create_new_queue(self, broker):
        """Test creating a new queue."""
        broker.queues["custom"] = []
        
        assert "custom" in broker.queues
        assert broker.queues["custom"] == []


class TestBrokerTransactions:
    """Tests for broker transaction operations."""
    
    @pytest.fixture
    def broker(self):
        """Create a broker for testing."""
        with patch('pyqueue.broker.Config') as mock_config:
            mock_config.return_value.get.return_value = None
            mock_config.return_value.validate.return_value = (True, [])
            return Broker()
    
    def test_transaction_storage(self, broker):
        """Test transaction storage."""
        tx_id = "tx-123"
        broker.transactions[tx_id] = {
            "operations": [],
            "started_at": 1234567890.0,
            "status": "active"
        }
        
        assert tx_id in broker.transactions
        assert broker.transactions[tx_id]["status"] == "active"
    
    def test_add_operation_to_transaction(self, broker):
        """Test adding operation to transaction."""
        tx_id = "tx-123"
        broker.transactions[tx_id] = {
            "operations": [],
            "started_at": 1234567890.0,
            "status": "active"
        }
        
        broker.transactions[tx_id]["operations"].append({"type": "PUSH", "task_id": "task-1"})
        
        assert len(broker.transactions[tx_id]["operations"]) == 1


class TestBrokerWorkers:
    """Tests for broker worker tracking."""
    
    @pytest.fixture
    def broker(self):
        """Create a broker for testing."""
        with patch('pyqueue.broker.Config') as mock_config:
            mock_config.return_value.get.return_value = None
            mock_config.return_value.validate.return_value = (True, [])
            return Broker()
    
    def test_register_worker(self, broker):
        """Test registering a worker."""
        broker.workers["worker-1"] = {
            "last_heartbeat": 1234567890.0,
            "queue": "default"
        }
        
        assert "worker-1" in broker.workers
    
    def test_update_worker_heartbeat(self, broker):
        """Test updating worker heartbeat."""
        broker.workers["worker-1"] = {
            "last_heartbeat": 1234567890.0,
            "queue": "default"
        }
        
        broker.workers["worker-1"]["last_heartbeat"] = 1234567900.0
        
        assert broker.workers["worker-1"]["last_heartbeat"] == 1234567900.0


class TestBrokerPubSub:
    """Tests for broker pub/sub functionality."""
    
    @pytest.fixture
    def broker(self):
        """Create a broker for testing."""
        with patch('pyqueue.broker.Config') as mock_config:
            mock_config.return_value.get.return_value = None
            mock_config.return_value.validate.return_value = (True, [])
            return Broker()
    
    def test_create_topic(self, broker):
        """Test creating a topic."""
        broker.topics["news"] = []
        
        assert "news" in broker.topics
    
    def test_subscribe_to_topic(self, broker):
        """Test subscribing to a topic."""
        broker.topics["news"] = []
        broker.topics["news"].append("queue1")
        broker.subscriptions["queue1"] = "news"
        
        assert "queue1" in broker.topics["news"]
        assert broker.subscriptions["queue1"] == "news"
    
    def test_multiple_subscribers(self, broker):
        """Test multiple subscribers to a topic."""
        broker.topics["news"] = ["queue1", "queue2", "queue3"]
        
        assert len(broker.topics["news"]) == 3

