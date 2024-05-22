"""Tests for broker module."""

import pytest
import time
import socket
from pyqueue.broker import Broker, Task
from pyqueue.protocol import encode_message, read_message
import json


@pytest.fixture
def broker():
    """Create a test broker instance."""
    broker = Broker(host='localhost', port=0, visibility_timeout=5)
    broker.start()
    time.sleep(0.1)
    yield broker
    broker.stop()


def test_push_pull(broker):
    """Test basic push and pull operations."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((broker.host, broker.port))
    
    push_data = json.dumps({"payload": "test task"})
    message = encode_message("PUSH", push_data)
    sock.send(message)
    
    command, response = read_message(sock)
    assert command == "OK"
    task_id = response.decode('utf-8')
    
    sock.close()
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((broker.host, broker.port))
    
    pull_data = json.dumps({"queue": "default"})
    message = encode_message("PULL", pull_data)
    sock.send(message)
    
    command, response = read_message(sock)
    assert command == "JOB"
    
    job_data = json.loads(response.decode('utf-8'))
    assert job_data["task_id"] == task_id
    assert job_data["payload"] == "test task"
    
    sock.close()


def test_visibility_timeout(broker):
    """Test visibility timeout mechanism."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((broker.host, broker.port))
    
    push_data = json.dumps({"payload": "timeout test"})
    message = encode_message("PUSH", push_data)
    sock.send(message)
    
    command, response = read_message(sock)
    task_id = response.decode('utf-8')
    sock.close()
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((broker.host, broker.port))
    
    pull_data = json.dumps({"queue": "default"})
    message = encode_message("PULL", pull_data)
    sock.send(message)
    
    command, response = read_message(sock)
    assert command == "JOB"
    sock.close()
    
    time.sleep(6)
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((broker.host, broker.port))
    
    pull_data = json.dumps({"queue": "default"})
    message = encode_message("PULL", pull_data)
    sock.send(message)
    
    command, response = read_message(sock)
    assert command == "JOB"
    
    job_data = json.loads(response.decode('utf-8'))
    assert job_data["task_id"] == task_id
    
    sock.close()


def test_priority_queue(broker):
    """Test priority queue ordering."""
    priorities = ["LOW", "NORMAL", "HIGH"]
    
    for priority in priorities:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((broker.host, broker.port))
        
        push_data = json.dumps({"payload": f"task_{priority}", "priority": priority})
        message = encode_message("PUSH", push_data)
        sock.send(message)
        read_message(sock)
        sock.close()
    
    time.sleep(0.1)
    
    pulled_priorities = []
    for _ in range(3):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((broker.host, broker.port))
        
        pull_data = json.dumps({"queue": "default"})
        message = encode_message("PULL", pull_data)
        sock.send(message)
        
        command, response = read_message(sock)
        if command == "JOB":
            job_data = json.loads(response.decode('utf-8'))
            pulled_priorities.append(job_data["priority"])
        sock.close()
    
    assert pulled_priorities == [0, 1, 2]

