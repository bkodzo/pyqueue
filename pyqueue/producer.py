"""Producer client for PyQueue - sends tasks to the broker."""

import socket
import json
from .protocol import encode_message, read_message
from .connection_pool import ConnectionPool


class Producer:
    """Client that sends tasks to the broker."""
    
    def __init__(self, host=None, port=None, use_pool=True, api_key=None, jwt_token=None, tls_enabled=False):
        import os
        self.host = host or os.getenv('BROKER_HOST', 'localhost')
        self.port = port or int(os.getenv('BROKER_PORT', '5555'))
        self.use_pool = use_pool
        self.api_key = api_key or os.getenv('PYQUEUE_API_KEY')
        self.jwt_token = jwt_token or os.getenv('PYQUEUE_JWT_TOKEN')
        self.tls_enabled = tls_enabled or (os.getenv('TLS_ENABLED', 'false').lower() == 'true')
        self.pool = None
        if use_pool:
            self.pool = ConnectionPool(self.host, self.port, max_connections=10)
        self.authenticated = False
    
    def _authenticate(self, sock):
        """Authenticate with the broker."""
        if not (self.api_key or self.jwt_token):
            return True
        
        if self.jwt_token:
            auth_data = {"type": "jwt", "value": self.jwt_token}
        elif self.api_key:
            auth_data = {"type": "apikey", "value": self.api_key}
        else:
            return True
        
        try:
            message = encode_message("AUTH", json.dumps(auth_data))
            sock.send(message)
            command, response = read_message(sock)
            if command == "OK":
                self.authenticated = True
                return True
            else:
                return False
        except Exception:
            return False
    
    def _get_connection(self):
        """Get a connection and authenticate if needed."""
        if self.pool:
            sock = self.pool.get_connection()
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.host, self.port))
        
        if self.tls_enabled:
            try:
                from .tls_wrapper import wrap_socket
                sock = wrap_socket(sock, server_side=False)
            except Exception:
                sock.close()
                return None
        
        if not self.authenticated:
            if not self._authenticate(sock):
                sock.close()
                return None
        
        return sock
    
    def push(self, payload: str, idempotency_key: str = None, priority: str = "NORMAL", queue: str = "default", transaction_id: str = None) -> str:
        """Send a task to the broker.
        
        Args:
            payload: Task payload
            idempotency_key: Optional key for deduplication
            priority: Task priority (HIGH, NORMAL, LOW)
            queue: Queue name
            transaction_id: Optional transaction ID
        
        Returns: task_id if successful, None otherwise
        """
        sock = None
        try:
            sock = self._get_connection()
            if not sock:
                return None
            
            push_data = {"payload": payload, "priority": priority, "queue": queue}
            if idempotency_key:
                push_data["idempotency_key"] = idempotency_key
            if transaction_id:
                push_data["transaction_id"] = transaction_id
            
            message = encode_message("PUSH", json.dumps(push_data))
            sock.send(message)
            
            command, response = read_message(sock)
            if command == "OK":
                task_id = response.decode('utf-8')
                return task_id
            else:
                error = response.decode('utf-8') if response else "Unknown error"
                print(f"Error: {error}")
                return None
        except Exception as e:
            print(f"Connection error: {e}")
            return None
        finally:
            if sock:
                if self.pool:
                    self.pool.return_connection(sock)
                else:
                    sock.close()
    
    def push_batch(self, tasks: list) -> dict:
        """Send multiple tasks in batch.
        
        Args:
            tasks: List of task dicts with 'payload', optional 'priority', 'queue', 'idempotency_key'
        
        Returns: dict with task_ids and count
        """
        sock = None
        try:
            sock = self._get_connection()
            if not sock:
                return None
            
            batch_data = {"tasks": tasks}
            message = encode_message("PUSH_BATCH", json.dumps(batch_data))
            sock.send(message)
            
            command, response = read_message(sock)
            if command == "OK":
                result = json.loads(response.decode('utf-8'))
                return result
            else:
                error = response.decode('utf-8') if response else "Unknown error"
                print(f"Error: {error}")
                return None
        except Exception as e:
            print(f"Connection error: {e}")
            return None
        finally:
            if sock:
                if self.pool:
                    self.pool.return_connection(sock)
                else:
                    sock.close()
    
    def schedule(self, payload: str, schedule: str, idempotency_key: str = None) -> str:
        """Schedule a task for future execution.
        
        Args:
            payload: Task payload
            schedule: Schedule expression (e.g., "every 30 seconds", "at 14:30", "cron: * * * * *")
            idempotency_key: Optional key for deduplication
        
        Returns: task_id if successful, None otherwise
        """
        sock = None
        try:
            sock = self._get_connection()
            if not sock:
                return None
            
            schedule_data = {"payload": payload, "schedule": schedule}
            if idempotency_key:
                schedule_data["idempotency_key"] = idempotency_key
            
            message = encode_message("SCHEDULE", json.dumps(schedule_data))
            sock.send(message)
            
            command, response = read_message(sock)
            if command == "OK":
                result = json.loads(response.decode('utf-8'))
                return result.get('task_id')
            else:
                error = response.decode('utf-8') if response else "Unknown error"
                print(f"Error: {error}")
                return None
        except Exception as e:
            print(f"Connection error: {e}")
            return None
        finally:
            if sock:
                if self.pool:
                    self.pool.return_connection(sock)
                else:
                    sock.close()
    
    def publish(self, topic: str, message: str) -> dict:
        """Publish a message to a topic (pub/sub).
        
        Args:
            topic: Topic name
            message: Message payload
        
        Returns: dict with published count and task_ids
        """
        sock = None
        try:
            sock = self._get_connection()
            if not sock:
                return None
            
            publish_data = {"topic": topic, "message": message}
            message_bytes = encode_message("PUBLISH", json.dumps(publish_data))
            sock.send(message_bytes)
            
            command, response = read_message(sock)
            if command == "OK":
                result = json.loads(response.decode('utf-8'))
                return result
            else:
                error = response.decode('utf-8') if response else "Unknown error"
                print(f"Error: {error}")
                return None
        except Exception as e:
            print(f"Connection error: {e}")
            return None
        finally:
            if sock:
                if self.pool:
                    self.pool.return_connection(sock)
                else:
                    sock.close()
    
    def subscribe(self, topic: str, queue_name: str = None) -> dict:
        """Subscribe to a topic (creates a queue for receiving messages).
        
        Args:
            topic: Topic name
            queue_name: Optional queue name (auto-generated if not provided)
        
        Returns: dict with topic and queue name
        """
        sock = None
        try:
            sock = self._get_connection()
            if not sock:
                return None
            
            subscribe_data = {"topic": topic}
            if queue_name:
                subscribe_data["queue"] = queue_name
            
            message_bytes = encode_message("SUBSCRIBE", json.dumps(subscribe_data))
            sock.send(message_bytes)
            
            command, response = read_message(sock)
            if command == "OK":
                result = json.loads(response.decode('utf-8'))
                return result
            else:
                error = response.decode('utf-8') if response else "Unknown error"
                print(f"Error: {error}")
                return None
        except Exception as e:
            print(f"Connection error: {e}")
            return None
        finally:
            if sock:
                if self.pool:
                    self.pool.return_connection(sock)
                else:
                    sock.close()
    
    def begin_transaction(self) -> str:
        """Begin a transaction.
        
        Returns: transaction_id if successful, None otherwise
        """
        sock = None
        try:
            sock = self._get_connection()
            if not sock:
                return None
            
            message_bytes = encode_message("BEGIN_TX", "")
            sock.send(message_bytes)
            
            command, response = read_message(sock)
            if command == "OK":
                result = json.loads(response.decode('utf-8'))
                return result.get('transaction_id')
            else:
                error = response.decode('utf-8') if response else "Unknown error"
                print(f"Error: {error}")
                return None
        except Exception as e:
            print(f"Connection error: {e}")
            return None
        finally:
            if sock:
                if self.pool:
                    self.pool.return_connection(sock)
                else:
                    sock.close()
    
    def commit_transaction(self, transaction_id: str) -> bool:
        """Commit a transaction.
        
        Args:
            transaction_id: Transaction ID from begin_transaction()
        
        Returns: True if successful, False otherwise
        """
        sock = None
        try:
            sock = self._get_connection()
            if not sock:
                return False
            
            commit_data = {"transaction_id": transaction_id}
            message_bytes = encode_message("COMMIT_TX", json.dumps(commit_data))
            sock.send(message_bytes)
            
            command, response = read_message(sock)
            if command == "OK":
                return True
            else:
                error = response.decode('utf-8') if response else "Unknown error"
                print(f"Error: {error}")
                return False
        except Exception as e:
            print(f"Connection error: {e}")
            return False
        finally:
            if sock:
                if self.pool:
                    self.pool.return_connection(sock)
                else:
                    sock.close()
    
    def rollback_transaction(self, transaction_id: str) -> bool:
        """Rollback a transaction.
        
        Args:
            transaction_id: Transaction ID from begin_transaction()
        
        Returns: True if successful, False otherwise
        """
        sock = None
        try:
            sock = self._get_connection()
            if not sock:
                return False
            
            rollback_data = {"transaction_id": transaction_id}
            message_bytes = encode_message("ROLLBACK_TX", json.dumps(rollback_data))
            sock.send(message_bytes)
            
            command, response = read_message(sock)
            if command == "OK":
                return True
            else:
                error = response.decode('utf-8') if response else "Unknown error"
                print(f"Error: {error}")
                return False
        except Exception as e:
            print(f"Connection error: {e}")
            return False
        finally:
            if sock:
                if self.pool:
                    self.pool.return_connection(sock)
                else:
                    sock.close()


def main():
    """Example producer usage."""
    import sys
    
    producer = Producer()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "schedule":
            if len(sys.argv) < 4:
                print("Usage: schedule <schedule_expr> <payload>")
                print("Examples:")
                print("  schedule 'every 30 seconds' 'my_task'")
                print("  schedule 'at 14:30' 'daily_report'")
                return
            schedule = sys.argv[2]
            payload = " ".join(sys.argv[3:])
            task_id = producer.schedule(payload, schedule)
            if task_id:
                print(f"Scheduled task: {task_id}")
                print(f"Schedule: {schedule}")
                print(f"Payload: {payload}")
            else:
                print("Failed to schedule task")
        else:
            payload = " ".join(sys.argv[1:])
            task_id = producer.push(payload)
            if task_id:
                print(f"Task submitted: {task_id}")
                print(f"Payload: {payload}")
            else:
                print("Failed to submit task")
    else:
        payload = "run_analysis"
        task_id = producer.push(payload)
        if task_id:
            print(f"Task submitted: {task_id}")
            print(f"Payload: {payload}")
        else:
            print("Failed to submit task")


if __name__ == "__main__":
    main()

