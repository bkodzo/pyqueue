"""Async Producer client for PyQueue - sends tasks to the broker."""

import asyncio
import json
from typing import Optional
from .protocol_async import encode_message, read_message_async, write_message_async


class AsyncProducer:
    """Async client that sends tasks to the broker."""
    
    def __init__(self, host=None, port=None, api_key=None, jwt_token=None, tls_enabled=False):
        import os
        self.host = host or os.getenv('BROKER_HOST', 'localhost')
        self.port = port or int(os.getenv('BROKER_PORT', '5555'))
        self.api_key = api_key or os.getenv('PYQUEUE_API_KEY')
        self.jwt_token = jwt_token or os.getenv('PYQUEUE_JWT_TOKEN')
        self.tls_enabled = tls_enabled or (os.getenv('TLS_ENABLED', 'false').lower() == 'true')
        self.authenticated = False
        self._reader = None
        self._writer = None
    
    async def _connect(self):
        """Establish connection to broker."""
        if self._writer and not self._writer.is_closing():
            return
        
        try:
            if self.tls_enabled:
                import ssl
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                self._reader, self._writer = await asyncio.open_connection(
                    self.host, self.port, ssl=ssl_context
                )
            else:
                self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
            
            await self._authenticate()
            return True
        except Exception as e:
            self._reader = None
            self._writer = None
            return False
    
    async def _authenticate(self):
        """Authenticate with the broker."""
        if not (self.api_key or self.jwt_token):
            self.authenticated = True
            return True
        
        if self.jwt_token:
            auth_data = {"type": "jwt", "value": self.jwt_token}
        elif self.api_key:
            auth_data = {"type": "apikey", "value": self.api_key}
        else:
            self.authenticated = True
            return True
        
        try:
            await write_message_async(self._writer, "AUTH", json.dumps(auth_data).encode('utf-8'))
            command, response = await read_message_async(self._reader)
            if command == "OK":
                self.authenticated = True
                return True
            else:
                return False
        except Exception:
            return False
    
    async def push(self, payload: str, idempotency_key: str = None, priority: str = "NORMAL", 
                   queue: str = "default", transaction_id: str = None) -> Optional[str]:
        """Send a task to the broker.
        
        Args:
            payload: Task payload
            idempotency_key: Optional key for deduplication
            priority: Task priority (HIGH, NORMAL, LOW)
            queue: Queue name
            transaction_id: Optional transaction ID
        
        Returns: task_id if successful, None otherwise
        """
        try:
            if not await self._connect():
                return None
            
            push_data = {"payload": payload, "priority": priority, "queue": queue}
            if idempotency_key:
                push_data["idempotency_key"] = idempotency_key
            if transaction_id:
                push_data["transaction_id"] = transaction_id
            
            await write_message_async(self._writer, "PUSH", json.dumps(push_data).encode('utf-8'))
            
            command, response = await read_message_async(self._reader)
            if command == "OK":
                return response.decode('utf-8') if response else None
            else:
                return None
        except Exception:
            return None
    
    async def push_batch(self, tasks: list) -> dict:
        """Send multiple tasks in a batch.
        
        Args:
            tasks: List of task dicts with 'payload', 'priority', 'queue', etc.
        
        Returns: dict with 'task_ids' and 'count'
        """
        try:
            if not await self._connect():
                return {"error": "Failed to connect"}
            
            batch_data = {"tasks": tasks}
            await write_message_async(self._writer, "PUSH_BATCH", json.dumps(batch_data).encode('utf-8'))
            
            command, response = await read_message_async(self._reader)
            if command == "OK":
                return json.loads(response.decode('utf-8'))
            else:
                return {"error": response.decode('utf-8') if response else "Unknown error"}
        except Exception as e:
            return {"error": str(e)}
    
    async def schedule(self, payload: str, schedule: str, idempotency_key: str = None) -> Optional[dict]:
        """Schedule a task for later execution.
        
        Args:
            payload: Task payload
            schedule: Schedule string (cron, interval, daily)
            idempotency_key: Optional key for deduplication
        
        Returns: dict with 'task_id' and 'next_run' if successful, None otherwise
        """
        try:
            if not await self._connect():
                return None
            
            schedule_data = {"payload": payload, "schedule": schedule}
            if idempotency_key:
                schedule_data["idempotency_key"] = idempotency_key
            
            await write_message_async(self._writer, "SCHEDULE", json.dumps(schedule_data).encode('utf-8'))
            
            command, response = await read_message_async(self._reader)
            if command == "OK":
                return json.loads(response.decode('utf-8'))
            else:
                return None
        except Exception:
            return None
    
    async def close(self):
        """Close the connection."""
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
            self._reader = None
            self._writer = None

