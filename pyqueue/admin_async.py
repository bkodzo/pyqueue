"""Async Admin CLI client for PyQueue broker management."""

import asyncio
import json
from typing import Optional, Dict, Any
from .protocol_async import encode_message, read_message_async, write_message_async


class AsyncAdminClient:
    """Async admin client for broker management operations."""
    
    def __init__(self, host: str = None, port: int = None, api_key: str = None, 
                 jwt_token: str = None, tls_enabled: bool = False):
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
            return True
        
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
        except Exception:
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
            response_cmd, response_payload = await read_message_async(self._reader)
            if response_cmd == "OK":
                self.authenticated = True
                return True
            else:
                return False
        except Exception:
            return False
    
    async def _send_command(self, command: str, payload: Optional[str] = None) -> Dict[str, Any]:
        """Send an admin command to the broker."""
        try:
            if not await self._connect():
                return {"error": "Failed to connect or authenticate"}
            
            if payload:
                await write_message_async(self._writer, command, payload.encode('utf-8'))
            else:
                await write_message_async(self._writer, command, b"")
            
            response_cmd, response_payload = await read_message_async(self._reader)
            
            if response_cmd == "ERROR":
                return {"error": response_payload.decode('utf-8') if response_payload else "Unknown error"}
            
            if response_payload:
                try:
                    return json.loads(response_payload.decode('utf-8'))
                except json.JSONDecodeError:
                    return {"response": response_payload.decode('utf-8')}
            
            return {"status": "ok"}
        except Exception as e:
            return {"error": str(e)}
    
    async def list_queues(self) -> Dict[str, Any]:
        """List all queues."""
        return await self._send_command("ADMIN_LIST_QUEUES")
    
    async def get_queue_info(self, queue_name: str) -> Dict[str, Any]:
        """Get information about a specific queue."""
        return await self._send_command("ADMIN_QUEUE_INFO", queue_name)
    
    async def purge_queue(self, queue_name: str) -> Dict[str, Any]:
        """Purge all tasks from a queue."""
        return await self._send_command("ADMIN_PURGE_QUEUE", queue_name)
    
    async def delete_queue(self, queue_name: str) -> Dict[str, Any]:
        """Delete a queue."""
        return await self._send_command("ADMIN_DELETE_QUEUE", queue_name)
    
    async def get_task_info(self, task_id: str) -> Dict[str, Any]:
        """Get information about a specific task."""
        return await self._send_command("ADMIN_GET_TASK", task_id)
    
    async def list_tasks(self, queue_name: Optional[str] = None, status: Optional[str] = None) -> Dict[str, Any]:
        """List tasks, optionally filtered by queue and status."""
        filters = {}
        if queue_name:
            filters["queue"] = queue_name
        if status:
            filters["status"] = status
        payload = json.dumps(filters) if filters else None
        return await self._send_command("ADMIN_LIST_TASKS", payload)
    
    async def cancel_task(self, task_id: str) -> Dict[str, Any]:
        """Cancel a task."""
        return await self._send_command("ADMIN_CANCEL_TASK", task_id)
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive broker statistics."""
        return await self._send_command("STATS")
    
    async def get_workers(self) -> Dict[str, Any]:
        """Get list of active workers."""
        return await self._send_command("ADMIN_LIST_WORKERS")
    
    async def get_dlq(self) -> Dict[str, Any]:
        """Get dead letter queue tasks."""
        return await self._send_command("ADMIN_GET_DLQ")
    
    async def replay_messages(self, from_timestamp: Optional[float] = None, 
                      to_timestamp: Optional[float] = None,
                      task_ids: Optional[list] = None,
                      queue_name: Optional[str] = None) -> Dict[str, Any]:
        """Replay messages from journal."""
        params = {}
        if from_timestamp:
            params["from_timestamp"] = from_timestamp
        if to_timestamp:
            params["to_timestamp"] = to_timestamp
        if task_ids:
            params["task_ids"] = task_ids
        if queue_name:
            params["queue_name"] = queue_name
        return await self._send_command("ADMIN_REPLAY", json.dumps(params))
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on broker."""
        return await self._send_command("ADMIN_HEALTH")
    
    async def set_queue_ttl(self, queue_name: str, ttl: int) -> Dict[str, Any]:
        """Set TTL for a queue."""
        params = {"queue": queue_name, "ttl": ttl}
        return await self._send_command("ADMIN_SET_QUEUE_TTL", json.dumps(params))
    
    async def set_queue_limit(self, queue_name: str, max_depth: int) -> Dict[str, Any]:
        """Set maximum depth for a queue."""
        params = {"queue": queue_name, "max_depth": max_depth}
        return await self._send_command("ADMIN_SET_QUEUE_LIMIT", json.dumps(params))
    
    async def get_queue_metrics(self, queue_name: str) -> Dict[str, Any]:
        """Get metrics for a specific queue."""
        return await self._send_command("ADMIN_QUEUE_METRICS", queue_name)
    
    async def close(self):
        """Close the connection."""
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
            self._reader = None
            self._writer = None

