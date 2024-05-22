"""Async Worker client for PyQueue - processes tasks from the broker."""

import asyncio
import json
import time
from typing import Optional, Callable
from .protocol_async import encode_message, read_message_async, write_message_async


class AsyncWorker:
    """Async client that pulls and processes tasks from the broker."""
    
    def __init__(self, host=None, port=None, worker_id=None, queue_name="default", tls_enabled=False):
        import os
        import uuid
        self.host = host or os.getenv('BROKER_HOST', 'localhost')
        self.port = port or int(os.getenv('BROKER_PORT', '5555'))
        self.worker_id = worker_id or str(uuid.uuid4())
        self.queue_name = queue_name
        self.running = False
        self.tls_enabled = tls_enabled or (os.getenv('TLS_ENABLED', 'false').lower() == 'true')
        self._reader = None
        self._writer = None
        self.process_task_callback: Optional[Callable[[str, str], bool]] = None
    
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
            return True
        except Exception:
            self._reader = None
            self._writer = None
            return False
    
    async def set_process_callback(self, callback: Callable[[str, str], bool]):
        """Set the callback function for processing tasks.
        
        Args:
            callback: Async function that takes (task_id, payload) and returns bool
        """
        self.process_task_callback = callback
    
    async def process_task(self, task_id: str, payload: str) -> bool:
        """Process a task (override this or use callback).
        
        Args:
            task_id: Task ID
            payload: Task payload
        
        Returns: True if successful, False otherwise
        """
        if self.process_task_callback:
            if asyncio.iscoroutinefunction(self.process_task_callback):
                return await self.process_task_callback(task_id, payload)
            else:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, self.process_task_callback, task_id, payload)
        
        print(f"Processing task {task_id}: {payload}")
        await asyncio.sleep(1)
        print(f"Task {task_id} completed")
        return True
    
    async def pull_and_process(self) -> bool:
        """Pull a task from broker and process it.
        
        Returns: True if task was processed, False if no task available
        """
        try:
            if not await self._connect():
                return False
            
            pull_data = json.dumps({"queue": self.queue_name})
            await write_message_async(self._writer, "PULL", pull_data.encode('utf-8'))
            
            command, response = await read_message_async(self._reader)
            
            if command == "NO_JOB":
                return False
            
            if command != "JOB":
                return False
            
            job_data = json.loads(response.decode('utf-8'))
            task_id = job_data["task_id"]
            payload = job_data["payload"]
            
            success = await self.process_task(task_id, payload)
            
            if success:
                if not await self._connect():
                    return False
                
                ack_data = json.dumps({"task_id": task_id})
                await write_message_async(self._writer, "ACK", ack_data.encode('utf-8'))
                
                ack_response, _ = await read_message_async(self._reader)
                return ack_response == "OK"
            
            return False
        except Exception as e:
            print(f"Error processing task: {e}")
            return False
    
    async def send_heartbeat(self):
        """Send heartbeat to broker."""
        try:
            if not await self._connect():
                return
            
            heartbeat_data = json.dumps({"worker_id": self.worker_id})
            await write_message_async(self._writer, "HEARTBEAT", heartbeat_data.encode('utf-8'))
            await read_message_async(self._reader)
        except Exception:
            pass
    
    async def run(self, poll_interval: float = 0.5):
        """Run worker in continuous loop."""
        self.running = True
        print(f"Worker {self.worker_id} started, polling for tasks from queue '{self.queue_name}'...")
        
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        try:
            while self.running:
                await self.pull_and_process()
                await asyncio.sleep(poll_interval)
        finally:
            heartbeat_task.cancel()
            await self.close()
    
    async def _heartbeat_loop(self):
        """Background task that sends heartbeats."""
        while self.running:
            await asyncio.sleep(10)
            await self.send_heartbeat()
    
    async def stop(self):
        """Stop the worker."""
        self.running = False
    
    async def close(self):
        """Close the connection."""
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
            self._reader = None
            self._writer = None

