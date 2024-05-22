"""Worker client for PyQueue - processes tasks from the broker."""

import socket
import time
import json
import threading
from .protocol import encode_message, read_message
from .connection_pool import ConnectionPool


class Worker:
    """Client that pulls and processes tasks from the broker."""
    
    def __init__(self, host=None, port=None, work_duration=2, worker_id=None, queue_name="default", use_pool=True, tls_enabled=False):
        import os
        import uuid
        self.host = host or os.getenv('BROKER_HOST', 'localhost')
        self.port = port or int(os.getenv('BROKER_PORT', '5555'))
        self.work_duration = work_duration
        self.worker_id = worker_id or str(uuid.uuid4())
        self.queue_name = queue_name
        self.running = False
        self.heartbeat_thread = None
        self.use_pool = use_pool
        self.tls_enabled = tls_enabled or (os.getenv('TLS_ENABLED', 'false').lower() == 'true')
        self.pool = None
        if use_pool:
            self.pool = ConnectionPool(self.host, self.port, max_connections=5)
    
    def _get_connection(self):
        """Get a connection with optional TLS."""
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
        
        return sock
    
    def process_task(self, task_id: str, payload: str) -> bool:
        """Simulate processing a task.
        
        Returns: True if successful, False otherwise
        """
        print(f"Processing task {task_id}: {payload}")
        time.sleep(self.work_duration)
        print(f"Task {task_id} completed")
        return True
    
    def pull_and_process(self) -> bool:
        """Pull a task from broker and process it.
        
        Returns: True if task was processed, False if no task available
        """
        sock = None
        try:
            sock = self._get_connection()
            if not sock:
                return False
            
            pull_data = json.dumps({"queue": self.queue_name})
            message = encode_message("PULL", pull_data)
            sock.send(message)
            
            command, response = read_message(sock)
            
            if command == "NO_JOB":
                if sock and self.pool:
                    self.pool.return_connection(sock)
                elif sock:
                    sock.close()
                return False
            
            if command != "JOB":
                print(f"Unexpected response: {command}")
                if sock and self.pool:
                    self.pool.return_connection(sock)
                elif sock:
                    sock.close()
                return False
            
            job_data = json.loads(response.decode('utf-8'))
            task_id = job_data["task_id"]
            payload = job_data["payload"]
            
            if sock and self.pool:
                self.pool.return_connection(sock)
                sock = None
            elif sock:
                sock.close()
                sock = None
            
            success = self.process_task(task_id, payload)
            
            if success:
                if self.pool:
                    sock = self.pool.get_connection()
                else:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect((self.host, self.port))
                
                ack_message = encode_message("ACK", task_id)
                sock.send(ack_message)
                
                ack_response, _ = read_message(sock)
                if ack_response == "OK":
                    if sock and self.pool:
                        self.pool.return_connection(sock)
                    elif sock:
                        sock.close()
                    return True
            
            return False
            
        except Exception as e:
            print(f"Error processing task: {e}")
            return False
        finally:
            if sock:
                if self.pool:
                    self.pool.return_connection(sock)
                else:
                    sock.close()
    
    def send_heartbeat(self):
        """Send heartbeat to broker."""
        sock = None
        try:
            sock = self._get_connection()
            if not sock:
                return
            
            heartbeat_data = json.dumps({"worker_id": self.worker_id})
            message = encode_message("HEARTBEAT", heartbeat_data)
            sock.send(message)
            read_message(sock)
        except (OSError, socket.error, ConnectionError) as e:
            pass
        finally:
            if sock:
                if self.pool:
                    self.pool.return_connection(sock)
                else:
                    sock.close()
    
    def _heartbeat_loop(self):
        """Background thread for sending heartbeats."""
        while self.running:
            time.sleep(10)
            self.send_heartbeat()
    
    def run(self):
        """Run worker in continuous loop."""
        self.running = True
        print(f"Worker {self.worker_id} started, polling for tasks from queue '{self.queue_name}'...")
        
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()
        
        while self.running:
            self.pull_and_process()
            time.sleep(0.5)
    
    def stop(self):
        """Stop the worker."""
        self.running = False


def main():
    """Run a worker."""
    import sys
    
    work_duration = 2
    if len(sys.argv) > 1:
        try:
            work_duration = float(sys.argv[1])
        except ValueError:
            pass
    
    worker = Worker(work_duration=work_duration)
    try:
        worker.run()
    except KeyboardInterrupt:
        print("\nShutting down worker...")
        worker.stop()


if __name__ == "__main__":
    main()

