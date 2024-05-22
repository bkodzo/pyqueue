"""Admin CLI client for PyQueue broker management."""

import socket
import json
import sys
import os
import argparse
from typing import Optional, Dict, Any
from .protocol import encode_message, read_message
from .producer import Producer
from .connection_pool import ConnectionPool


class AdminClient:
    """Admin client for broker management operations."""
    
    def __init__(self, host: str = None, port: int = None, api_key: str = None, 
                 jwt_token: str = None, use_pool: bool = True, tls_enabled: bool = False):
        import os as os_module
        self.host = host or os_module.getenv('BROKER_HOST', 'localhost')
        self.port = port or int(os_module.getenv('BROKER_PORT', '5555'))
        self.api_key = api_key or os_module.getenv('PYQUEUE_API_KEY')
        self.jwt_token = jwt_token or os_module.getenv('PYQUEUE_JWT_TOKEN')
        self.tls_enabled = tls_enabled or (os_module.getenv('TLS_ENABLED', 'false').lower() == 'true')
        self.pool = ConnectionPool(self.host, self.port, max_size=5) if use_pool else None
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
            response_cmd, response_payload = read_message(sock)
            if response_cmd == "OK":
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
            sock.settimeout(5)
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
    
    def _send_command(self, command: str, payload: Optional[str] = None) -> Dict[str, Any]:
        """Send an admin command to the broker."""
        sock = None
        try:
            sock = self._get_connection()
            if not sock:
                return {"error": "Failed to connect or authenticate"}
            
            if payload:
                message = encode_message(command, payload)
            else:
                message = encode_message(command, "")
            
            sock.send(message)
            response_cmd, response_payload = read_message(sock)
            
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
        finally:
            if sock:
                if self.pool:
                    self.pool.release_connection(sock)
                else:
                    try:
                        sock.close()
                    except (OSError, socket.error):
                        pass
    
    def list_queues(self) -> Dict[str, Any]:
        """List all queues and their statistics."""
        return self._send_command("ADMIN_LIST_QUEUES")
    
    def get_queue_info(self, queue_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific queue."""
        return self._send_command("ADMIN_QUEUE_INFO", queue_name)
    
    def purge_queue(self, queue_name: str) -> Dict[str, Any]:
        """Purge all tasks from a queue."""
        return self._send_command("ADMIN_PURGE_QUEUE", queue_name)
    
    def delete_queue(self, queue_name: str) -> Dict[str, Any]:
        """Delete a queue (must be empty)."""
        return self._send_command("ADMIN_DELETE_QUEUE", queue_name)
    
    def get_task_info(self, task_id: str) -> Dict[str, Any]:
        """Get information about a specific task."""
        return self._send_command("ADMIN_GET_TASK", task_id)
    
    def list_tasks(self, queue_name: Optional[str] = None, status: Optional[str] = None) -> Dict[str, Any]:
        """List tasks, optionally filtered by queue and status."""
        filters = {}
        if queue_name:
            filters["queue"] = queue_name
        if status:
            filters["status"] = status
        payload = json.dumps(filters) if filters else None
        return self._send_command("ADMIN_LIST_TASKS", payload)
    
    def cancel_task(self, task_id: str) -> Dict[str, Any]:
        """Cancel a task (remove from queue if not processing)."""
        return self._send_command("ADMIN_CANCEL_TASK", task_id)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive broker statistics."""
        return self._send_command("STATS")
    
    def get_workers(self) -> Dict[str, Any]:
        """Get list of active workers."""
        return self._send_command("ADMIN_LIST_WORKERS")
    
    def get_dlq(self) -> Dict[str, Any]:
        """Get dead letter queue tasks."""
        return self._send_command("ADMIN_GET_DLQ")
    
    def replay_messages(self, from_timestamp: Optional[float] = None, 
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
        return self._send_command("ADMIN_REPLAY", json.dumps(params))
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check on broker."""
        return self._send_command("ADMIN_HEALTH")
    
    def set_queue_ttl(self, queue_name: str, ttl_seconds: Optional[int]) -> Dict[str, Any]:
        """Set TTL for a queue."""
        params = {"queue": queue_name, "ttl": ttl_seconds}
        return self._send_command("ADMIN_SET_QUEUE_TTL", json.dumps(params))
    
    def set_queue_limit(self, queue_name: str, max_depth: int) -> Dict[str, Any]:
        """Set maximum depth for a queue."""
        params = {"queue": queue_name, "max_depth": max_depth}
        return self._send_command("ADMIN_SET_QUEUE_LIMIT", json.dumps(params))
    
    def get_queue_metrics(self, queue_name: str) -> Dict[str, Any]:
        """Get metrics for a specific queue."""
        return self._send_command("ADMIN_QUEUE_METRICS", queue_name)


def format_output(data: Dict[str, Any], format_type: str = "pretty") -> str:
    """Format output for display."""
    if format_type == "json":
        return json.dumps(data, indent=2)
    
    if "error" in data:
        return f"Error: {data['error']}"
    
    if "status" in data and data["status"] == "ok":
        return "Success"
    
    return json.dumps(data, indent=2)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="PyQueue Admin CLI")
    parser.add_argument("--host", default=None, help="Broker host (default: localhost or BROKER_HOST env)")
    parser.add_argument("--port", type=int, default=None, help="Broker port (default: 5555 or BROKER_PORT env)")
    parser.add_argument("--api-key", help="API key for authentication (or PYQUEUE_API_KEY env)")
    parser.add_argument("--jwt-token", help="JWT token for authentication (or PYQUEUE_JWT_TOKEN env)")
    parser.add_argument("--format", choices=["pretty", "json"], default="pretty", help="Output format")
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # List queues
    subparsers.add_parser("queues", help="List all queues")
    
    # Queue info
    queue_info = subparsers.add_parser("queue-info", help="Get queue information")
    queue_info.add_argument("queue_name", help="Queue name")
    
    # Purge queue
    purge = subparsers.add_parser("purge", help="Purge all tasks from a queue")
    purge.add_argument("queue_name", help="Queue name")
    
    # Delete queue
    delete = subparsers.add_parser("delete-queue", help="Delete a queue")
    delete.add_argument("queue_name", help="Queue name")
    
    # Task info
    task_info = subparsers.add_parser("task", help="Get task information")
    task_info.add_argument("task_id", help="Task ID")
    
    # List tasks
    list_tasks = subparsers.add_parser("tasks", help="List tasks")
    list_tasks.add_argument("--queue", help="Filter by queue name")
    list_tasks.add_argument("--status", choices=["ready", "processing", "scheduled", "dlq"], help="Filter by status")
    
    # Cancel task
    cancel = subparsers.add_parser("cancel", help="Cancel a task")
    cancel.add_argument("task_id", help="Task ID")
    
    # Stats
    subparsers.add_parser("stats", help="Get broker statistics")
    
    # Workers
    subparsers.add_parser("workers", help="List active workers")
    
    # DLQ
    subparsers.add_parser("dlq", help="Get dead letter queue")
    
    # Replay
    replay = subparsers.add_parser("replay", help="Replay messages from journal")
    replay.add_argument("--from", dest="from_ts", type=float, help="From timestamp")
    replay.add_argument("--to", dest="to_ts", type=float, help="To timestamp")
    replay.add_argument("--task-ids", nargs="+", help="Specific task IDs to replay")
    replay.add_argument("--queue", help="Queue name filter")
    
    # Health
    subparsers.add_parser("health", help="Health check")
    
    # Set queue TTL
    set_ttl = subparsers.add_parser("set-ttl", help="Set TTL for a queue")
    set_ttl.add_argument("queue_name", help="Queue name")
    set_ttl.add_argument("ttl", type=int, nargs="?", help="TTL in seconds (0 to disable)")
    
    # Set queue limit
    set_limit = subparsers.add_parser("set-limit", help="Set maximum depth for a queue")
    set_limit.add_argument("queue_name", help="Queue name")
    set_limit.add_argument("max_depth", type=int, help="Maximum queue depth")
    
    # Queue metrics
    queue_metrics = subparsers.add_parser("queue-metrics", help="Get queue metrics")
    queue_metrics.add_argument("queue_name", help="Queue name")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    client = AdminClient(host=args.host, port=args.port, 
                       api_key=args.api_key, jwt_token=args.jwt_token)
    
    if args.command == "queues":
        result = client.list_queues()
    elif args.command == "queue-info":
        result = client.get_queue_info(args.queue_name)
    elif args.command == "purge":
        result = client.purge_queue(args.queue_name)
    elif args.command == "delete-queue":
        result = client.delete_queue(args.queue_name)
    elif args.command == "task":
        result = client.get_task_info(args.task_id)
    elif args.command == "tasks":
        result = client.list_tasks(queue_name=args.queue, status=args.status)
    elif args.command == "cancel":
        result = client.cancel_task(args.task_id)
    elif args.command == "stats":
        result = client.get_stats()
    elif args.command == "workers":
        result = client.get_workers()
    elif args.command == "dlq":
        result = client.get_dlq()
    elif args.command == "replay":
        result = client.replay_messages(
            from_timestamp=args.from_ts,
            to_timestamp=args.to_ts,
            task_ids=args.task_ids,
            queue_name=args.queue
        )
    elif args.command == "health":
        result = client.health_check()
    elif args.command == "set-ttl":
        result = client.set_queue_ttl(args.queue_name, args.ttl)
    elif args.command == "set-limit":
        result = client.set_queue_limit(args.queue_name, args.max_depth)
    elif args.command == "queue-metrics":
        result = client.get_queue_metrics(args.queue_name)
    else:
        parser.print_help()
        sys.exit(1)
    
    print(format_output(result, args.format))
    
    if "error" in result:
        sys.exit(1)


if __name__ == "__main__":
    main()

