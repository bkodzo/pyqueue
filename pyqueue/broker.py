"""Broker server for PyQueue - manages task queues and worker assignments."""

import asyncio
import socket
import ssl
import time
import uuid
import json
import os
import heapq
from typing import Dict, List, Optional, Tuple, Set
from .protocol import encode_message
from .protocol_async import read_message_async, write_message_async
from .logger import Logger
from .metrics import Metrics
from .scheduler import parse_schedule, next_run_time
from .deduplication import DeduplicationStore
from .ratelimit import RateLimiter
from .config import Config
from .auth import Authenticator
from .ttl import TTLManager
from .backpressure import BackpressureManager
from .ordering import OrderingManager
from .wildcard import WildcardMatcher
from .prometheus_metrics import PrometheusMetrics

try:
    from .cluster import RaftConsensus
except ImportError:
    RaftConsensus = None


# Constants
DEFAULT_TTL_CLEANUP_INTERVAL = 60  # seconds
DEFAULT_METRICS_INTERVAL = 10  # seconds
DEFAULT_REAPER_INTERVAL = 1  # seconds
DEFAULT_SCHEDULER_INTERVAL = 1  # seconds
DEFAULT_HEARTBEAT_CHECK_INTERVAL = 5  # seconds
DEFAULT_WORKER_TIMEOUT = 30  # seconds
DEFAULT_TRANSACTION_CLEANUP_INTERVAL = 10  # seconds
DEFAULT_DEDUPLICATION_TTL = 3600  # seconds
DEFAULT_MAX_TASK_LIST_SIZE = 100
DEFAULT_PAYLOAD_PREVIEW_LENGTH = 50
DEFAULT_QUEUE_INFO_TASK_LIMIT = 10


class Task:
    """Represents a task in the system."""
    
    PRIORITY_HIGH = 0
    PRIORITY_NORMAL = 1
    PRIORITY_LOW = 2
    
    def __init__(self, task_id: str, payload: str, expires_at: Optional[float] = None,
                 scheduled_at: Optional[float] = None, schedule: Optional[dict] = None,
                 idempotency_key: Optional[str] = None, priority: int = PRIORITY_NORMAL,
                 queue_name: str = "default", retry_count: int = 0, ttl: Optional[int] = None,
                 sequence: Optional[int] = None):
        self.task_id = task_id
        self.payload = payload
        self.expires_at = expires_at
        self.scheduled_at = scheduled_at
        self.schedule = schedule
        self.idempotency_key = idempotency_key
        self.priority = priority
        self.queue_name = queue_name
        self.retry_count = retry_count
        self.created_at = time.time()
        self.ttl = ttl
        self.sequence = sequence
    
    def __lt__(self, other):
        """For heapq priority queue ordering."""
        if self.priority != other.priority:
            return self.priority < other.priority
        if self.sequence is not None and other.sequence is not None:
            return self.sequence < other.sequence
        return self.created_at < other.created_at


class Broker:
    """Message broker with visibility timeout for fault tolerance."""
    
    def __init__(self, host='localhost', port=5000, visibility_timeout=10, journal_file=None, config_file=None):
        import os
        self.config = Config(config_file)
        
        if journal_file is None:
            journal_file = self.config.get("broker", "journal_file", os.getenv('JOURNAL_FILE', 'journal.log'))
        
        self.host = host or self.config.get("broker", "host", "localhost")
        self.port = port or self.config.get("broker", "port", 5000)
        self.visibility_timeout = visibility_timeout or self.config.get("broker", "visibility_timeout", 10)
        self.journal_file = journal_file
        
        self.queues: Dict[str, List[Task]] = {"default": []}
        self.processing_queue: Dict[str, Task] = {}
        self.dead_letter_queue: List[Task] = []
        self.scheduled_tasks: List[Task] = []
        self.task_start_times: Dict[str, float] = {}
        self.workers: Dict[str, dict] = {}
        self.max_retries = 3
        
        # Pub/Sub support
        self.topics: Dict[str, List[str]] = {}  # topic -> list of subscriber queue names
        self.subscriptions: Dict[str, str] = {}  # queue_name -> topic
        
        # Transaction support
        self.transactions: Dict[str, Dict] = {}  # transaction_id -> operations
        self.transaction_lock = asyncio.Lock()
        
        # Clustering support
        cluster_nodes_env = os.getenv('CLUSTER_NODES', '')
        cluster_nodes = []
        if cluster_nodes_env:
            for node_str in cluster_nodes_env.split(','):
                parts = node_str.strip().split(':')
                if len(parts) == 2:
                    cluster_nodes.append((parts[0], int(parts[1])))
        
        self.node_id = str(uuid.uuid4())[:8]
        self.raft = None
        if cluster_nodes and RaftConsensus:
            self.raft = RaftConsensus(self.node_id, host, port, cluster_nodes, 
                                     journal_file=self.journal_file,
                                     state_machine_callback=self._apply_raft_entry)
        
        # Locks will be initialized as asyncio.Lock in start()
        self.queue_lock = None
        self.journal_lock = None
        self.transaction_lock = None
        self.connection_lock = None
        self.running = False
        
        self.deduplication = DeduplicationStore(ttl_seconds=DEFAULT_DEDUPLICATION_TTL)
        default_rate = self.config.get("rate_limiting", "default_rate", 10.0)
        default_burst = self.config.get("rate_limiting", "default_burst", 20)
        self.rate_limiter = RateLimiter(default_rate=default_rate, default_burst=default_burst)
        
        self.authenticator = Authenticator(self.config)
        self.ttl_manager = TTLManager(self.config.get("queues", "default_ttl"))
        self.backpressure = BackpressureManager(self.config)
        self.ordering = OrderingManager()
        
        self.max_message_size = self.config.get("broker", "max_message_size", 10 * 1024 * 1024)
        self.transaction_timeout = self.config.get("broker", "transaction_timeout", 60)
        self.max_connections = self.config.get("broker", "max_connections", 1000)
        self.request_timeout = self.config.get("broker", "request_timeout", 30.0)
        
        self.logger = Logger("broker")
        self.audit_logger = Logger("audit")  # Separate logger for admin audit trail
        self.metrics = Metrics()
        
        # Journal batching
        self.journal_batch: List[dict] = []
        self.journal_batch_size = self.config.get("broker", "journal_batch_size", 10)
        self.journal_batch_timeout = self.config.get("broker", "journal_batch_timeout", 1.0)
        self.last_journal_flush = time.time()
        
        prometheus_enabled = self.config.get("monitoring", "prometheus_enabled", True)
        if prometheus_enabled:
            prometheus_port = self.config.get("monitoring", "prometheus_port", 9090)
            self.prometheus_metrics = PrometheusMetrics(prometheus_port)
        else:
            self.prometheus_metrics = None
        
        self.active_connections: Set[asyncio.StreamWriter] = set()
        self.connection_lock = None  # Will be asyncio.Lock after start()
        self.shutdown_event = None  # Will be asyncio.Event after start()
        self.server = None
        self.event_loop = None  # Will be set in start()
        self.background_tasks: Set[asyncio.Task] = set()  # Track all background tasks
        
    async def start(self):
        """Start the broker server and background tasks."""
        # Store event loop for use in sync callbacks
        self.event_loop = asyncio.get_event_loop()
        
        # Initialize asyncio locks
        self.queue_lock = asyncio.Lock()
        self.journal_lock = asyncio.Lock()
        self.transaction_lock = asyncio.Lock()
        self.connection_lock = asyncio.Lock()
        
        self.logger.info("Starting broker", host=self.host, port=self.port, node_id=self.node_id)
        self.start_time = time.time()
        
        is_valid, errors = self.config.validate()
        if not is_valid:
            self.logger.error("Configuration validation failed", errors=errors)
            raise ValueError(f"Invalid configuration: {errors}")
        
        if self.prometheus_metrics:
            self.prometheus_metrics.start()
            self.logger.info("Prometheus metrics enabled", port=self.prometheus_metrics.port)
        
        # Initialize shutdown event before using it
        self.shutdown_event = asyncio.Event()
        
        # Replay journal synchronously (before async context)
        self._replay_journal()
        
        # Start Raft if enabled
        if self.raft:
            await self.raft.start()
        
        self.running = True
        self.shutdown_event.clear()
        
        # Start background tasks
        self.background_tasks.add(asyncio.create_task(self._reaper_loop()))
        self.background_tasks.add(asyncio.create_task(self._metrics_loop()))
        self.background_tasks.add(asyncio.create_task(self._scheduler_loop()))
        self.background_tasks.add(asyncio.create_task(self._heartbeat_loop()))
        self.background_tasks.add(asyncio.create_task(self._ttl_cleanup_loop()))
        self.background_tasks.add(asyncio.create_task(self._transaction_cleanup_loop()))
        
        # Setup TLS context if enabled
        ssl_context = None
        self.tls_enabled = self.config.get("security", "tls_enabled", False)
        self.tls_cert_file = self.config.get("security", "tls_cert_file")
        self.tls_key_file = self.config.get("security", "tls_key_file")
        if self.tls_enabled and self.tls_cert_file and self.tls_key_file:
            ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ssl_context.load_cert_chain(self.tls_cert_file, self.tls_key_file)
            self.logger.info("TLS enabled", cert_file=self.tls_cert_file)
        
        # Start async server
        self.server = await asyncio.start_server(
            self._handle_client_async,
            self.host,
            self.port,
            ssl=ssl_context
        )
        
        self.logger.info("Broker listening", host=self.host, port=self.port, tls_enabled=self.tls_enabled)
        
        async with self.server:
            await self.server.serve_forever()
    
    async def stop(self):
        """Stop the broker server gracefully."""
        self.logger.info("Shutting down broker")
        self.running = False
        self.shutdown_event.set()
        
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        # Stop Raft if enabled
        if self.raft:
            self.raft.stop()
        
        # Cancel all background tasks
        for task in self.background_tasks:
            task.cancel()
        
        await asyncio.gather(*self.background_tasks, return_exceptions=True)
        
        await self._graceful_shutdown()
    
    async def _graceful_shutdown(self):
        """Perform graceful shutdown: drain connections and flush journal."""
        self.logger.info("Starting graceful shutdown")
        
        async with self.connection_lock:
            active_count = len(self.active_connections)
            self.logger.info("Waiting for connections to close", active_connections=active_count)
        
        timeout = 30
        start_time = time.time()
        while time.time() - start_time < timeout:
            async with self.connection_lock:
                if len(self.active_connections) == 0:
                    break
            await asyncio.sleep(0.5)
        
        async with self.connection_lock:
            remaining = len(self.active_connections)
            if remaining > 0:
                self.logger.warn("Force closing remaining connections", count=remaining)
                for writer in list(self.active_connections):
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except (OSError, ConnectionError, RuntimeError) as e:
                        self.logger.debug("Error closing connection during shutdown", error=str(e))
                self.active_connections.clear()
        
        await self._flush_journal()
        self.logger.info("Graceful shutdown complete")
    
    async def _flush_journal(self):
        """Flush journal to disk, including any pending batched entries."""
        try:
            async with self.journal_lock:
                # Flush any pending batch entries first
                if self.journal_batch:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, self._sync_write_journal_batch, self.journal_batch.copy())
                    self.journal_batch.clear()
                    self.last_journal_flush = time.time()
                
                if hasattr(self, 'journal_file') and os.path.exists(self.journal_file):
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, self._sync_flush_journal)
            self.logger.info("Journal flushed to disk")
        except Exception as e:
            self.logger.error("Failed to flush journal", error=str(e))
    
    def _sync_flush_journal(self):
        """Synchronous journal flush (run in executor)."""
        with open(self.journal_file, 'a') as f:
            f.flush()
            os.fsync(f.fileno())
    
    async def _ttl_cleanup_loop(self):
        """Background task that removes expired tasks based on TTL."""
        while self.running:
            await asyncio.sleep(DEFAULT_TTL_CLEANUP_INTERVAL)
            current_time = time.time()
            
            async with self.queue_lock:
                for queue_name, queue in list(self.queues.items()):
                    valid_tasks, expired_tasks = self.ttl_manager.filter_expired(list(queue), current_time)
                    if expired_tasks:
                        self.queues[queue_name] = valid_tasks
                        heapq.heapify(self.queues[queue_name])
                        for task in expired_tasks:
                            self.logger.info("Task expired by TTL", task_id=task.task_id, queue=queue_name)
    
    async def _handle_client_async(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle a client connection (Producer or Worker) asynchronously.
        
        Args:
            reader: Async stream reader
            writer: Async stream writer
        """
        address = writer.get_extra_info('peername')
        principal = None
        
        try:
            # Check connection limit
            async with self.connection_lock:
                if len(self.active_connections) >= self.max_connections:
                    self.logger.warn("Connection limit reached", max=self.max_connections, address=str(address))
                    await self._send_error_async(writer, "Server connection limit reached")
                    writer.close()
                    await writer.wait_closed()
                    return
                self.active_connections.add(writer)
            
            try:
                command, payload = await asyncio.wait_for(
                    read_message_async(reader, max_size=self.max_message_size),
                    timeout=self.request_timeout
                )
            except asyncio.TimeoutError:
                self.logger.warn("Request timeout", address=str(address))
                await self._send_error_async(writer, "Request timeout")
                return
            
            if command is None:
                await self._send_error_async(writer, "Invalid message or message too large")
                return
            
            principal = await self._handle_authentication_async(writer, command, payload)
            if principal is None and self.authenticator.enabled:
                return
            
            if command == "AUTH":
                pass
            elif command == "PUSH":
                if not await self._check_authorization_async(writer, "default", "push"):
                    return
                await self._handle_push_async(writer, reader, payload, address)
            elif command == "PUSH_BATCH":
                if not await self._check_authorization_async(writer, "default", "push"):
                    return
                await self._handle_push_batch_async(writer, reader, payload, address)
            elif command == "SCHEDULE":
                if not await self._check_authorization_async(writer, "default", "push"):
                    return
                await self._handle_schedule_async(writer, reader, payload, address)
            elif command == "PULL":
                queue_name = "default"
                if payload:
                    try:
                        request_data = json.loads(payload.decode('utf-8'))
                        queue_name = request_data.get('queue', 'default')
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        queue_name = 'default'
                if not await self._check_authorization_async(writer, queue_name, "pull"):
                    return
                await self._handle_pull_async(writer, reader, payload)
            elif command == "PULL_BATCH":
                queue_name = "default"
                if payload:
                    try:
                        request_data = json.loads(payload.decode('utf-8'))
                        queue_name = request_data.get('queue', 'default')
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        queue_name = 'default'
                if not await self._check_authorization_async(writer, queue_name, "pull"):
                    return
                await self._handle_pull_batch_async(writer, reader, payload)
            elif command == "ACK":
                await self._handle_ack_async(writer, reader, payload)
            elif command == "NACK":
                await self._handle_nack_async(writer, reader, payload)
            elif command == "HEARTBEAT":
                await self._handle_heartbeat_async(writer, reader, payload)
            elif command == "STATS":
                if not await self._check_authorization_async(writer, "*", "read"):
                    return
                await self._handle_stats_async(writer, reader)
            elif command.startswith("ADMIN_"):
                if not await self._check_authorization_async(writer, "*", "admin"):
                    return
                await self._handle_admin_command_async(writer, reader, command, payload)
            elif command == "SUBSCRIBE":
                if not await self._check_authorization_async(writer, "*", "subscribe"):
                    return
                await self._handle_subscribe_async(writer, reader, payload)
            elif command == "PUBLISH":
                if not await self._check_authorization_async(writer, "*", "publish"):
                    return
                await self._handle_publish_async(writer, reader, payload, address)
            elif command == "UNSUBSCRIBE":
                if not await self._check_authorization_async(writer, "*", "subscribe"):
                    return
                await self._handle_unsubscribe_async(writer, reader, payload)
            elif command == "BEGIN_TX":
                if not await self._check_authorization_async(writer, "default", "push"):
                    return
                await self._handle_begin_transaction_async(writer, reader)
            elif command == "COMMIT_TX":
                await self._handle_commit_transaction_async(writer, reader, payload)
            elif command == "ROLLBACK_TX":
                await self._handle_rollback_transaction_async(writer, reader, payload)
            elif command == "RAFT_VOTE":
                await self._handle_raft_vote_async(writer, reader, payload)
            elif command == "RAFT_HEARTBEAT":
                await self._handle_raft_heartbeat_async(writer, reader, payload)
            elif command == "RAFT_APPEND_ENTRIES":
                await self._handle_raft_append_entries_async(writer, reader, payload)
            else:
                await self._send_error_async(writer, "Invalid command")
                self.logger.warn("Invalid command received", command=command, address=str(address))
        except Exception as e:
            self.logger.error("Error handling client", error=str(e), address=str(address))
            await self._send_error_async(writer, f"Internal error: {str(e)}")
        finally:
            async with self.connection_lock:
                self.active_connections.discard(writer)
            try:
                writer.close()
                await writer.wait_closed()
            except (OSError, ConnectionError, RuntimeError) as e:
                self.logger.debug("Error closing client connection", error=str(e))
    
    async def _handle_authentication_async(self, writer: asyncio.StreamWriter, command: str, payload: Optional[bytes]) -> Optional[str]:
        """Handle authentication for client connection.
        
        Args:
            writer: Client stream writer
            command: Command received
            payload: Command payload
        
        Returns:
            Principal name if authenticated, None if authentication failed
        """
        if not self.authenticator.enabled:
            principal = "anonymous"
            writer.principal = principal
            return principal
        
        if command == "AUTH":
            return await self._process_auth_command_async(writer, payload)
        else:
            principal = getattr(writer, 'principal', None)
            if not principal:
                await self._send_error_async(writer, "Authentication required. Send AUTH command first.")
                return None
            return principal
    
    async def _process_auth_command_async(self, writer: asyncio.StreamWriter, payload: Optional[bytes]) -> Optional[str]:
        """Process AUTH command.
        
        Args:
            writer: Client stream writer
            payload: Auth payload
        
        Returns:
            Principal name if authenticated, None otherwise
        """
        if not payload:
            await self._send_error_async(writer, "AUTH command requires payload")
            return None
        
        try:
            auth_data = json.loads(payload.decode('utf-8'))
            auth_type = auth_data.get('type', '')
            auth_value = auth_data.get('value', '')
            
            if auth_type == 'jwt':
                claims = self.authenticator.authenticate_jwt(auth_value)
                if claims:
                    principal = claims.get("principal")
                    writer.principal = principal
                    await write_message_async(writer, "OK", json.dumps({"authenticated": True, "principal": principal}).encode('utf-8'))
                    return principal
                else:
                    await self._send_error_async(writer, "Authentication failed")
                    return None
            elif auth_type == 'apikey':
                principal = self.authenticator.authenticate_api_key(auth_value)
                if principal:
                    writer.principal = principal
                    await write_message_async(writer, "OK", json.dumps({"authenticated": True, "principal": principal}).encode('utf-8'))
                    return principal
                else:
                    await self._send_error_async(writer, "Authentication failed")
                    return None
            else:
                await self._send_error_async(writer, "Invalid auth type")
                return None
        except (json.JSONDecodeError, UnicodeDecodeError):
            await self._send_error_async(writer, "Invalid auth payload")
            return None
    
    async def _send_error_async(self, writer: asyncio.StreamWriter, message: str):
        """Send error message to client.
        
        Args:
            writer: Client stream writer
            message: Error message
        """
        try:
            await write_message_async(writer, "ERROR", message.encode('utf-8'))
        except Exception:
            pass
    
    async def _handle_push_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: bytes, address: Tuple):
        """Handle PUSH command from Producer."""
        if not self._check_leader():
            await self._send_error_async(writer, "Not the leader. Write operations must go to leader.")
            return
        
        tx_id = getattr(self, '_current_tx_id', None)
        
        try:
            payload_str = payload.decode('utf-8')
            idempotency_key = None
            priority = Task.PRIORITY_NORMAL
            queue_name = "default"
            
            try:
                payload_data = json.loads(payload_str)
                payload_str = payload_data.get('payload', '')
                idempotency_key = payload_data.get('idempotency_key')
                priority_str = payload_data.get('priority', 'NORMAL').upper()
                queue_name = payload_data.get('queue', 'default')
                tx_id = payload_data.get('transaction_id', tx_id)
                
                if priority_str == 'HIGH':
                    priority = Task.PRIORITY_HIGH
                elif priority_str == 'LOW':
                    priority = Task.PRIORITY_LOW
            except (json.JSONDecodeError, AttributeError):
                pass
            
            producer_id = str(address[0])
            current_time = time.time()
            
            if not await self.rate_limiter.check_rate_limit(producer_id, current_time):
                await self._send_error_async(writer, "Rate limit exceeded")
                self.logger.warn("Rate limit exceeded", producer_id=producer_id, address=str(address))
                return
            
            dedup_key = self.deduplication.generate_key(payload_str, idempotency_key)
            if self.deduplication.is_duplicate(dedup_key, current_time):
                await self._send_error_async(writer, "Duplicate task detected")
                self.logger.warn("Duplicate task rejected", dedup_key=dedup_key, address=str(address))
                return
            
            task_id = str(uuid.uuid4())
            self.deduplication.mark_seen(dedup_key, current_time)
            
            if tx_id and await self.add_to_transaction_async(tx_id, {
                "type": "PUSH",
                "task_id": task_id,
                "payload": payload_str,
                "queue": queue_name,
                "priority": priority,
                "idempotency_key": idempotency_key
            }):
                await write_message_async(writer, "OK", task_id.encode('utf-8'))
                self.logger.info("Task added to transaction", task_id=task_id, tx_id=tx_id)
                return
            
            can_accept, reason = self.backpressure.can_accept(queue_name)
            if not can_accept:
                await self._send_error_async(writer, reason or "Queue at capacity")
                self.logger.warn("Backpressure triggered", queue=queue_name, reason=reason)
                return
            
            if self.raft and self.raft.is_leader():
                replicated = await self.raft.replicate_log_entry_async("PUSH", {
                    "timestamp": current_time,
                    "task_id": task_id,
                    "payload": payload_str,
                    "queue": queue_name,
                    "priority": priority
                })
                if not replicated:
                    await self._send_error_async(writer, "Failed to replicate to cluster")
                    self.logger.warn("Log entry not yet committed", task_id=task_id)
                    return
            
            queue_lock = await self._get_queue_lock(queue_name)
            async with queue_lock:
                if queue_name not in self.queues:
                    async with self.queue_lock:
                        if queue_name not in self.queues:
                            self.queues[queue_name] = []
                
                sequence = await self.ordering.get_next_sequence(queue_name)
                queue_ttl = self.ttl_manager.get_queue_ttl(queue_name)
                task = Task(task_id, payload_str, idempotency_key=idempotency_key,
                          priority=priority, queue_name=queue_name, ttl=queue_ttl, sequence=sequence)
                heapq.heappush(self.queues[queue_name], task)
                
                self.backpressure.update_depth(queue_name, len(self.queues[queue_name]))
            
            if not (self.raft and self.raft.is_leader()):
                await self._write_journal_entry_async("PUSH", {
                    "task_id": task_id,
                    "payload": payload_str,
                    "queue": queue_name,
                    "priority": priority
                })
            
            await self.metrics.record_push(task_id)
            if self.prometheus_metrics:
                self.prometheus_metrics.record_push(queue_name)
            
            await write_message_async(writer, "OK", task_id.encode('utf-8'))
            self.logger.info("Task added to queue", task_id=task_id, payload=payload_str, 
                           idempotency_key=idempotency_key, priority=priority, queue=queue_name)
        except Exception as e:
            await self._send_error_async(writer, f"Failed to add task: {str(e)}")
            self.logger.error("Failed to add task", error=str(e))
    
    async def _handle_push_batch_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: bytes, address: Tuple):
        """Handle PUSH_BATCH command from Producer."""
        try:
            payload_data = json.loads(payload.decode('utf-8'))
            tasks = payload_data.get('tasks', [])
            producer_id = str(address[0])
            current_time = time.time()
            
            if not self.rate_limiter.check_rate_limit(producer_id, current_time):
                await self._send_error_async(writer, "Rate limit exceeded")
                return
            
            task_ids = []
            async with self.queue_lock:
                for task_data in tasks:
                    payload_str = task_data.get('payload', '')
                    idempotency_key = task_data.get('idempotency_key')
                    priority_str = task_data.get('priority', 'NORMAL').upper()
                    queue_name = task_data.get('queue', 'default')
                    
                    priority = Task.PRIORITY_NORMAL
                    if priority_str == 'HIGH':
                        priority = Task.PRIORITY_HIGH
                    elif priority_str == 'LOW':
                        priority = Task.PRIORITY_LOW
                    
                    dedup_key = self.deduplication.generate_key(payload_str, idempotency_key)
                    if not self.deduplication.is_duplicate(dedup_key, current_time):
                        task_id = str(uuid.uuid4())
                        self.deduplication.mark_seen(dedup_key, current_time)
                        
                        if queue_name not in self.queues:
                            self.queues[queue_name] = []
                        task = Task(task_id, payload_str, idempotency_key=idempotency_key,
                                  priority=priority, queue_name=queue_name)
                        heapq.heappush(self.queues[queue_name], task)
                        task_ids.append(task_id)
                        await self.metrics.record_push(task_id)
            
            batch_response = json.dumps({"task_ids": task_ids, "count": len(task_ids)})
            await write_message_async(writer, "OK", batch_response.encode('utf-8'))
            self.logger.info("Batch tasks added", count=len(task_ids))
        except Exception as e:
            await self._send_error_async(writer, f"Failed to add batch: {str(e)}")
            self.logger.error("Failed to add batch", error=str(e))
    
    async def _handle_schedule_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: bytes, address: Tuple):
        """Handle SCHEDULE command from Producer."""
        try:
            payload_data = json.loads(payload.decode('utf-8'))
            payload_str = payload_data.get('payload', '')
            schedule_str = payload_data.get('schedule', '')
            idempotency_key = payload_data.get('idempotency_key')
            producer_id = str(address[0])
            
            current_time = time.time()
            
            if not self.rate_limiter.check_rate_limit(producer_id, current_time):
                await self._send_error_async(writer, "Rate limit exceeded")
                return
            
            schedule = parse_schedule(schedule_str)
            if not schedule:
                await self._send_error_async(writer, f"Invalid schedule format: {schedule_str}")
                self.logger.warn("Invalid schedule", schedule=schedule_str)
                return
            
            next_run = next_run_time(schedule, current_time)
            task_id = str(uuid.uuid4())
            
            async with self.queue_lock:
                task = Task(task_id, payload_str, scheduled_at=next_run, schedule=schedule,
                          idempotency_key=idempotency_key)
                self.scheduled_tasks.append(task)
                self.scheduled_tasks.sort(key=lambda t: t.scheduled_at or float('inf'))
            
            # Replicate to cluster if enabled
            if self.raft and self.raft.is_leader():
                await self.raft.replicate_log_entry_async("SCHEDULE", {
                    "timestamp": current_time,
                    "task_id": task_id,
                    "scheduled_at": next_run,
                    "payload": payload_str,
                    "schedule": schedule_str,
                    "idempotency_key": idempotency_key
                })
            else:
                # Single node or follower: write directly to journal
                await self._write_journal_entry_async("SCHEDULE", {
                    "task_id": task_id,
                    "scheduled_at": next_run,
                    "payload": payload_str,
                    "schedule": schedule_str,
                    "idempotency_key": idempotency_key
                })
            
            schedule_response = json.dumps({"task_id": task_id, "next_run": next_run})
            await write_message_async(writer, "OK", schedule_response.encode('utf-8'))
            self.logger.info("Task scheduled", task_id=task_id, schedule=schedule_str, 
                           next_run=next_run, payload=payload_str)
        except json.JSONDecodeError:
            await self._send_error_async(writer, "Invalid JSON payload")
        except Exception as e:
            await self._send_error_async(writer, f"Failed to schedule task: {str(e)}")
            self.logger.error("Failed to schedule task", error=str(e))
    
    async def _handle_pull_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: bytes):
        """Handle PULL command from Worker."""
        queue_name = "default"
        if payload:
            try:
                request_data = json.loads(payload.decode('utf-8'))
                queue_name = request_data.get('queue', 'default')
            except (json.JSONDecodeError, UnicodeDecodeError):
                queue_name = 'default'
        
        queue_lock = await self._get_queue_lock(queue_name)
        async with queue_lock:
            if queue_name not in self.queues or not self.queues[queue_name]:
                await write_message_async(writer, "NO_JOB", b"")
                return
            
            task = heapq.heappop(self.queues[queue_name])
            task.expires_at = time.time() + self.visibility_timeout
            async with self.queue_lock:
                self.processing_queue[task.task_id] = task
                self.task_start_times[task.task_id] = time.time()
        
        job_data = json.dumps({
            "task_id": task.task_id,
            "payload": task.payload,
            "queue": task.queue_name,
            "priority": task.priority
        })
        await write_message_async(writer, "JOB", job_data.encode('utf-8'))
        self.logger.info("Task assigned to worker", task_id=task.task_id, queue=queue_name)
    
    async def _handle_pull_batch_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: bytes):
        """Handle PULL_BATCH command from Worker."""
        queue_name = "default"
        batch_size = 1
        
        if payload:
            try:
                request_data = json.loads(payload.decode('utf-8'))
                queue_name = request_data.get('queue', 'default')
                batch_size = min(request_data.get('count', 1), 10)
            except (json.JSONDecodeError, UnicodeDecodeError):
                queue_name = 'default'
                batch_size = 1
        
        tasks = []
        async with self.queue_lock:
            if queue_name in self.queues:
                for _ in range(batch_size):
                    if not self.queues[queue_name]:
                        break
                    task = heapq.heappop(self.queues[queue_name])
                    task.expires_at = time.time() + self.visibility_timeout
                    self.processing_queue[task.task_id] = task
                    self.task_start_times[task.task_id] = time.time()
                    tasks.append(task)
        
        if not tasks:
            await write_message_async(writer, "NO_JOB", b"")
            return
        
        jobs = [{
            "task_id": t.task_id,
            "payload": t.payload,
            "queue": t.queue_name,
            "priority": t.priority
        } for t in tasks]
        
        batch_data = json.dumps({"jobs": jobs, "count": len(jobs)})
        await write_message_async(writer, "JOB_BATCH", batch_data.encode('utf-8'))
        self.logger.info("Batch tasks assigned", count=len(tasks), queue=queue_name)
    
    async def _handle_ack_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: bytes):
        """Handle ACK command from Worker."""
        try:
            payload_data = {}
            try:
                payload_data = json.loads(payload.decode('utf-8'))
                task_id = payload_data.get('task_id', '')
                tx_id = payload_data.get('transaction_id')
            except (json.JSONDecodeError, UnicodeDecodeError):
                task_id = payload.decode('utf-8').strip()
                tx_id = None
            
            processing_time = 0.0
            async with self.queue_lock:
                if task_id in self.processing_queue:
                    start_time = self.task_start_times.get(task_id, time.time())
                    processing_time = time.time() - start_time
                    task = self.processing_queue[task_id]
                    
                    if tx_id and await self.add_to_transaction_async(tx_id, {
                        "type": "ACK",
                        "task_id": task_id,
                        "processing_time": processing_time
                    }):
                        await write_message_async(writer, "OK", "Task added to transaction".encode('utf-8'))
                        self.logger.info("ACK added to transaction", task_id=task_id, tx_id=tx_id)
                        return
                    
                    self.task_start_times.pop(task_id, None)
                    self.processing_queue.pop(task_id)
                    
                    ack_time = time.time()
                    if self.raft and self.raft.is_leader():
                        await self.raft.replicate_log_entry_async("ACK", {
                            "timestamp": ack_time,
                            "task_id": task_id
                        })
                    else:
                        await self._write_journal_entry_async("ACK", {
                            "task_id": task_id
                        })
                    await self.metrics.record_completion(task_id, processing_time)
                    
                    await write_message_async(writer, "OK", "Task acknowledged".encode('utf-8'))
                    self.logger.info("Task completed", task_id=task_id, processing_time=round(processing_time, 3))
                else:
                    await self._send_error_async(writer, "Task not found")
                    self.logger.warn("ACK for unknown task", task_id=task_id)
        except Exception as e:
            await self._send_error_async(writer, f"Failed to acknowledge: {str(e)}")
            self.logger.error("Failed to acknowledge task", error=str(e))
    
    async def _handle_nack_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: bytes):
        """Handle NACK command from Worker (task failed)."""
        try:
            payload_data = json.loads(payload.decode('utf-8'))
            task_id = payload_data.get('task_id', '')
            error_msg = payload_data.get('error', 'Unknown error')
            
            async with self.queue_lock:
                if task_id in self.processing_queue:
                    task = self.processing_queue.pop(task_id)
                    self.task_start_times.pop(task_id, None)
                    task.retry_count += 1
                    
                    if task.retry_count >= self.max_retries:
                        self.dead_letter_queue.append(task)
                        await self.metrics.record_failure(task_id)
                        self.logger.warn("Task moved to DLQ", task_id=task_id, retry_count=task.retry_count, error=error_msg)
                    else:
                        task.expires_at = None
                        if task.queue_name not in self.queues:
                            self.queues[task.queue_name] = []
                        heapq.heappush(self.queues[task.queue_name], task)
                        self.logger.warn("Task retry scheduled", task_id=task_id, retry_count=task.retry_count)
                    
                    response_msg = f"Task {'moved to DLQ' if task.retry_count >= self.max_retries else 'retry scheduled'}"
                    await write_message_async(writer, "OK", response_msg.encode('utf-8'))
                else:
                    await self._send_error_async(writer, "Task not found")
        except Exception as e:
            await self._send_error_async(writer, f"Failed to handle NACK: {str(e)}")
            self.logger.error("Failed to handle NACK", error=str(e))
    
    async def _handle_heartbeat_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: bytes):
        """Handle HEARTBEAT command from Worker."""
        try:
            payload_data = json.loads(payload.decode('utf-8'))
            worker_id = payload_data.get('worker_id', '')
            current_time = time.time()
            
            async with self.queue_lock:
                self.workers[worker_id] = {
                    "last_heartbeat": current_time,
                    "status": "active"
                }
            
            await write_message_async(writer, "OK", "Heartbeat received".encode('utf-8'))
        except Exception as e:
            await self._send_error_async(writer, f"Failed to handle heartbeat: {str(e)}")
    
    async def _reaper_loop(self):
        """Background task that moves expired tasks back to ready queue."""
        while self.running:
            await asyncio.sleep(DEFAULT_REAPER_INTERVAL)
            current_time = time.time()
            
            async with self.queue_lock:
                expired_tasks = []
                for task_id, task in list(self.processing_queue.items()):
                    if task.expires_at and current_time > task.expires_at:
                        expired_tasks.append(task)
                        del self.processing_queue[task_id]
                        self.task_start_times.pop(task_id, None)
                        await self.metrics.record_expiration(task_id)
                
                for task in expired_tasks:
                    task.expires_at = None
                    if task.queue_name not in self.queues:
                        self.queues[task.queue_name] = []
                    heapq.heappush(self.queues[task.queue_name], task)
                    self.logger.warn("Task expired, moved back to queue", task_id=task.task_id, queue=task.queue_name)
    
    async def _heartbeat_loop(self):
        """Background task that checks for dead workers."""
        while self.running:
            await asyncio.sleep(DEFAULT_HEARTBEAT_CHECK_INTERVAL)
            current_time = time.time()
            
            async with self.queue_lock:
                dead_workers = []
                for worker_id, worker_info in list(self.workers.items()):
                    if current_time - worker_info["last_heartbeat"] > DEFAULT_WORKER_TIMEOUT:
                        dead_workers.append(worker_id)
                        self.logger.warn("Worker marked as dead", worker_id=worker_id)
                
                for worker_id in dead_workers:
                    del self.workers[worker_id]
    
    async def _write_journal_entry_async(self, command: str, data: dict):
        """Write structured journal entry in JSON lines format.
        
        Args:
            command: Command name (PUSH, ACK, SCHEDULE, etc.)
            data: Command data dictionary
        """
        entry = {
            "command": command,
            "timestamp": time.time(),
            "data": data
        }
        try:
            async with self.journal_lock:
                self.journal_batch.append(entry)
                # Flush if batch is full or timeout exceeded
                should_flush = (
                    len(self.journal_batch) >= self.journal_batch_size or
                    time.time() - self.last_journal_flush >= self.journal_batch_timeout
                )
                if should_flush:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, self._sync_write_journal_batch, self.journal_batch.copy())
                    self.journal_batch.clear()
                    self.last_journal_flush = time.time()
        except Exception as e:
            self.logger.error("Failed to write to journal", error=str(e))
    
    def _sync_write_journal_batch(self, entries: List[dict]):
        """Synchronous batch journal write (run in executor)."""
        with open(self.journal_file, 'a') as f:
            for entry in entries:
                f.write(json.dumps(entry) + '\n')
            f.flush()
            os.fsync(f.fileno())
    
    def _replay_journal(self):
        """Replay journal log to rebuild queue state on startup.
        
        Only supports JSON lines format.
        """
        if not os.path.exists(self.journal_file):
            self.logger.info("No journal file found, starting fresh")
            return
        
        if os.path.isdir(self.journal_file):
            self.logger.warn("Journal file is a directory, removing it", journal_file=self.journal_file)
            os.rmdir(self.journal_file)
            return
        
        acked_tasks = set()
        pending_tasks = []
        
        try:
            with open(self.journal_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        entry = json.loads(line)
                        command = entry.get("command")
                        data = entry.get("data", {})
                        
                        if command == 'PUSH':
                            task_id = data.get("task_id", "")
                            payload = data.get("payload", "")
                            queue_name = data.get("queue", "default")
                            if task_id and task_id not in acked_tasks:
                                pending_tasks.append((task_id, payload, queue_name))
                        elif command == 'ACK':
                            task_id = data.get("task_id", "")
                            if task_id:
                                acked_tasks.add(task_id)
                                pending_tasks = [(tid, p, q) for tid, p, q in pending_tasks if tid != task_id]
                        elif command == 'SCHEDULE':
                            task_id = data.get("task_id", "")
                            scheduled_at = data.get("scheduled_at")
                            payload = data.get("payload", "")
                            schedule_str = data.get("schedule")
                            if task_id:
                                from .scheduler import parse_schedule
                                schedule = parse_schedule(schedule_str) if schedule_str else None
                                task = Task(task_id, payload, scheduled_at=scheduled_at, schedule=schedule)
                                self.scheduled_tasks.append(task)
                        elif command == 'PUBLISH':
                            task_id = data.get("task_id", "")
                            payload = data.get("message", "")
                            queue_name = data.get("queue", "default")
                            if task_id and task_id not in acked_tasks:
                                pending_tasks.append((task_id, payload, queue_name))
                    except json.JSONDecodeError as e:
                        self.logger.warn("Invalid journal entry format, skipping", line=line[:100], error=str(e))
                        continue
        except Exception as e:
            self.logger.error("Failed to replay journal", error=str(e))
            return
        
        # Note: _replay_journal is called during __init__ before async context
        # So we use synchronous locks here
        import threading
        temp_lock = threading.Lock()
        with temp_lock:
            for task_data in pending_tasks:
                task_id, payload, queue_name = task_data
                task = Task(task_id, payload, queue_name=queue_name)
                if task.queue_name not in self.queues:
                    self.queues[task.queue_name] = []
                heapq.heappush(self.queues[task.queue_name], task)
            
            if self.scheduled_tasks:
                self.scheduled_tasks.sort(key=lambda t: t.scheduled_at or float('inf'))
        
        if pending_tasks or self.scheduled_tasks:
            self.logger.info("Journal replayed", tasks_restored=len(pending_tasks), scheduled_restored=len(self.scheduled_tasks))
    
    async def _scheduler_loop(self):
        """Background task that processes scheduled tasks."""
        while self.running:
            await asyncio.sleep(DEFAULT_SCHEDULER_INTERVAL)
            current_time = time.time()
            
            async with self.queue_lock:
                ready_tasks = []
                remaining_scheduled = []
                
                for task in self.scheduled_tasks:
                    if task.scheduled_at and current_time >= task.scheduled_at:
                        if task.schedule:
                            next_run = next_run_time(task.schedule, current_time)
                            new_task = Task(str(uuid.uuid4()), task.payload, 
                                          scheduled_at=next_run, schedule=task.schedule,
                                          idempotency_key=task.idempotency_key)
                            remaining_scheduled.append(new_task)
                            schedule_str = None
                            if new_task.schedule:
                                if new_task.schedule.get("type") == "interval":
                                    schedule_str = f"every {new_task.schedule['seconds']} seconds"
                                elif new_task.schedule.get("type") == "daily":
                                    schedule_str = f"at {new_task.schedule['hour']}:{new_task.schedule['minute']:02d}"
                                elif new_task.schedule.get("type") == "cron":
                                    schedule_str = "cron: " + " ".join(new_task.schedule.get("expression", []))
                            
                            if self.raft and self.raft.is_leader():
                                await self.raft.replicate_log_entry("SCHEDULE", {
                                    "timestamp": current_time,
                                    "task_id": new_task.task_id,
                                    "scheduled_at": next_run,
                                    "payload": new_task.payload,
                                    "schedule": schedule_str
                                })
                            else:
                                await self._write_journal_entry_async("SCHEDULE", {
                                    "task_id": new_task.task_id,
                                    "scheduled_at": next_run,
                                    "payload": new_task.payload,
                                    "schedule": schedule_str
                                })
                        
                        ready_tasks.append(task)
                        self.logger.info("Scheduled task ready", task_id=task.task_id, 
                                       scheduled_at=task.scheduled_at)
                    else:
                        remaining_scheduled.append(task)
                
                self.scheduled_tasks = remaining_scheduled
                self.scheduled_tasks.sort(key=lambda t: t.scheduled_at or float('inf'))
                
                for task in ready_tasks:
                    task.scheduled_at = None
                    task.schedule = None
                    if task.queue_name not in self.queues:
                        self.queues[task.queue_name] = []
                    heapq.heappush(self.queues[task.queue_name], task)
                    await self.metrics.record_push(task.task_id)
    
    async def _metrics_loop(self):
        """Background task that reports metrics periodically."""
        while self.running:
            await asyncio.sleep(DEFAULT_METRICS_INTERVAL)
            async with self.queue_lock:
                total_ready = sum(len(q) for q in self.queues.values())
                processing_count = len(self.processing_queue)
                scheduled_count = len(self.scheduled_tasks)
                dlq_count = len(self.dead_letter_queue)
                active_workers = len([w for w in self.workers.values() if w.get("status") == "active"])
            
            stats = await self.metrics.get_stats(total_ready, processing_count)
            stats['scheduled_tasks'] = scheduled_count
            stats['dlq_count'] = dlq_count
            stats['active_workers'] = active_workers
            stats['queue_count'] = len(self.queues)
            self.logger.info("Metrics", **stats)
            
            if self.prometheus_metrics:
                self.prometheus_metrics.update_processing_tasks(processing_count)
                self.prometheus_metrics.update_active_workers(active_workers)
                self.prometheus_metrics.update_throughput(stats.get('throughput', 0.0))
                
                for queue_name, queue in self.queues.items():
                    self.prometheus_metrics.update_queue_depth(queue_name, len(queue))
                
                if self.raft:
                    log_info = self.raft.get_log_info()
                    self.prometheus_metrics.update_raft_metrics(
                        log_info.get('current_term', 0),
                        log_info.get('log_length', 0),
                        log_info.get('commit_index', 0)
                    )
    
    def get_queue_stats(self) -> dict:
        """Get statistics for all queues."""
        with self.queue_lock:
            queue_stats = {}
            for queue_name, queue in self.queues.items():
                queue_stats[queue_name] = {
                    "depth": len(queue),
                    "processing": len([t for t in self.processing_queue.values() if t.queue_name == queue_name])
                }
            return {
                "queues": queue_stats,
                "processing": len(self.processing_queue),
                "scheduled": len(self.scheduled_tasks),
                "dlq": len(self.dead_letter_queue),
                "workers": len(self.workers)
            }
    
    async def _handle_stats_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader):
        """Handle STATS command from API."""
        try:
            stats = await self.get_queue_stats_async()
            async with self.queue_lock:
                total_ready = sum(len(q) for q in self.queues.values())
                processing_count = len(self.processing_queue)
            
            metrics = await self.metrics.get_stats(total_ready, processing_count)
            metrics['scheduled_tasks'] = len(self.scheduled_tasks)
            metrics['dlq_count'] = len(self.dead_letter_queue)
            metrics['active_workers'] = len(self.workers)
            metrics['queue_count'] = len(self.queues)
            
            combined_stats = {**stats, **metrics}
            await write_message_async(writer, "STATS", json.dumps(combined_stats).encode('utf-8'))
        except Exception as e:
            await self._send_error_async(writer, f"Failed to get stats: {str(e)}")
            self.logger.error("Failed to get stats", error=str(e))
    
    
    async def _handle_admin_command_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, command: str, payload: Optional[bytes]):
        """Handle admin commands.
        
        Args:
            writer: Client stream writer
            reader: Client stream reader
            command: Admin command name
            payload: Command payload
        """
        try:
            handler_map = {
                "ADMIN_LIST_QUEUES": self._handle_admin_list_queues_async,
                "ADMIN_QUEUE_INFO": self._handle_admin_queue_info_async,
                "ADMIN_PURGE_QUEUE": self._handle_admin_purge_queue_async,
                "ADMIN_DELETE_QUEUE": self._handle_admin_delete_queue_async,
                "ADMIN_GET_TASK": self._handle_admin_get_task_async,
                "ADMIN_LIST_TASKS": self._handle_admin_list_tasks_async,
                "ADMIN_CANCEL_TASK": self._handle_admin_cancel_task_async,
                "ADMIN_LIST_WORKERS": self._handle_admin_list_workers_async,
                "ADMIN_GET_DLQ": self._handle_admin_get_dlq_async,
                "ADMIN_REPLAY": self._handle_admin_replay_async,
                "ADMIN_HEALTH": self._handle_admin_health_async,
                "ADMIN_SET_QUEUE_TTL": self._handle_admin_set_queue_ttl_async,
                "ADMIN_SET_QUEUE_LIMIT": self._handle_admin_set_queue_limit_async,
                "ADMIN_QUEUE_METRICS": self._handle_admin_queue_metrics_async,
            }
            
            handler = handler_map.get(command)
            if handler:
                try:
                    # Audit log the admin operation
                    self.audit_logger.info("Admin operation", 
                        command=command, 
                        address=str(writer.get_extra_info('peername')),
                        timestamp=time.time())
                    await handler(writer, reader, payload)
                except AttributeError as e:
                    await self._send_error_async(writer, f"Admin command handler not implemented: {command}")
                    self.logger.error("Admin handler missing", command=command, error=str(e))
            else:
                await self._send_error_async(writer, f"Unknown admin command: {command}")
        except Exception as e:
            await self._send_error_async(writer, f"Admin command failed: {str(e)}")
            self.logger.error("Admin command failed", error=str(e), command=command)
    
    async def _handle_admin_list_queues_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: Optional[bytes]):
        """Handle ADMIN_LIST_QUEUES command."""
        async with self.queue_lock:
            queue_info = {}
            for queue_name, queue in self.queues.items():
                queue_info[queue_name] = {
                    "depth": len(queue),
                    "processing": len([t for t in self.processing_queue.values() if t.queue_name == queue_name]),
                    "scheduled": len([t for t in self.scheduled_tasks if t.queue_name == queue_name])
                }
            await write_message_async(writer, "OK", json.dumps({"queues": queue_info}).encode('utf-8'))
    
    async def _handle_admin_queue_info_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: Optional[bytes]):
        """Handle ADMIN_QUEUE_INFO command."""
        queue_name = payload.decode('utf-8') if payload else "default"
        async with self.queue_lock:
            if queue_name not in self.queues:
                await self._send_error_async(writer, f"Queue '{queue_name}' not found")
            else:
                queue = self.queues[queue_name]
                processing = [t for t in self.processing_queue.values() if t.queue_name == queue_name]
                info = {
                    "name": queue_name,
                    "depth": len(queue),
                    "processing": len(processing),
                    "scheduled": len([t for t in self.scheduled_tasks if t.queue_name == queue_name]),
                    "tasks": [{"id": t.task_id, "payload": t.payload, "priority": t.priority} for t in list(queue)[:DEFAULT_QUEUE_INFO_TASK_LIMIT]]
                }
                await write_message_async(writer, "OK", json.dumps(info).encode('utf-8'))
    
    async def _handle_admin_purge_queue_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: Optional[bytes]):
        """Handle ADMIN_PURGE_QUEUE command."""
        queue_name = payload.decode('utf-8') if payload else "default"
        async with self.queue_lock:
            if queue_name in self.queues:
                count = len(self.queues[queue_name])
                self.queues[queue_name] = []
                await write_message_async(writer, "OK", json.dumps({"purged": count, "queue": queue_name}).encode('utf-8'))
            else:
                await self._send_error_async(writer, f"Queue '{queue_name}' not found")
    
    async def _handle_admin_delete_queue_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: Optional[bytes]):
        """Handle ADMIN_DELETE_QUEUE command."""
        queue_name = payload.decode('utf-8') if payload else "default"
        async with self.queue_lock:
            if queue_name == "default":
                await self._send_error_async(writer, "Cannot delete default queue")
            elif queue_name not in self.queues:
                await self._send_error_async(writer, f"Queue '{queue_name}' not found")
            elif len(self.queues[queue_name]) > 0:
                await self._send_error_async(writer, f"Queue '{queue_name}' is not empty")
            else:
                del self.queues[queue_name]
                await write_message_async(writer, "OK", json.dumps({"deleted": queue_name}).encode('utf-8'))
    
    async def _handle_admin_get_task_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: Optional[bytes]):
        """Handle ADMIN_GET_TASK command."""
        task_id = payload.decode('utf-8') if payload else ""
        async with self.queue_lock:
            task = self._find_task(task_id)
            
            if task:
                info = {
                    "task_id": task.task_id,
                    "payload": task.payload,
                    "priority": task.priority,
                    "queue": task.queue_name,
                    "retry_count": task.retry_count,
                    "created_at": task.created_at,
                    "scheduled_at": task.scheduled_at
                }
                await write_message_async(writer, "OK", json.dumps(info).encode('utf-8'))
            else:
                await self._send_error_async(writer, f"Task '{task_id}' not found")
    
    def _find_task(self, task_id: str) -> Optional[Task]:
        """Find a task by ID across all queues and states.
        
        Args:
            task_id: Task ID to find
        
        Returns:
            Task if found, None otherwise
        """
        for queue in self.queues.values():
            for t in queue:
                if t.task_id == task_id:
                    return t
        
        for t in self.processing_queue.values():
            if t.task_id == task_id:
                return t
        
        for t in self.scheduled_tasks:
            if t.task_id == task_id:
                return t
        
        for t in self.dead_letter_queue:
            if t.task_id == task_id:
                return t
        
        return None
    
    async def _handle_admin_list_tasks_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: Optional[bytes]):
        """Handle ADMIN_LIST_TASKS command."""
        filters = {}
        if payload:
            try:
                filters = json.loads(payload.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        
        queue_name = filters.get("queue")
        status = filters.get("status", "all")
        
        async with self.queue_lock:
            tasks = []
            if status in ["all", "ready"]:
                tasks.extend(self._get_ready_tasks(queue_name))
            
            if status in ["all", "processing"]:
                tasks.extend(self._get_processing_tasks(queue_name))
            
            if status in ["all", "scheduled"]:
                tasks.extend(self._get_scheduled_tasks(queue_name))
            
            if status in ["all", "dlq"]:
                tasks.extend(self._get_dlq_tasks(queue_name))
            
            await write_message_async(writer, "OK", json.dumps({"tasks": tasks[:DEFAULT_MAX_TASK_LIST_SIZE], "count": len(tasks)}).encode('utf-8'))
    
    def _get_ready_tasks(self, queue_name: Optional[str]) -> List[dict]:
        """Get ready tasks, optionally filtered by queue."""
        tasks = []
        for qname, queue in self.queues.items():
            if not queue_name or qname == queue_name:
                for task in queue:
                    tasks.append({
                        "task_id": task.task_id,
                        "payload": task.payload[:DEFAULT_PAYLOAD_PREVIEW_LENGTH],
                        "queue": task.queue_name,
                        "priority": task.priority,
                        "status": "ready"
                    })
        return tasks
    
    def _get_processing_tasks(self, queue_name: Optional[str]) -> List[dict]:
        """Get processing tasks, optionally filtered by queue."""
        tasks = []
        for task in self.processing_queue.values():
            if not queue_name or task.queue_name == queue_name:
                tasks.append({
                    "task_id": task.task_id,
                    "payload": task.payload[:DEFAULT_PAYLOAD_PREVIEW_LENGTH],
                    "queue": task.queue_name,
                    "priority": task.priority,
                    "status": "processing"
                })
        return tasks
    
    def _get_scheduled_tasks(self, queue_name: Optional[str]) -> List[dict]:
        """Get scheduled tasks, optionally filtered by queue."""
        tasks = []
        for task in self.scheduled_tasks:
            if not queue_name or task.queue_name == queue_name:
                tasks.append({
                    "task_id": task.task_id,
                    "payload": task.payload[:DEFAULT_PAYLOAD_PREVIEW_LENGTH],
                    "queue": task.queue_name,
                    "priority": task.priority,
                    "status": "scheduled",
                    "scheduled_at": task.scheduled_at
                })
        return tasks
    
    def _get_dlq_tasks(self, queue_name: Optional[str]) -> List[dict]:
        """Get DLQ tasks, optionally filtered by queue."""
        tasks = []
        for task in self.dead_letter_queue:
            if not queue_name or task.queue_name == queue_name:
                tasks.append({
                    "task_id": task.task_id,
                    "payload": task.payload[:DEFAULT_PAYLOAD_PREVIEW_LENGTH],
                    "queue": task.queue_name,
                    "priority": task.priority,
                    "status": "dlq",
                    "retry_count": task.retry_count
                })
        return tasks
    
    async def _handle_admin_cancel_task_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: Optional[bytes]):
        """Handle ADMIN_CANCEL_TASK command."""
        task_id = payload.decode('utf-8') if payload else ""
        async with self.queue_lock:
            removed = False
            for queue in self.queues.values():
                queue[:] = [t for t in queue if t.task_id != task_id or (removed := True) and False]
                if removed:
                    break
            
            if not removed and task_id in self.processing_queue:
                del self.processing_queue[task_id]
                self.task_start_times.pop(task_id, None)
                removed = True
            
            if not removed:
                self.scheduled_tasks = [t for t in self.scheduled_tasks if t.task_id != task_id or (removed := True) and False]
            
            if removed:
                await write_message_async(writer, "OK", json.dumps({"cancelled": task_id}).encode('utf-8'))
            else:
                await self._send_error_async(writer, f"Task '{task_id}' not found or cannot be cancelled")
    
    async def _handle_admin_list_workers_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: Optional[bytes]):
        """Handle ADMIN_LIST_WORKERS command."""
        async with self.queue_lock:
            workers = []
            for worker_id, worker_info in self.workers.items():
                workers.append({
                    "worker_id": worker_id,
                    "last_heartbeat": worker_info.get("last_heartbeat"),
                    "status": worker_info.get("status", "active")
                })
            await write_message_async(writer, "OK", json.dumps({"workers": workers, "count": len(workers)}).encode('utf-8'))
    
    async def _handle_admin_get_dlq_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: Optional[bytes]):
        """Handle ADMIN_GET_DLQ command."""
        async with self.queue_lock:
            dlq_tasks = [{
                "task_id": t.task_id,
                "payload": t.payload,
                "queue": t.queue_name,
                "retry_count": t.retry_count,
                "created_at": t.created_at
            } for t in self.dead_letter_queue]
            await write_message_async(writer, "OK", json.dumps({"tasks": dlq_tasks, "count": len(dlq_tasks)}).encode('utf-8'))
    
    async def _handle_admin_replay_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: Optional[bytes]):
        """Handle ADMIN_REPLAY command."""
        params = {}
        params = {}
        if payload:
            try:
                params = json.loads(payload.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            self._replay_from_journal,
            params.get("from_timestamp"),
            params.get("to_timestamp"),
            params.get("task_ids"),
            params.get("queue_name")
        )
        await write_message_async(writer, "OK", json.dumps(result).encode('utf-8'))
    
    async def _handle_admin_health_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: Optional[bytes]):
        """Handle ADMIN_HEALTH command."""
        async with self.queue_lock:
            total_ready = sum(len(q) for q in self.queues.values())
            processing_count = len(self.processing_queue)
            active_workers = len([w for w in self.workers.values() if w.get("status") == "active"])
        
        health = {
            "status": "healthy",
            "queues": len(self.queues),
            "ready_tasks": total_ready,
            "processing_tasks": processing_count,
            "active_workers": active_workers,
            "uptime_seconds": time.time() - (getattr(self, 'start_time', time.time()))
        }
        await write_message_async(writer, "OK", json.dumps(health).encode('utf-8'))
    
    async def _handle_admin_set_queue_ttl_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: Optional[bytes]):
        """Handle ADMIN_SET_QUEUE_TTL command."""
        params = {}
        params = {}
        if payload:
            try:
                params = json.loads(payload.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        
        queue_name = params.get("queue")
        ttl = params.get("ttl")
        
        if queue_name:
            self.ttl_manager.set_queue_ttl(queue_name, ttl)
            await write_message_async(writer, "OK", json.dumps({"queue": queue_name, "ttl": ttl}).encode('utf-8'))
        else:
            await self._send_error_async(writer, "Queue name required")
    
    async def _handle_admin_set_queue_limit_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: Optional[bytes]):
        """Handle ADMIN_SET_QUEUE_LIMIT command."""
        params = {}
        params = {}
        if payload:
            try:
                params = json.loads(payload.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        
        queue_name = params.get("queue")
        max_depth = params.get("max_depth")
        
        if queue_name and max_depth is not None:
            self.backpressure.set_queue_limit(queue_name, max_depth)
            await write_message_async(writer, "OK", json.dumps({"queue": queue_name, "max_depth": max_depth}).encode('utf-8'))
        else:
            await self._send_error_async(writer, "Queue name and max_depth required")
    
    async def _handle_admin_queue_metrics_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: Optional[bytes]):
        """Handle ADMIN_QUEUE_METRICS command."""
        queue_name = payload.decode('utf-8') if payload else "default"
        async with self.queue_lock:
            queue_depth = len(self.queues.get(queue_name, []))
            processing = len([t for t in self.processing_queue.values() if t.queue_name == queue_name])
            pressure = self.backpressure.get_pressure_level(queue_name)
            ttl = self.ttl_manager.get_queue_ttl(queue_name)
            limit = self.backpressure.get_queue_limit(queue_name)
        
        metrics = {
            "queue": queue_name,
            "depth": queue_depth,
            "processing": processing,
            "backpressure_level": pressure,
            "ttl": ttl,
            "max_depth": limit
        }
        await write_message_async(writer, "OK", json.dumps(metrics).encode('utf-8'))
    
    def _replay_from_journal(self, from_timestamp=None, to_timestamp=None, task_ids=None, queue_name=None):
        """Replay messages from journal with filters (JSON lines format only)."""
        if not os.path.exists(self.journal_file):
            return {"replayed": 0, "error": "Journal file not found"}
        
        replayed = 0
        replayed_tasks = []
        
        try:
            with open(self.journal_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        entry = json.loads(line)
                        command = entry.get("command")
                        timestamp = entry.get("timestamp")
                        data = entry.get("data", {})
                        
                        if from_timestamp and timestamp and timestamp < from_timestamp:
                            continue
                        if to_timestamp and timestamp and timestamp > to_timestamp:
                            continue
                        
                        if command == 'PUSH':
                            task_id = data.get("task_id", "")
                            payload = data.get("payload", "")
                            task_queue_name = data.get("queue", queue_name or "default")
                            
                            if task_ids and task_id not in task_ids:
                                continue
                            
                            if queue_name and task_queue_name != queue_name:
                                continue
                            
                            with self.queue_lock:
                                task = Task(task_id, payload, queue_name=task_queue_name)
                                if task.queue_name not in self.queues:
                                    self.queues[task.queue_name] = []
                                heapq.heappush(self.queues[task.queue_name], task)
                                replayed_tasks.append(task_id)
                                replayed += 1
                        
                        elif command == 'SCHEDULE':
                            task_id = data.get("task_id", "")
                            scheduled_at = data.get("scheduled_at")
                            payload = data.get("payload", "")
                            schedule_str = data.get("schedule")
                            
                            if task_ids and task_id not in task_ids:
                                continue
                            
                            try:
                                if scheduled_at:
                                    scheduled_at = float(scheduled_at)
                                else:
                                    continue
                                
                                from .scheduler import parse_schedule
                                schedule = parse_schedule(schedule_str) if schedule_str else None
                                
                                with self.queue_lock:
                                    task = Task(task_id, payload, scheduled_at=scheduled_at, schedule=schedule)
                                    self.scheduled_tasks.append(task)
                                    self.scheduled_tasks.sort(key=lambda t: t.scheduled_at or float('inf'))
                                    replayed_tasks.append(task_id)
                                    replayed += 1
                            except (ValueError, TypeError) as e:
                                self.logger.warn("Invalid scheduled_at in journal", task_id=task_id, error=str(e))
                        
                        elif command == 'PUBLISH':
                            task_id = data.get("task_id", "")
                            payload = data.get("message", "")
                            task_queue_name = data.get("queue", queue_name or "default")
                            
                            if task_ids and task_id not in task_ids:
                                continue
                            
                            if queue_name and task_queue_name != queue_name:
                                continue
                            
                            with self.queue_lock:
                                task = Task(task_id, payload, queue_name=task_queue_name)
                                if task.queue_name not in self.queues:
                                    self.queues[task.queue_name] = []
                                heapq.heappush(self.queues[task.queue_name], task)
                                replayed_tasks.append(task_id)
                                replayed += 1
                    except json.JSONDecodeError as e:
                        self.logger.warn("Invalid journal entry format, skipping", line=line[:100], error=str(e))
                        continue
        
        except Exception as e:
            self.logger.error("Failed to replay from journal", error=str(e))
            return {"replayed": replayed, "error": str(e)}
        
        return {"replayed": replayed, "task_ids": replayed_tasks}
    
    async def _handle_subscribe_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: bytes):
        """Handle SUBSCRIBE command for pub/sub."""
        try:
            payload_data = json.loads(payload.decode('utf-8'))
            topic = payload_data.get('topic')
            queue_name = payload_data.get('queue', f"sub_{topic}_{uuid.uuid4().hex[:8]}")
            
            if not topic:
                await self._send_error_async(writer, "Topic name required")
                return
            
            if not WildcardMatcher.validate_pattern(topic):
                await self._send_error_async(writer, f"Invalid topic pattern: {topic}")
                return
            
            async with self.queue_lock:
                if topic not in self.topics:
                    self.topics[topic] = []
                if queue_name not in self.topics[topic]:
                    self.topics[topic].append(queue_name)
                self.subscriptions[queue_name] = topic
                if queue_name not in self.queues:
                    self.queues[queue_name] = []
            
            subscribe_response = json.dumps({"topic": topic, "queue": queue_name})
            await write_message_async(writer, "OK", subscribe_response.encode('utf-8'))
            self.logger.info("Subscription created", topic=topic, queue=queue_name)
        except Exception as e:
            await self._send_error_async(writer, f"Failed to subscribe: {str(e)}")
            self.logger.error("Failed to subscribe", error=str(e))
    
    async def _handle_publish_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: bytes, address: Tuple):
        """Handle PUBLISH command for pub/sub."""
        try:
            payload_data = json.loads(payload.decode('utf-8'))
            topic = payload_data.get('topic')
            message = payload_data.get('message', '')
            
            if not topic:
                await self._send_error_async(writer, "Topic name required")
                return
            
            async with self.queue_lock:
                matching_queues = []
                for sub_topic, queues in self.topics.items():
                    if WildcardMatcher.match(sub_topic, topic):
                        matching_queues.extend(queues)
                
                if not matching_queues:
                    await self._send_error_async(writer, f"No subscribers for topic '{topic}'")
                    return
                
                published_count = 0
                task_ids = []
                current_time = time.time()
                
                for queue_name in matching_queues:
                    task_id = str(uuid.uuid4())
                    task = Task(task_id, message, queue_name=queue_name)
                    if queue_name not in self.queues:
                        self.queues[queue_name] = []
                    heapq.heappush(self.queues[queue_name], task)
                    task_ids.append(task_id)
                    published_count += 1
                    await self.metrics.record_push(task_id)
                    
                    # Replicate to cluster if enabled
                    if self.raft and self.raft.is_leader():
                        await self.raft.replicate_log_entry_async("PUBLISH", {
                            "timestamp": current_time,
                            "topic": topic,
                            "task_id": task_id,
                            "queue": queue_name,
                            "message": message
                        })
                    else:
                        # Single node or follower: write directly to journal
                        await self._write_journal_entry_async("PUBLISH", {
                            "topic": topic,
                            "task_id": task_id,
                            "queue": queue_name,
                            "message": message
                        })
                
                publish_response = json.dumps({
                    "topic": topic,
                    "published": published_count,
                    "task_ids": task_ids
                })
                await write_message_async(writer, "OK", publish_response.encode('utf-8'))
                self.logger.info("Message published", topic=topic, subscribers=published_count)
        except Exception as e:
            await self._send_error_async(writer, f"Failed to publish: {str(e)}")
            self.logger.error("Failed to publish", error=str(e))
    
    async def _handle_unsubscribe_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: bytes):
        """Handle UNSUBSCRIBE command for pub/sub."""
        try:
            payload_data = json.loads(payload.decode('utf-8'))
            topic = payload_data.get('topic')
            queue_name = payload_data.get('queue')
            
            async with self.queue_lock:
                if topic and topic in self.topics:
                    if queue_name:
                        if queue_name in self.topics[topic]:
                            self.topics[topic].remove(queue_name)
                            self.subscriptions.pop(queue_name, None)
                            unsubscribe_response = json.dumps({"unsubscribed": queue_name, "topic": topic})
                            await write_message_async(writer, "OK", unsubscribe_response.encode('utf-8'))
                        else:
                            await self._send_error_async(writer, f"Queue '{queue_name}' not subscribed to topic '{topic}'")
                    else:
                        # Unsubscribe all from topic
                        count = len(self.topics[topic])
                        for q in self.topics[topic]:
                            self.subscriptions.pop(q, None)
                        self.topics[topic] = []
                        unsubscribe_response = json.dumps({"unsubscribed": count, "topic": topic})
                        await write_message_async(writer, "OK", unsubscribe_response.encode('utf-8'))
                else:
                    await self._send_error_async(writer, f"Topic '{topic}' not found")
        except Exception as e:
            await self._send_error_async(writer, f"Failed to unsubscribe: {str(e)}")
            self.logger.error("Failed to unsubscribe", error=str(e))
    
    async def _handle_begin_transaction_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader):
        """Handle BEGIN_TX command to start a transaction."""
        try:
            tx_id = str(uuid.uuid4())
            async with self.transaction_lock:
                self.transactions[tx_id] = {
                    "operations": [],
                    "started_at": time.time(),
                    "status": "active"
                }
            tx_response = json.dumps({"transaction_id": tx_id})
            await write_message_async(writer, "OK", tx_response.encode('utf-8'))
            self.logger.info("Transaction started", transaction_id=tx_id)
        except Exception as e:
            await self._send_error_async(writer, f"Failed to begin transaction: {str(e)}")
            self.logger.error("Failed to begin transaction", error=str(e))
    
    async def _transaction_cleanup_loop(self):
        """Background task that cleans up expired transactions."""
        while self.running:
            await asyncio.sleep(DEFAULT_TRANSACTION_CLEANUP_INTERVAL)
            current_time = time.time()
            
            async with self.transaction_lock:
                expired_txs = []
                for tx_id, tx in list(self.transactions.items()):
                    if tx["status"] == "active":
                        age = current_time - tx["started_at"]
                        if age > self.transaction_timeout:
                            expired_txs.append(tx_id)
                            tx["status"] = "expired"
                            self.logger.warn("Transaction expired", transaction_id=tx_id, age=age)
                
                for tx_id in expired_txs:
                    self.transactions.pop(tx_id, None)
    
    async def _handle_commit_transaction_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: bytes):
        """Handle COMMIT_TX command to commit a transaction."""
        try:
            payload_data = json.loads(payload.decode('utf-8'))
            tx_id = payload_data.get('transaction_id')
            
            if not tx_id or tx_id not in self.transactions:
                await self._send_error_async(writer, "Invalid transaction ID")
                return
            
            tx = self.transactions[tx_id]
            if tx["status"] != "active":
                await self._send_error_async(writer, f"Transaction {tx_id} is not active (status: {tx['status']})")
                return
            
            current_time = time.time()
            age = current_time - tx["started_at"]
            if age > self.transaction_timeout:
                tx["status"] = "expired"
                await self._send_error_async(writer, f"Transaction {tx_id} has expired")
                async with self.transaction_lock:
                    self.transactions.pop(tx_id, None)
                return
            
            # Execute all operations atomically
            async with self.queue_lock:
                async with self.transaction_lock:
                    try:
                        commit_time = time.time()
                        for op in tx["operations"]:
                            op_type = op.get("type")
                            if op_type == "PUSH":
                                task_id = op.get("task_id")
                                payload_str = op.get("payload")
                                queue_name = op.get("queue", "default")
                                priority = op.get("priority", Task.PRIORITY_NORMAL)
                                
                                if queue_name not in self.queues:
                                    self.queues[queue_name] = []
                                task = Task(task_id, payload_str, priority=priority, queue_name=queue_name)
                                heapq.heappush(self.queues[queue_name], task)
                                await self.metrics.record_push(task_id)
                                
                                if self.raft and self.raft.is_leader():
                                    await self.raft.replicate_log_entry_async("PUSH", {
                                        "timestamp": commit_time,
                                        "task_id": task_id,
                                        "payload": payload_str,
                                        "queue": queue_name,
                                        "priority": priority
                                    })
                                else:
                                    await self._write_journal_entry_async("PUSH", {
                                        "task_id": task_id,
                                        "payload": payload_str,
                                        "queue": queue_name,
                                        "priority": priority
                                    })
                            
                            elif op_type == "ACK":
                                task_id = op.get("task_id")
                                if task_id in self.processing_queue:
                                    task = self.processing_queue.pop(task_id)
                                    self.task_start_times.pop(task_id, None)
                                    ack_time = commit_time
                                    processing_time = op.get("processing_time", 0.0)
                                    await self.metrics.record_completion(task_id, processing_time)
                                    
                                    if self.raft and self.raft.is_leader():
                                        await self.raft.replicate_log_entry_async("ACK", {
                                            "timestamp": ack_time,
                                            "task_id": task_id
                                        })
                                    else:
                                        await self._write_journal_entry_async("ACK", {
                                            "task_id": task_id
                                        })
                        
                        tx["status"] = "committed"
                        commit_response = json.dumps({
                            "transaction_id": tx_id,
                            "operations": len(tx["operations"])
                        })
                        await write_message_async(writer, "OK", commit_response.encode('utf-8'))
                    except Exception as e:
                        tx["status"] = "failed"
                        await self._send_error_async(writer, f"Transaction commit failed: {str(e)}")
            
            # Clean up transaction after a delay
            async def cleanup():
                await asyncio.sleep(60)
                async with self.transaction_lock:
                    self.transactions.pop(tx_id, None)
            cleanup_task = asyncio.create_task(cleanup())
            self.background_tasks.add(cleanup_task)
            
            self.logger.info("Transaction committed", transaction_id=tx_id, operations=len(tx["operations"]))
        except Exception as e:
            await self._send_error_async(writer, f"Failed to commit transaction: {str(e)}")
            self.logger.error("Failed to commit transaction", error=str(e))
    
    async def _handle_rollback_transaction_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: bytes):
        """Handle ROLLBACK_TX command to rollback a transaction."""
        try:
            payload_data = json.loads(payload.decode('utf-8'))
            tx_id = payload_data.get('transaction_id')
            
            if not tx_id or tx_id not in self.transactions:
                await self._send_error_async(writer, "Invalid transaction ID")
                return
            
            async with self.transaction_lock:
                tx = self.transactions[tx_id]
                tx["status"] = "rolled_back"
                rollback_response = json.dumps({"transaction_id": tx_id, "status": "rolled_back"})
                await write_message_async(writer, "OK", rollback_response.encode('utf-8'))
            
            # Clean up
            async def cleanup():
                await asyncio.sleep(60)
                async with self.transaction_lock:
                    self.transactions.pop(tx_id, None)
            cleanup_task = asyncio.create_task(cleanup())
            self.background_tasks.add(cleanup_task)
            
            self.logger.info("Transaction rolled back", transaction_id=tx_id)
        except Exception as e:
            await self._send_error_async(writer, f"Failed to rollback transaction: {str(e)}")
            self.logger.error("Failed to rollback transaction", error=str(e))
    
    async def add_to_transaction_async(self, tx_id: str, operation: dict):
        """Add an operation to a transaction (called by PUSH/ACK handlers when in transaction mode)."""
        async with self.transaction_lock:
            if tx_id in self.transactions and self.transactions[tx_id]["status"] == "active":
                self.transactions[tx_id]["operations"].append(operation)
                return True
        return False
    
    
    async def _handle_raft_vote_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: bytes):
        """Handle RAFT_VOTE command for cluster consensus (async)."""
        if not self.raft:
            await self._send_error_async(writer, "Clustering not enabled")
            return
        
        try:
            vote_request = json.loads(payload.decode('utf-8'))
            term = vote_request.get('term')
            candidate_id = vote_request.get('candidate_id')
            
            vote_granted = self.raft.handle_vote_request(term, candidate_id)
            
            response_data = {"vote_granted": vote_granted, "term": self.raft.current_term}
            await write_message_async(writer, "OK", json.dumps(response_data).encode('utf-8'))
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            await self._send_error_async(writer, f"Invalid vote request: {str(e)}")
            self.logger.error("Failed to handle vote", error=str(e))
        except Exception as e:
            await self._send_error_async(writer, f"Failed to handle vote: {str(e)}")
            self.logger.error("Failed to handle vote", error=str(e))
    
    async def _handle_raft_heartbeat_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: bytes):
        """Handle RAFT_HEARTBEAT command for cluster consensus (async)."""
        if not self.raft:
            await self._send_error_async(writer, "Clustering not enabled")
            return
        
        try:
            heartbeat = json.loads(payload.decode('utf-8'))
            term = heartbeat.get('term')
            leader_id = heartbeat.get('leader_id')
            
            self.raft.handle_heartbeat(term, leader_id)
            
            response_data = {"term": self.raft.current_term}
            await write_message_async(writer, "OK", json.dumps(response_data).encode('utf-8'))
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            await self._send_error_async(writer, f"Invalid heartbeat request: {str(e)}")
            self.logger.error("Failed to handle heartbeat", error=str(e))
        except Exception as e:
            await self._send_error_async(writer, f"Failed to handle heartbeat: {str(e)}")
            self.logger.error("Failed to handle heartbeat", error=str(e))
    
    async def _handle_raft_append_entries_async(self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader, payload: bytes):
        """Handle RAFT_APPEND_ENTRIES command for log replication."""
        if not self.raft:
            await self._send_error_async(writer, "Clustering not enabled")
            return
        
        try:
            append_entries = json.loads(payload.decode('utf-8'))
            term = append_entries.get('term')
            leader_id = append_entries.get('leader_id')
            prev_log_index = append_entries.get('prev_log_index', 0)
            prev_log_term = append_entries.get('prev_log_term', 0)
            entries = append_entries.get('entries', [])
            leader_commit = append_entries.get('leader_commit', 0)
            
            success, response_term = await self.raft.handle_append_entries(
                term, leader_id, prev_log_index, prev_log_term, entries, leader_commit
            )
            
            response_data = {"success": success, "term": response_term}
            await write_message_async(writer, "OK", json.dumps(response_data).encode('utf-8'))
        except Exception as e:
            await self._send_error_async(writer, f"Failed to handle AppendEntries: {str(e)}")
            self.logger.error("Failed to handle AppendEntries", error=str(e))
    
    
    def _check_leader(self) -> bool:
        """Check if this broker is the leader (for write operations)."""
        if not self.raft:
            return True
        return self.raft.is_leader()
    
    async def _check_authorization_async(self, writer: asyncio.StreamWriter, queue: str, operation: str) -> bool:
        """Check if client is authorized for operation on queue."""
        if not self.authenticator.enabled:
            return True
        
        principal = getattr(writer, 'principal', None)
        if not principal:
            await self._send_error_async(writer, "Not authenticated")
            return False
        
        if not self.authenticator.authorize(principal, queue, operation):
            await self._send_error_async(writer, f"Unauthorized: {principal} cannot {operation} on {queue}")
            self.logger.warn("Authorization denied", principal=principal, queue=queue, operation=operation)
            return False
        
        return True
    
    def _apply_raft_entry(self, command: str, data: dict):
        """Apply a Raft log entry to broker state machine (called from sync Raft thread)."""
        if command == "PUSH":
            task_id = data.get("task_id")
            payload = data.get("payload", "")
            queue_name = data.get("queue", "default")
            priority = data.get("priority", Task.PRIORITY_NORMAL)
            
            if queue_name not in self.queues:
                self.queues[queue_name] = []
            task = Task(task_id, payload, priority=priority, queue_name=queue_name)
            heapq.heappush(self.queues[queue_name], task)
            if self.event_loop and self.event_loop.is_running():
                asyncio.run_coroutine_threadsafe(self.metrics.record_push(task_id), self.event_loop)
            self.logger.debug("Applied PUSH entry", task_id=task_id, queue=queue_name)
        
        elif command == "ACK":
            task_id = data.get("task_id")
            if task_id in self.processing_queue:
                task = self.processing_queue.pop(task_id)
                start_time = self.task_start_times.pop(task_id, time.time())
                processing_time = time.time() - start_time
                if self.event_loop and self.event_loop.is_running():
                    asyncio.run_coroutine_threadsafe(self.metrics.record_completion(task_id, processing_time), self.event_loop)
                self.logger.debug("Applied ACK entry", task_id=task_id)
        
        elif command == "SCHEDULE":
            task_id = data.get("task_id")
            payload = data.get("payload", "")
            scheduled_at = data.get("scheduled_at")
            schedule_str = data.get("schedule")
            
            if schedule_str:
                from .scheduler import parse_schedule
                schedule = parse_schedule(schedule_str)
            else:
                schedule = None
            
            if scheduled_at:
                task = Task(task_id, payload, scheduled_at=scheduled_at, schedule=schedule)
                self.scheduled_tasks.append(task)
                self.scheduled_tasks.sort(key=lambda t: t.scheduled_at or float('inf'))
                self.logger.debug("Applied SCHEDULE entry", task_id=task_id, scheduled_at=scheduled_at)
        
        elif command == "PUBLISH":
            topic = data.get("topic")
            task_id = data.get("task_id")
            queue_name = data.get("queue")
            message = data.get("message", "")
            
            if topic and queue_name:
                if topic not in self.topics:
                    self.topics[topic] = []
                if queue_name not in self.topics[topic]:
                    self.topics[topic].append(queue_name)
                self.subscriptions[queue_name] = topic
                
                if queue_name not in self.queues:
                    self.queues[queue_name] = []
                task = Task(task_id, message, queue_name=queue_name)
                heapq.heappush(self.queues[queue_name], task)
                if self.event_loop and self.event_loop.is_running():
                    asyncio.run_coroutine_threadsafe(self.metrics.record_push(task_id), self.event_loop)
                self.logger.debug("Applied PUBLISH entry", topic=topic, task_id=task_id, queue=queue_name)


async def main_async():
    """Run the broker server asynchronously."""
    import os
    host = os.getenv('BROKER_HOST', '0.0.0.0')
    port = int(os.getenv('BROKER_PORT', '5555'))
    broker = Broker(host=host, port=port, visibility_timeout=10)
    try:
        await broker.start()
    except KeyboardInterrupt:
        print("\nShutting down broker...")
        await broker.stop()

def main():
    """Run the broker server."""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()


