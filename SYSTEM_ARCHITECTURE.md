# PyQueue: A Comprehensive System Architecture Guide

## Table of Contents
1. [System Overview](#system-overview)
2. [Core Architecture](#core-architecture)
3. [Protocol Design](#protocol-design)
4. [Fault Tolerance Mechanisms](#fault-tolerance-mechanisms)
5. [Advanced Features](#advanced-features)
6. [Clustering and Consensus](#clustering-and-consensus)
7. [Security Architecture](#security-architecture)
8. [Performance Optimizations](#performance-optimizations)
9. [Data Flow and Lifecycle](#data-flow-and-lifecycle)
10. [Component Interactions](#component-interactions)

---

## System Overview

PyQueue is a distributed message broker system designed to decouple the creation of work from the execution of work. At its core, it implements a producer-consumer pattern where producers submit tasks to a central broker, and workers pull and process those tasks asynchronously. The system guarantees at-least-once delivery semantics, meaning every task will be delivered at least once, even in the face of failures.

The fundamental problem PyQueue solves is the coordination of asynchronous work in distributed systems. Traditional synchronous request-response patterns create tight coupling between components. If a service needs to process a task but the processing service is unavailable, the entire operation fails. PyQueue introduces an intermediary broker that acts as a buffer, allowing producers to submit work without waiting for immediate processing, and enabling workers to process tasks at their own pace.

### Design Philosophy

The system is built on several key principles:

1. **Decoupling**: Producers and workers operate independently, communicating only through the broker
2. **Fault Tolerance**: Tasks are never lost, even if workers crash mid-processing
3. **Scalability**: Multiple workers can process tasks in parallel, and the system can be clustered for high availability
4. **Observability**: Comprehensive logging, metrics, and monitoring capabilities
5. **Flexibility**: Support for priority queues, scheduling, pub/sub patterns, and transactions

---

## Core Architecture

### The Three-Tier Model

PyQueue follows a classic three-tier architecture:

#### 1. Producer (Client)
The producer is responsible for creating and submitting tasks to the broker. It connects to the broker over TCP, encodes tasks using the custom protocol, and receives acknowledgment of task submission. Producers are stateless and can be scaled horizontally. They don't need to know about workers or processing logic - their only concern is task submission.

When a producer submits a task, it includes:
- **Payload**: The actual data to be processed
- **Priority**: HIGH, NORMAL, or LOW (affects processing order)
- **Queue Name**: Which logical queue the task belongs to
- **Idempotency Key**: Optional key to prevent duplicate processing
- **Transaction ID**: Optional, for grouping operations atomically

#### 2. Broker (Server)
The broker is the central nervous system of PyQueue. It maintains several critical data structures:

**Ready Queues**: Priority queues (implemented using Python's `heapq`) organized by queue name. Tasks wait here until a worker pulls them. The priority queue ensures HIGH priority tasks are processed before NORMAL, and NORMAL before LOW. Within the same priority level, tasks are ordered by sequence number for FIFO guarantees.

**Processing Queue**: A dictionary mapping task IDs to tasks currently being processed by workers. When a worker pulls a task, it moves from the ready queue to the processing queue and gets a visibility timeout timestamp.

**Scheduled Tasks**: A sorted list of tasks that should execute at a future time. The scheduler thread periodically checks this list and moves due tasks to ready queues.

**Dead Letter Queue (DLQ)**: Tasks that have exceeded their maximum retry count are moved here for manual inspection and potential replay.

**Worker Registry**: Tracks active workers and their last heartbeat timestamp to detect dead workers.

The broker operates as a multi-threaded server. The main thread accepts TCP connections and spawns worker threads for each client. Background threads handle:
- **Reaper Thread**: Monitors the processing queue and moves expired tasks back to ready queues
- **Scheduler Thread**: Processes scheduled tasks
- **Heartbeat Thread**: Detects and removes dead workers
- **Metrics Thread**: Periodically logs system statistics
- **TTL Cleanup Thread**: Removes expired tasks based on time-to-live settings

#### 3. Worker (Consumer)
Workers are the execution engines of the system. They continuously poll the broker for tasks, process them, and send acknowledgment (ACK) or negative acknowledgment (NACK) back to the broker. Workers are designed to be stateless and horizontally scalable - you can run as many workers as needed to handle the workload.

A worker's lifecycle:
1. Connect to broker
2. Send periodic heartbeats to indicate liveness
3. Pull tasks from a specified queue
4. Process the task (application-specific logic)
5. Send ACK if successful, or NACK if failed
6. Repeat

Workers can specify which queue to pull from, enabling workload partitioning. For example, you might have dedicated workers for high-priority tasks and separate workers for batch processing.

---

## Protocol Design

### Custom Application-Layer Protocol

PyQueue implements a custom binary protocol over TCP. This design choice provides several advantages over HTTP/REST:
- Lower overhead (no HTTP headers)
- Persistent connections (connection pooling)
- Binary encoding (more efficient than JSON for large payloads)
- Full control over message framing

### Message Format

Every message follows a strict format:
```
<COMMAND> <LENGTH> <PAYLOAD>
```

- **COMMAND**: Fixed-width string (e.g., "PUSH", "PULL", "ACK")
- **LENGTH**: 4-byte integer (network byte order) indicating payload size
- **PAYLOAD**: Variable-length binary data (typically JSON-encoded)

### Protocol Commands

**Producer Commands:**
- `PUSH`: Submit a single task
- `PUSH_BATCH`: Submit multiple tasks atomically
- `SCHEDULE`: Schedule a task for future execution
- `PUBLISH`: Publish a message to a topic (pub/sub)
- `SUBSCRIBE`: Subscribe to a topic
- `UNSUBSCRIBE`: Unsubscribe from a topic
- `BEGIN_TX`: Start a transaction
- `COMMIT_TX`: Commit a transaction
- `ROLLBACK_TX`: Rollback a transaction

**Worker Commands:**
- `PULL`: Request a task from a queue
- `PULL_BATCH`: Request multiple tasks
- `ACK`: Acknowledge successful task completion
- `NACK`: Indicate task failure
- `HEARTBEAT`: Signal worker is alive

**Admin Commands:**
- `ADMIN_LIST_QUEUES`: List all queues
- `ADMIN_QUEUE_INFO`: Get queue details
- `ADMIN_PURGE_QUEUE`: Remove all tasks from a queue
- `ADMIN_DELETE_QUEUE`: Delete an empty queue
- `ADMIN_GET_TASK`: Get task details
- `ADMIN_LIST_TASKS`: List tasks with filters
- `ADMIN_CANCEL_TASK`: Cancel a task
- `ADMIN_LIST_WORKERS`: List active workers
- `ADMIN_GET_DLQ`: Get dead letter queue contents
- `ADMIN_REPLAY`: Replay messages from journal
- `ADMIN_HEALTH`: Health check
- `ADMIN_SET_QUEUE_TTL`: Set queue TTL
- `ADMIN_SET_QUEUE_LIMIT`: Set queue depth limit
- `ADMIN_QUEUE_METRICS`: Get queue metrics

**Response Codes:**
- `OK`: Operation successful
- `ERROR`: Operation failed
- `JOB`: Task assigned to worker
- `NO_JOB`: No tasks available
- `JOB_BATCH`: Multiple tasks assigned

### Protocol Implementation Details

The protocol implementation handles several critical challenges:

**Partial Reads**: TCP is a stream protocol, meaning a single `recv()` call might not receive a complete message. The `read_message()` function buffers incoming data until a complete message (command + length + full payload) is received.

**Message Framing**: The length prefix allows the receiver to know exactly how many bytes to read for the payload, preventing message boundary issues.

**Error Handling**: Network errors, timeouts, and malformed messages are handled gracefully, with appropriate error responses sent back to clients.

---

## Fault Tolerance Mechanisms

### At-Least-Once Delivery Guarantee

The core guarantee of PyQueue is that every task will be delivered at least once. This is achieved through the visibility timeout mechanism.

#### Visibility Timeout

When a worker pulls a task, the broker:
1. Moves the task from the ready queue to the processing queue
2. Sets an `expires_at` timestamp (current time + visibility timeout, default 10 seconds)
3. Returns the task to the worker

If the worker crashes or becomes unresponsive:
- The task remains in the processing queue
- The reaper thread continuously monitors tasks in the processing queue
- When `expires_at` is reached, the task is automatically moved back to the ready queue
- Another worker can then pull and process the task

This mechanism ensures tasks are never lost, but it can lead to duplicate processing if:
- A worker processes a task successfully but crashes before sending ACK
- The visibility timeout expires
- Another worker picks up the same task

Applications must be designed to handle idempotency - processing the same task multiple times should produce the same result.

#### Journal-Based Persistence

To survive broker crashes, all state-changing operations are written to an append-only journal file:

```
PUSH <timestamp> <task_id> <payload>
ACK <timestamp> <task_id>
SCHEDULE <timestamp> <task_id> <scheduled_at> <payload>
PUBLISH <timestamp> <topic> <task_id> <queue> <message>
```

On broker startup, the journal is replayed:
1. All PUSH entries are restored to ready queues
2. ACK entries are used to filter out already-completed tasks
3. SCHEDULE entries are restored to the scheduled tasks list

This ensures that even if the broker crashes, tasks are not lost. The journal is written synchronously (with flushing) to ensure durability.

#### Dead Letter Queue (DLQ)

Tasks that fail repeatedly are moved to the DLQ:
- Each task has a `retry_count` field
- When a worker sends NACK, the retry count is incremented
- If `retry_count >= max_retries` (default 3), the task moves to DLQ
- Otherwise, the task is requeued for retry

The DLQ serves as a safety net for problematic tasks that need manual inspection or different handling.

---

## Advanced Features

### Priority Queues

Tasks are organized by priority using Python's `heapq` module, which implements a binary heap. The priority values are:
- `PRIORITY_HIGH = 0` (highest)
- `PRIORITY_NORMAL = 1`
- `PRIORITY_LOW = 2` (lowest)

When multiple tasks exist in a queue, higher priority tasks are always processed first. Within the same priority level, tasks are ordered by sequence number for FIFO guarantees.

### Named Queues

The system supports multiple logical queues, enabling workload partitioning:
- Different queues for different types of work
- Dedicated workers for specific queues
- Independent scaling and configuration per queue

### Task Scheduling

Tasks can be scheduled for future execution using cron-like expressions:
- **Interval-based**: "every 30 seconds"
- **Daily**: "at 14:30"
- **Cron**: "cron: * * * * *" (standard cron syntax)

The scheduler thread continuously monitors scheduled tasks and moves them to ready queues when their execution time arrives. Recurring tasks are automatically rescheduled.

### Task Deduplication

To prevent duplicate processing, the system supports idempotency keys:
- Producer can provide an `idempotency_key` with each task
- The broker generates a deduplication key from the payload and idempotency key
- If a task with the same key is submitted within the TTL window (default 1 hour), it's rejected
- This prevents accidental duplicate submissions

### Rate Limiting

To protect the broker from being overwhelmed, rate limiting is implemented using a token bucket algorithm:
- Each producer IP has its own token bucket
- Tokens are replenished at a configurable rate (default 10 tokens/second)
- Burst capacity is configurable (default 20 tokens)
- If a producer exceeds its rate limit, requests are rejected

### Message TTL (Time To Live)

Tasks can have an expiration time:
- **Queue-level TTL**: All tasks in a queue expire after a set duration
- **Per-message TTL**: Individual tasks can have custom TTL
- Expired tasks are automatically removed by the TTL cleanup thread
- This prevents stale tasks from consuming resources

### Backpressure Handling

To prevent queue overflow, the system implements backpressure:
- Each queue has a configurable maximum depth
- When a queue reaches its limit, new tasks are rejected
- The rejection includes a clear error message
- Producers can implement retry logic with exponential backoff

### Message Compression

Large payloads are automatically compressed:
- Configurable minimum size threshold (default 1KB)
- Supports Gzip and Zstandard algorithms
- Compression is transparent to clients
- Reduces network bandwidth and storage requirements

### Message Ordering

For FIFO guarantees, tasks are assigned sequence numbers:
- Each queue maintains a sequence counter
- Tasks are ordered by priority first, then sequence number
- Enables strict ordering within priority levels
- Supports partition keys for ordered processing within partitions

### Wildcard Subscriptions

Pub/sub supports pattern matching:
- Single-level wildcard: `events.*` matches `events.user` but not `events.user.login`
- Multi-level wildcard: `events.#` matches `events.user.login`
- Enables flexible topic routing and filtering

---

## Clustering and Consensus

### Raft Consensus Algorithm

For high availability, PyQueue implements the Raft consensus algorithm, allowing multiple broker nodes to form a cluster.

#### Raft Basics

Raft ensures that all nodes in a cluster agree on the same sequence of state changes. It divides time into terms (monotonically increasing integers) and elects a leader for each term. Only the leader can accept write operations, which are then replicated to followers.

#### Node States

Each node can be in one of three states:
- **FOLLOWER**: Passive state, receives updates from leader
- **CANDIDATE**: Actively seeking votes to become leader
- **LEADER**: Accepts client requests and replicates to followers

#### Leader Election

When a follower doesn't hear from the leader within the election timeout (default 5 seconds):
1. It transitions to CANDIDATE state
2. Increments its term number
3. Votes for itself
4. Sends vote requests to all other nodes
5. If it receives votes from a majority, it becomes LEADER
6. Otherwise, it returns to FOLLOWER state

This ensures at most one leader per term and prevents split-brain scenarios.

#### Log Replication

When the leader receives a write operation (PUSH, ACK, SCHEDULE, PUBLISH):
1. It appends the operation to its local log
2. Sends AppendEntries RPCs to all followers
3. Waits for majority acknowledgment
4. Commits the entry (applies to state machine)
5. Notifies followers of the commit index

Followers:
1. Receive AppendEntries RPC
2. Check log consistency (previous entry matches)
3. Append new entries or truncate conflicting entries
4. Update commit index when leader commits
5. Apply committed entries to their state machine

#### Log Consistency

Raft maintains log consistency through:
- **nextIndex**: For each follower, the leader tracks the next log index to send
- **matchIndex**: The highest log index known to be replicated on each follower
- **Conflict Resolution**: If a follower's log doesn't match, the leader backtracks and resends entries

#### State Machine Application

When a log entry is committed (majority has replicated it), it's applied to the broker's state machine:
- PUSH entries add tasks to queues
- ACK entries remove tasks from processing queue
- SCHEDULE entries add tasks to scheduled list
- PUBLISH entries distribute messages to subscribers

This ensures all nodes in the cluster have identical state.

#### Snapshots and Compaction

To prevent unbounded log growth, the system supports snapshots:
- Periodic snapshots of the entire broker state
- Log entries before the snapshot index can be discarded
- Snapshots are loaded on startup to restore state quickly
- Reduces storage requirements and startup time

### Cluster Configuration

A cluster is configured by specifying all node addresses:
```bash
CLUSTER_NODES="node1:5555,node2:5555,node3:5555"
```

Each node knows about all other nodes and can communicate with them for consensus operations.

---

## Security Architecture

### Authentication

PyQueue supports two authentication methods:

#### API Key Authentication
- Simple shared secret model
- Client sends: `ApiKey <key>`
- Server validates using constant-time comparison (`hmac.compare_digest`)
- Prevents timing attacks
- Returns principal: `"api_key_user"`

#### JWT Token Authentication
- Token-based authentication with expiration
- Client sends: `Bearer <token>`
- Server validates signature and expiration
- Token contains principal and metadata
- More flexible for multi-user scenarios

### Authorization

After authentication, authorization checks queue access:
- Permissions are configured per principal
- Supports wildcard patterns (`*` for all queues, `queue*` for prefix matching)
- Each operation (PUSH, PULL, etc.) can be authorized separately
- Currently implemented but not enforced in all handlers

### Configuration

Security is configured via:
- Configuration file: `security.enabled`, `security.api_key`, `security.jwt_secret`
- Environment variables: `SECURITY_ENABLED`, `API_KEY`, `JWT_SECRET`
- Permissions map: `security.permissions[principal] = [queue_list]`

---

## Performance Optimizations

### Connection Pooling

To reduce connection overhead, clients use connection pooling:
- Maintains a pool of pre-established TCP connections
- Reuses connections across multiple requests
- Health checks ensure connections are still valid
- Reduces latency and resource usage

### Batch Operations

For high-throughput scenarios, batch operations are supported:
- `PUSH_BATCH`: Submit multiple tasks in one request
- `PULL_BATCH`: Receive multiple tasks in one request
- Reduces network round-trips
- Improves throughput significantly

### Async Journal Writes

While journal writes are synchronous for durability, the system batches multiple operations when possible:
- Multiple operations can be written in a single journal entry
- Reduces I/O overhead
- Maintains durability guarantees

### Efficient Data Structures

- Priority queues use binary heaps (O(log n) insert, O(1) peek)
- Processing queue uses hash maps (O(1) lookup)
- Worker registry uses hash maps for fast updates
- All critical sections are protected by locks to prevent race conditions

---

## Data Flow and Lifecycle

### Task Submission Flow

1. **Producer** creates a task with payload, priority, queue name
2. **Producer** connects to broker (or uses connection pool)
3. **Producer** sends `PUSH` command with task data
4. **Broker** validates request (auth, rate limit, deduplication)
5. **Broker** checks backpressure (queue depth limits)
6. **Broker** generates task ID and sequence number
7. **Broker** adds task to priority queue
8. **Broker** writes to journal (for persistence)
9. **Broker** replicates to cluster (if enabled)
10. **Broker** sends acknowledgment to producer
11. **Producer** receives task ID

### Task Processing Flow

1. **Worker** connects to broker
2. **Worker** sends periodic heartbeats
3. **Worker** sends `PULL` command with queue name
4. **Broker** checks if tasks are available in queue
5. **Broker** pops highest-priority task from queue
6. **Broker** moves task to processing queue
7. **Broker** sets visibility timeout
8. **Broker** sends task to worker
9. **Worker** processes task (application logic)
10. **Worker** sends `ACK` (success) or `NACK` (failure)
11. **Broker** removes task from processing queue (ACK) or requeues (NACK)
12. **Broker** writes to journal and replicates

### Visibility Timeout Recovery

1. **Reaper thread** wakes up every second
2. Checks all tasks in processing queue
3. Finds tasks where `expires_at < current_time`
4. Moves expired tasks back to ready queue
5. Clears `expires_at` timestamp
6. Logs the expiration event

### Scheduled Task Execution

1. **Producer** sends `SCHEDULE` command with schedule expression
2. **Broker** parses schedule and calculates next run time
3. **Broker** adds task to scheduled tasks list
4. **Scheduler thread** wakes up every second
5. Checks scheduled tasks for due items
6. Moves due tasks to ready queues
7. For recurring tasks, calculates next run time and reschedules

---

## Component Interactions

### Threading Model

The broker uses a multi-threaded architecture:

**Main Thread**: Accepts connections and spawns client handler threads

**Client Handler Threads**: One per connected client, handles all commands from that client

**Background Threads**:
- Reaper: Monitors visibility timeouts
- Scheduler: Processes scheduled tasks
- Heartbeat: Detects dead workers
- Metrics: Logs statistics
- TTL Cleanup: Removes expired tasks
- Raft Election: Handles leader election (if clustered)
- Raft Heartbeat: Sends heartbeats to followers (if leader)
- Raft Replication: Replicates log entries (if leader)

All shared data structures are protected by locks to ensure thread safety.

### Locking Strategy

- **queue_lock**: Protects all queue operations (ready queues, processing queue, scheduled tasks)
- **journal_lock**: Protects journal file writes
- **transaction_lock**: Protects transaction state
- **vote_lock**: Protects Raft vote counting
- **log_lock**: Protects Raft log operations

Locks are held for minimal time to maximize concurrency.

### Error Handling

The system handles various error conditions:
- Network failures: Connection retries, graceful degradation
- Invalid commands: Error responses sent to clients
- Queue full: Backpressure rejection with clear error
- Rate limit exceeded: Rejection with rate limit error
- Duplicate tasks: Rejection with deduplication error
- Worker crashes: Automatic task recovery via visibility timeout
- Broker crashes: State recovery from journal on restart

### Monitoring and Observability

**Structured Logging**: All events are logged in JSON format with:
- Timestamp
- Log level (DEBUG, INFO, WARN, ERROR)
- Component name
- Event details
- Context information

**Metrics Collection**: Tracks:
- Tasks pushed, completed, failed, expired
- Queue depths
- Processing times
- Throughput
- Active workers
- Raft metrics (term, log length, commit index)

**Prometheus Integration**: Exposes metrics via HTTP endpoint for scraping by Prometheus

**Health Checks**: Admin command provides system health status

---

## Summary

PyQueue is a comprehensive distributed message broker that provides:

1. **Reliable Message Delivery**: At-least-once semantics with visibility timeout
2. **High Availability**: Raft-based clustering for fault tolerance
3. **Scalability**: Horizontal scaling of workers and brokers
4. **Flexibility**: Priority queues, scheduling, pub/sub, transactions
5. **Observability**: Comprehensive logging and metrics
6. **Security**: Authentication and authorization
7. **Performance**: Connection pooling, batching, compression
8. **Durability**: Journal-based persistence and crash recovery

The system is designed for production use with enterprise-grade features while maintaining simplicity and ease of use. It demonstrates advanced distributed systems concepts including consensus algorithms, fault tolerance, and high-performance networking.

