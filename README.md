# PyQueue

A distributed message broker with Raft consensus, async I/O, and fault tolerance.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

PyQueue decouples task creation from execution, providing **at-least-once delivery** guarantees through visibility timeout mechanisms.

## Features

- **Fault Tolerance**: Visibility timeout with automatic task requeuing
- **Persistence**: Append-only journal log for crash recovery
- **Clustering**: Raft consensus for high availability
- **Async I/O**: Full asyncio implementation
- **Priority Queues**: HIGH/NORMAL/LOW priority levels
- **Task Scheduling**: Cron expressions, intervals, daily schedules
- **Pub/Sub**: Topic-based messaging with wildcards
- **Transactions**: Atomic multi-operation support
- **Security**: API key/JWT auth, TLS, rate limiting
- **Observability**: Prometheus metrics, structured logging
- **REST API**: FastAPI with OpenAPI docs
- **Web Dashboard**: Real-time monitoring

## Quick Start

### Docker

```bash
docker-compose up -d
open http://localhost:8080
```

### Local

```bash
pip install -r requirements.txt
python run_broker.py

# In separate terminals
python run_worker.py
python run_producer.py "task_payload"
```

## Usage

### Producer/Worker

```python
from pyqueue.producer import Producer
from pyqueue.worker import Worker

# Submit task
producer = Producer()
producer.push("process_data", priority="HIGH", queue="jobs")

# Process tasks
def handler(task):
    print(f"Processing: {task.payload}")
    return True

worker = Worker()
worker.set_process_callback(handler)
worker.run()
```

### REST API

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{"payload": "task_data", "priority": "HIGH"}'
```

### Scheduling

```python
producer.schedule("every 30 seconds", "health_check")
producer.schedule("at 14:30", "daily_report")
producer.schedule("cron: 0 * * * *", "hourly_backup")
```

### Clustering

```bash
CLUSTER_NODES=node2:5556,node3:5557 python run_broker.py
```

## Architecture

```
Producer ──PUSH──> Broker <──PULL── Worker
                     │
              ┌──────┴──────┐
              │  Raft       │
              │  Consensus  │
              └─────────────┘
```

- **Broker**: Manages queues, visibility timeouts, persistence
- **Producer**: Submits tasks, disconnects immediately
- **Worker**: Pulls tasks, sends ACK/NACK

## Configuration

```yaml
broker:
  host: "0.0.0.0"
  port: 5555
  visibility_timeout: 10

security:
  enabled: false
  tls_enabled: false

monitoring:
  prometheus_enabled: true
  prometheus_port: 9090
```

See `pyqueue.yaml.example` for all options.

## Testing

```bash
pytest tests/
pytest --cov=pyqueue tests/
```

## Admin CLI

```bash
python run_admin.py queues
python run_admin.py queue-info default
python run_admin.py health
python run_admin.py stats
```

## Documentation

- [System Architecture](SYSTEM_ARCHITECTURE.md)
- [Docker Guide](DOCKER.md)

## Project Structure

```
pyqueue/           # Core package
tests/             # Test suite
run_*.py           # Launcher scripts
docker-compose.yml # Docker orchestration
```

## License

MIT License - see [LICENSE](LICENSE)
