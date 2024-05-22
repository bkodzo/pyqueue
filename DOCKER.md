# Docker Guide for PyQueue

## Quick Start

### Start the System

```bash
# Build and start all services (broker, API, workers)
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f
```

### Access Services

- **Web Dashboard**: http://localhost:8080
- **REST API**: http://localhost:8080/api
- **API Docs**: http://localhost:8080/docs
- **Broker TCP**: localhost:5555

### Submit Tasks

**Option 1: Using REST API (Recommended)**
```bash
# From host machine
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{"payload": "resize_image.jpg", "priority": "HIGH", "queue": "image-processing"}'
```

**Option 2: Using Producer Script**
```bash
# From host machine (if broker is exposed)
BROKER_HOST=localhost BROKER_PORT=5555 python run_producer.py "resize_image.jpg"

# Or using Docker
docker-compose run --rm producer python run_producer.py "your_task_here"
```

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f broker
docker-compose logs -f api
docker-compose logs -f worker

# Last 100 lines
docker-compose logs --tail=100 broker
```

### Scale Workers

```bash
# Run 5 workers
docker-compose up -d --scale worker=5

# Check running workers
docker-compose ps | grep worker
```

### Stop the System

```bash
# Stop all containers
docker-compose down

# Stop and remove volumes (clears journal.log)
docker-compose down -v
```

## Architecture

```
┌─────────────────────────────────────────┐
│         Docker Network                  │
│                                         │
│  ┌──────────┐      ┌──────────┐       │
│  │  Broker  │◄──────│  Worker  │       │
│  │ :5555    │      │  (x2)    │       │
│  └────┬─────┘      └──────────┘       │
│       │                                 │
│       │                                 │
│  ┌────▼─────┐                          │
│  │   API    │                          │
│  │  :8080   │                          │
│  └────┬─────┘                          │
│       │                                 │
│       │ Ports exposed to host          │
└───────┼─────────────────────────────────┘
        │
        ▼
   Host Machine
   - Dashboard: localhost:8080
   - Broker TCP: localhost:5555
```

## Environment Variables

Configure via `docker-compose.yml` or `.env` file:

- `BROKER_HOST`: Broker bind address (default: `0.0.0.0` for broker, `broker` for clients)
- `BROKER_PORT`: Broker port (default: `5000`)
- `PYTHONUNBUFFERED`: Disable Python output buffering (default: `1`)

## Persistence

The `journal.log` file is mounted as a volume, so:
- Tasks survive container restarts
- Journal persists on host filesystem
- Broker replays journal on startup

## Development

### Rebuild After Code Changes

```bash
docker-compose build
docker-compose up -d
```

### Run Single Container

```bash
# Just the broker
docker-compose up broker

# Just one worker
docker-compose run --rm worker python run_worker.py
```

### Access Container Shell

```bash
docker-compose exec broker /bin/bash
docker-compose exec worker /bin/bash
```

## Troubleshooting

### Port Already in Use

The broker is exposed on port **5555** on the host (maps to 5555 in container) 

The API server is exposed on port **8080** on the host (maps to 8080 in container).

To change the port, edit `docker-compose.yml`:
```yaml
ports:
  - "YOUR_PORT:5000"  # Host:Container
```

Then update `BROKER_PORT` environment variable if needed.

### Workers Can't Connect

Check that workers use `BROKER_HOST=broker` (service name, not `localhost`)

### Journal Not Persisting

Ensure volume mount exists:
```yaml
volumes:
  - ./journal.log:/app/journal.log
```

