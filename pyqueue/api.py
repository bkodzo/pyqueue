"""FastAPI REST API server for PyQueue."""

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional, List, Dict
import json
import asyncio
from .broker import Broker


app = FastAPI(title="PyQueue API", version="1.0.0")
broker_instance: Optional[Broker] = None


class TaskRequest(BaseModel):
    payload: str
    priority: Optional[str] = "NORMAL"
    queue: Optional[str] = "default"
    idempotency_key: Optional[str] = None


class BatchTaskRequest(BaseModel):
    tasks: List[Dict]


class ScheduleRequest(BaseModel):
    payload: str
    schedule: str
    idempotency_key: Optional[str] = None


class NackRequest(BaseModel):
    task_id: str
    error: str


class HeartbeatRequest(BaseModel):
    worker_id: str


def set_broker(broker: Broker):
    """Set the broker instance for the API."""
    global broker_instance
    broker_instance = broker


@app.get("/")
async def root():
    """Serve the web dashboard."""
    return HTMLResponse(content=get_dashboard_html())


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "broker_running": broker_instance is not None and broker_instance.running}


@app.get("/api/queues")
async def get_queues():
    """Get all queue statistics."""
    if not broker_instance:
        raise HTTPException(status_code=503, detail="Broker not available")
    
    if broker_instance.host == 'broker':
        return await _get_stats_from_broker()
    
    stats = broker_instance.get_queue_stats()
    return stats


@app.get("/api/queues/{queue_name}")
async def get_queue(queue_name: str):
    """Get statistics for a specific queue."""
    if not broker_instance:
        raise HTTPException(status_code=503, detail="Broker not available")
    
    if broker_instance.host == 'broker':
        stats = await _get_stats_from_broker()
    else:
        stats = broker_instance.get_queue_stats()
    
    if queue_name not in stats.get("queues", {}):
        raise HTTPException(status_code=404, detail="Queue not found")
    
    return stats["queues"][queue_name]


@app.post("/api/tasks")
async def create_task(task: TaskRequest):
    """Create a new task."""
    if not broker_instance:
        raise HTTPException(status_code=503, detail="Broker not available")
    
    from .producer_async import AsyncProducer
    
    try:
        producer = AsyncProducer(host=broker_instance.host, port=broker_instance.port)
        task_id = await producer.push(
            task.payload,
            priority=task.priority,
            queue=task.queue,
            idempotency_key=task.idempotency_key
        )
        await producer.close()
        return {"task_id": task_id, "status": "created"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create task: {str(e)}")


@app.post("/api/tasks/batch")
async def create_batch_tasks(batch: BatchTaskRequest):
    """Create multiple tasks in batch."""
    if not broker_instance:
        raise HTTPException(status_code=503, detail="Broker not available")
    
    from .producer_async import AsyncProducer
    
    try:
        producer = AsyncProducer(host=broker_instance.host, port=broker_instance.port)
        result = await producer.push_batch(batch.tasks)
        await producer.close()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create batch: {str(e)}")


async def _get_stats_from_broker():
    """Get stats from broker service via TCP (async)."""
    if not broker_instance:
        raise HTTPException(status_code=503, detail="Broker not available")
    
    from .admin_async import AsyncAdminClient
    
    try:
        admin_client = AsyncAdminClient(host=broker_instance.host, port=broker_instance.port)
        stats = await admin_client.get_stats()
        await admin_client.close()
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Connection error: {str(e)}")

@app.get("/api/metrics")
async def get_metrics():
    """Get system metrics."""
    if not broker_instance:
        raise HTTPException(status_code=503, detail="Broker not available")
    
    if broker_instance.host == 'broker':
        return await _get_stats_from_broker()
    
    async with broker_instance.queue_lock:
        total_ready = sum(len(q) for q in broker_instance.queues.values())
        processing_count = len(broker_instance.processing_queue)
    
    stats = broker_instance.metrics.get_stats(total_ready, processing_count)
    queue_stats = broker_instance.get_queue_stats()
    
    return {
        **stats,
        **queue_stats
    }


@app.get("/api/workers")
async def get_workers():
    """Get active workers."""
    if not broker_instance:
        raise HTTPException(status_code=503, detail="Broker not available")
    
    if broker_instance.host == 'broker':
        stats = await _get_stats_from_broker()
        return {"workers": [], "count": stats.get("active_workers", 0)}
    
    async with broker_instance.queue_lock:
        workers = [
            {
                "worker_id": wid,
                "last_heartbeat": info["last_heartbeat"],
                "status": info["status"]
            }
            for wid, info in broker_instance.workers.items()
        ]
    
    return {"workers": workers, "count": len(workers)}


@app.get("/api/dlq")
async def get_dlq():
    """Get dead letter queue tasks."""
    if not broker_instance:
        raise HTTPException(status_code=503, detail="Broker not available")
    
    if broker_instance.host == 'broker':
        stats = await _get_stats_from_broker()
        return {"tasks": [], "count": stats.get("dlq", 0)}
    
    async with broker_instance.queue_lock:
        dlq_tasks = [
            {
                "task_id": task.task_id,
                "payload": task.payload,
                "retry_count": task.retry_count,
                "created_at": task.created_at
            }
            for task in broker_instance.dead_letter_queue
        ]
    
    return {"tasks": dlq_tasks, "count": len(dlq_tasks)}


def get_dashboard_html() -> str:
    """Generate the web dashboard HTML."""
    return """<!DOCTYPE html>
<html>
<head>
    <title>PyQueue Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; padding: 20px; }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { color: #333; margin-bottom: 30px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .card { background: white; border-radius: 8px; padding: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .card h2 { font-size: 18px; margin-bottom: 15px; color: #555; }
        .stat { font-size: 32px; font-weight: bold; color: #2196F3; }
        .stat-label { color: #999; font-size: 14px; margin-top: 5px; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: #f9f9f9; font-weight: 600; color: #555; }
        .queue-name { font-weight: 600; color: #2196F3; }
        .status-active { color: #4CAF50; }
        .status-inactive { color: #f44336; }
        .refresh-btn { background: #2196F3; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; margin-bottom: 20px; }
        .refresh-btn:hover { background: #1976D2; }
    </style>
</head>
<body>
    <div class="container">
        <h1>PyQueue Dashboard</h1>
        <button class="refresh-btn" onclick="loadData()">Refresh</button>
        
        <div class="grid">
            <div class="card">
                <h2>System Metrics</h2>
                <div id="metrics"></div>
            </div>
            <div class="card">
                <h2>Queue Statistics</h2>
                <div id="queues"></div>
            </div>
            <div class="card">
                <h2>Workers</h2>
                <div id="workers"></div>
            </div>
            <div class="card">
                <h2>Dead Letter Queue</h2>
                <div id="dlq"></div>
            </div>
        </div>
    </div>
    
    <script>
        async function loadData() {
            try {
                const [metrics, queues, workers, dlq] = await Promise.all([
                    fetch('/api/metrics').then(r => r.json()),
                    fetch('/api/queues').then(r => r.json()),
                    fetch('/api/workers').then(r => r.json()),
                    fetch('/api/dlq').then(r => r.json())
                ]);
                
                document.getElementById('metrics').innerHTML = `
                    <div class="stat">${metrics.tasks_pushed || 0}</div>
                    <div class="stat-label">Tasks Pushed</div>
                    <div class="stat" style="margin-top: 20px;">${metrics.tasks_completed || 0}</div>
                    <div class="stat-label">Tasks Completed</div>
                    <div class="stat" style="margin-top: 20px;">${metrics.tasks_per_second || 0}</div>
                    <div class="stat-label">Tasks/Second</div>
                `;
                
                let queuesHtml = '<table><tr><th>Queue</th><th>Ready</th><th>Processing</th></tr>';
                for (const [name, stats] of Object.entries(queues.queues || {})) {
                    queuesHtml += `<tr>
                        <td class="queue-name">${name}</td>
                        <td>${stats.depth || 0}</td>
                        <td>${stats.processing || 0}</td>
                    </tr>`;
                }
                queuesHtml += '</table>';
                document.getElementById('queues').innerHTML = queuesHtml;
                
                let workersHtml = '<table><tr><th>Worker ID</th><th>Status</th><th>Last Heartbeat</th></tr>';
                (workers.workers || []).forEach(w => {
                    const date = new Date(w.last_heartbeat * 1000);
                    workersHtml += `<tr>
                        <td>${w.worker_id}</td>
                        <td class="status-active">${w.status}</td>
                        <td>${date.toLocaleString()}</td>
                    </tr>`;
                });
                workersHtml += '</table>';
                document.getElementById('workers').innerHTML = workersHtml || '<p>No active workers</p>';
                
                document.getElementById('dlq').innerHTML = `
                    <div class="stat">${dlq.count || 0}</div>
                    <div class="stat-label">Failed Tasks</div>
                `;
            } catch (error) {
                console.error('Error loading data:', error);
            }
        }
        
        loadData();
        setInterval(loadData, 5000);
    </script>
</body>
</html>
"""

