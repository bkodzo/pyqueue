"""Prometheus metrics export for PyQueue."""

from prometheus_client import Counter, Gauge, Histogram, start_http_server
from typing import Optional


# Module-level metrics (singleton pattern to avoid duplicate registration)
_metrics_initialized = False
_tasks_pushed = None
_tasks_completed = None
_tasks_failed = None
_tasks_expired = None
_queue_depth = None
_processing_tasks = None
_active_workers = None
_processing_time = None
_throughput = None
_raft_term = None
_raft_log_length = None
_raft_commit_index = None


def _init_metrics():
    """Initialize metrics only once."""
    global _metrics_initialized, _tasks_pushed, _tasks_completed, _tasks_failed
    global _tasks_expired, _queue_depth, _processing_tasks, _active_workers
    global _processing_time, _throughput, _raft_term, _raft_log_length, _raft_commit_index
    
    if _metrics_initialized:
        return
    
    _tasks_pushed = Counter('pyqueue_tasks_pushed_total', 'Total tasks pushed')
    _tasks_completed = Counter('pyqueue_tasks_completed_total', 'Total tasks completed')
    _tasks_failed = Counter('pyqueue_tasks_failed_total', 'Total tasks failed')
    _tasks_expired = Counter('pyqueue_tasks_expired_total', 'Total tasks expired')
    
    _queue_depth = Gauge('pyqueue_queue_depth', 'Current queue depth', ['queue_name'])
    _processing_tasks = Gauge('pyqueue_processing_tasks', 'Tasks currently processing')
    _active_workers = Gauge('pyqueue_active_workers', 'Active workers')
    
    _processing_time = Histogram('pyqueue_processing_time_seconds', 'Task processing time', buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0])
    _throughput = Gauge('pyqueue_throughput_tasks_per_second', 'Tasks per second throughput')
    
    _raft_term = Gauge('pyqueue_raft_term', 'Current Raft term')
    _raft_log_length = Gauge('pyqueue_raft_log_length', 'Raft log length')
    _raft_commit_index = Gauge('pyqueue_raft_commit_index', 'Raft commit index')
    
    _metrics_initialized = True


class PrometheusMetrics:
    """Prometheus metrics collector."""
    
    _server_started = False
    
    def __init__(self, port: int = 9090):
        self.port = port
        _init_metrics()
        
        self.tasks_pushed = _tasks_pushed
        self.tasks_completed = _tasks_completed
        self.tasks_failed = _tasks_failed
        self.tasks_expired = _tasks_expired
        self.queue_depth = _queue_depth
        self.processing_tasks = _processing_tasks
        self.active_workers = _active_workers
        self.processing_time = _processing_time
        self.throughput = _throughput
        self.raft_term = _raft_term
        self.raft_log_length = _raft_log_length
        self.raft_commit_index = _raft_commit_index
    
    def start(self):
        """Start Prometheus metrics server."""
        if not PrometheusMetrics._server_started:
            start_http_server(self.port)
            PrometheusMetrics._server_started = True
    
    def record_push(self, queue_name: str = "default"):
        """Record a task push."""
        self.tasks_pushed.inc()
        self.queue_depth.labels(queue_name=queue_name).inc()
    
    def record_completion(self, processing_time: float):
        """Record a task completion."""
        self.tasks_completed.inc()
        self.processing_time.observe(processing_time)
        self.processing_tasks.dec()
    
    def record_failure(self):
        """Record a task failure."""
        self.tasks_failed.inc()
        self.processing_tasks.dec()
    
    def record_expiration(self):
        """Record a task expiration."""
        self.tasks_expired.inc()
        self.processing_tasks.dec()
    
    def update_queue_depth(self, queue_name: str, depth: int):
        """Update queue depth metric."""
        self.queue_depth.labels(queue_name=queue_name).set(depth)
    
    def update_processing_tasks(self, count: int):
        """Update processing tasks metric."""
        self.processing_tasks.set(count)
    
    def update_active_workers(self, count: int):
        """Update active workers metric."""
        self.active_workers.set(count)
    
    def update_throughput(self, tasks_per_second: float):
        """Update throughput metric."""
        self.throughput.set(tasks_per_second)
    
    def update_raft_metrics(self, term: int, log_length: int, commit_index: int):
        """Update Raft metrics."""
        self.raft_term.set(term)
        self.raft_log_length.set(log_length)
        self.raft_commit_index.set(commit_index)

