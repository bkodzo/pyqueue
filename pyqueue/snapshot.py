"""Raft log snapshot and compaction."""

import json
import time
import os
from typing import List, Optional
from .cluster import LogEntry


class SnapshotManager:
    """Manages Raft log snapshots for compaction."""
    
    def __init__(self, snapshot_dir: str = "snapshots", snapshot_interval: int = 3600, 
                 snapshot_threshold: int = 1000):
        self.snapshot_dir = snapshot_dir
        self.snapshot_interval = snapshot_interval
        self.snapshot_threshold = snapshot_threshold
        self.last_snapshot_time = time.time()
        self.last_snapshot_index = 0
        
        os.makedirs(snapshot_dir, exist_ok=True)
    
    def should_snapshot(self, log_length: int, current_time: float) -> bool:
        """Check if snapshot should be taken."""
        time_elapsed = current_time - self.last_snapshot_time
        entries_since_snapshot = log_length - self.last_snapshot_index
        
        return (time_elapsed >= self.snapshot_interval or 
                entries_since_snapshot >= self.snapshot_threshold)
    
    def create_snapshot(self, log_index: int, state: dict, snapshot_file: Optional[str] = None) -> str:
        """Create a snapshot of the current state."""
        if snapshot_file is None:
            snapshot_file = os.path.join(self.snapshot_dir, f"snapshot_{log_index}_{int(time.time())}.json")
        
        snapshot_data = {
            "log_index": log_index,
            "timestamp": time.time(),
            "state": state
        }
        
        with open(snapshot_file, 'w') as f:
            json.dump(snapshot_data, f, indent=2)
        
        self.last_snapshot_time = time.time()
        self.last_snapshot_index = log_index
        
        return snapshot_file
    
    def load_snapshot(self, snapshot_file: str) -> tuple[int, dict]:
        """Load state from snapshot. Returns (log_index, state)."""
        with open(snapshot_file, 'r') as f:
            snapshot_data = json.load(f)
        
        log_index = snapshot_data["log_index"]
        state = snapshot_data["state"]
        
        self.last_snapshot_index = log_index
        
        return log_index, state
    
    def get_latest_snapshot(self) -> Optional[str]:
        """Get the latest snapshot file."""
        if not os.path.exists(self.snapshot_dir):
            return None
        
        snapshots = [f for f in os.listdir(self.snapshot_dir) if f.startswith("snapshot_") and f.endswith(".json")]
        if not snapshots:
            return None
        
        snapshots.sort(reverse=True)
        return os.path.join(self.snapshot_dir, snapshots[0])
    
    def compact_log(self, log: List[LogEntry], snapshot_index: int) -> List[LogEntry]:
        """Remove log entries before snapshot index."""
        return [entry for entry in log if entry.index > snapshot_index]

