"""Task scheduling with cron-like expressions."""

import time
import re
from typing import Optional, List
from datetime import datetime, timedelta


def parse_schedule(schedule_str: str) -> Optional[dict]:
    """Parse a simple schedule expression.
    
    Formats supported:
    - "every N seconds" - e.g., "every 30 seconds"
    - "every N minutes" - e.g., "every 5 minutes"
    - "at HH:MM" - e.g., "at 14:30" (daily)
    - "cron: * * * * *" - basic cron format (minute hour day month weekday)
    
    Returns: dict with schedule info or None if invalid
    """
    schedule_str = schedule_str.strip().lower()
    
    if schedule_str.startswith("every"):
        match = re.match(r"every\s+(\d+)\s+(second|minute|hour)s?", schedule_str)
        if match:
            value = int(match.group(1))
            unit = match.group(2)
            
            if unit.startswith("second"):
                return {"type": "interval", "seconds": value}
            elif unit.startswith("minute"):
                return {"type": "interval", "seconds": value * 60}
            elif unit.startswith("hour"):
                return {"type": "interval", "seconds": value * 3600}
    
    if schedule_str.startswith("at"):
        match = re.match(r"at\s+(\d{1,2}):(\d{2})", schedule_str)
        if match:
            hour = int(match.group(1))
            minute = int(match.group(2))
            if 0 <= hour < 24 and 0 <= minute < 60:
                return {"type": "daily", "hour": hour, "minute": minute}
    
    if schedule_str.startswith("cron:"):
        cron_parts = schedule_str[5:].strip().split()
        if len(cron_parts) == 5:
            return {"type": "cron", "expression": cron_parts}
    
    return None


def next_run_time(schedule: dict, current_time: Optional[float] = None) -> float:
    """Calculate next run time based on schedule.
    
    Returns: Unix timestamp of next run
    """
    if current_time is None:
        current_time = time.time()
    
    now = datetime.fromtimestamp(current_time)
    
    if schedule["type"] == "interval":
        return current_time + schedule["seconds"]
    
    elif schedule["type"] == "daily":
        target = now.replace(hour=schedule["hour"], minute=schedule["minute"], second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        return target.timestamp()
    
    elif schedule["type"] == "cron":
        return _next_cron_time(schedule["expression"], current_time)
    
    return current_time + 60


def _next_cron_time(cron_parts: List[str], current_time: float) -> float:
    """Calculate next run time for cron expression.
    
    Simplified cron parser - supports:
    - * (any value)
    - numbers
    - ranges (e.g., 1-5)
    """
    minute, hour, day, month, weekday = cron_parts
    
    now = datetime.fromtimestamp(current_time)
    next_time = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    
    def matches(value: int, pattern: str) -> bool:
        if pattern == "*":
            return True
        if "-" in pattern:
            start, end = map(int, pattern.split("-"))
            return start <= value <= end
        try:
            return value == int(pattern)
        except ValueError:
            return False
    
    for _ in range(10080):
        if (matches(next_time.minute, minute) and
            matches(next_time.hour, hour) and
            matches(next_time.day, day) and
            matches(next_time.month, month) and
            matches(next_time.weekday(), weekday)):
            if next_time.timestamp() > current_time:
                return next_time.timestamp()
        next_time += timedelta(minutes=1)
    
    return current_time + 3600

