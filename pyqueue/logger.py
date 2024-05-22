"""Structured logging for PyQueue."""

import json
import time
from typing import Dict, Any, Optional


class Logger:
    """JSON-structured logger for observability."""
    
    def __init__(self, component: str = "broker"):
        self.component = component
    
    def _log(self, level: str, message: str, **kwargs):
        """Internal logging method."""
        log_entry = {
            "timestamp": time.time(),
            "level": level,
            "component": self.component,
            "message": message,
            **kwargs
        }
        print(json.dumps(log_entry))
    
    def info(self, message: str, **kwargs):
        """Log info level message."""
        self._log("INFO", message, **kwargs)
    
    def warn(self, message: str, **kwargs):
        """Log warning level message."""
        self._log("WARN", message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error level message."""
        self._log("ERROR", message, **kwargs)
    
    def debug(self, message: str, **kwargs):
        """Log debug level message."""
        self._log("DEBUG", message, **kwargs)

