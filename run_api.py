#!/usr/bin/env python3
"""Launcher script for the REST API server."""

import uvicorn
import threading
import os
from pyqueue.broker import Broker, main as broker_main
from pyqueue.api import app, set_broker


def start_broker():
    """Connect to broker service (for Docker) or start local broker."""
    import time
    import socket
    
    broker_host = os.getenv('BROKER_HOST', 'broker')
    broker_port = int(os.getenv('BROKER_PORT', '5555'))
    
    broker = Broker(host=broker_host, port=broker_port, visibility_timeout=10)
    set_broker(broker)
    
    if broker_host == '0.0.0.0' or broker_host == 'localhost':
        broker_thread = threading.Thread(target=broker.start, daemon=True)
        broker_thread.start()
        time.sleep(1)
    
    return broker


if __name__ == "__main__":
    broker = start_broker()
    
    api_port = int(os.getenv('API_PORT', '8080'))
    print(f"Starting API server on port {api_port}")
    print(f"Broker running on {broker.host}:{broker.port}")
    print(f"Dashboard: http://localhost:{api_port}")
    
    try:
        uvicorn.run(app, host="0.0.0.0", port=api_port)
    except KeyboardInterrupt:
        print("\nShutting down...")
        broker.stop()

