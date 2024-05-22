"""Connection pooling for improved performance."""

import socket
import threading
import time
from typing import Optional, List
from queue import Queue, Empty


class ConnectionPool:
    """Simple connection pool for TCP connections."""
    
    def __init__(self, host: str, port: int, max_connections: int = 10, idle_timeout: float = 30.0):
        self.host = host
        self.port = port
        self.max_connections = max_connections
        self.idle_timeout = idle_timeout
        self.pool: Queue = Queue(maxsize=max_connections)
        self.active_connections = 0
        self.lock = threading.Lock()
    
    def get_connection(self, timeout: float = 5.0) -> Optional[socket.socket]:
        """Get a connection from the pool or create a new one."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Try to get from pool
                conn = self.pool.get(timeout=0.1)
                # Check if connection is still alive
                try:
                    conn.getpeername()
                    # Test connection with a small timeout
                    conn.settimeout(0.1)
                    try:
                        conn.recv(1, socket.MSG_PEEK)
                    except (socket.timeout, socket.error):
                        pass
                    conn.settimeout(timeout)
                    return conn
                except (OSError, socket.error):
                    # Connection is dead, close it
                    try:
                        conn.close()
                    except (OSError, socket.error):
                        pass
                    with self.lock:
                        self.active_connections -= 1
            except Empty:
                pass
            
            # Create new connection
            with self.lock:
                if self.active_connections < self.max_connections:
                    try:
                        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        conn.settimeout(timeout)
                        conn.connect((self.host, self.port))
                        self.active_connections += 1
                        return conn
                    except Exception as e:
                        if attempt < max_retries - 1:
                            time.sleep(0.1 * (attempt + 1))
                            continue
                        return None
                else:
                    # Pool is full, wait for a connection
                    try:
                        conn = self.pool.get(timeout=timeout)
                        # Verify connection is still valid
                        try:
                            conn.getpeername()
                            return conn
                        except (OSError, socket.error):
                            try:
                                conn.close()
                            except (OSError, socket.error):
                                pass
                            with self.lock:
                                self.active_connections -= 1
                            continue
                    except Empty:
                        return None
        
        return None
    
    def return_connection(self, conn: socket.socket):
        """Return a connection to the pool."""
        if conn is None:
            return
        
        try:
            # Check if connection is still valid
            conn.getpeername()
            # Test connection health
            conn.settimeout(0.1)
            try:
                conn.recv(1, socket.MSG_PEEK)
            except (socket.timeout, socket.error):
                pass
            conn.settimeout(None)
            
            try:
                self.pool.put_nowait(conn)
            except Exception:
                # Pool is full, close the connection
                try:
                    conn.close()
                except (OSError, socket.error):
                    pass
                with self.lock:
                    self.active_connections -= 1
        except (OSError, socket.error):
            # Connection is dead, don't return it
            try:
                conn.close()
            except (OSError, socket.error):
                pass
            with self.lock:
                self.active_connections -= 1
    
    def release_connection(self, conn: socket.socket):
        """Alias for return_connection for consistency."""
        self.return_connection(conn)

