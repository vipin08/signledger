"""Connection pooling implementation for database backends."""

import time
import threading
import queue
import logging
from typing import Any, Dict, Optional, Callable, List, Union, ContextManager
from dataclasses import dataclass, field
from contextlib import contextmanager
from abc import ABC, abstractmethod
import weakref
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class PoolStats:
    """Connection pool statistics."""
    created_connections: int = 0
    active_connections: int = 0
    idle_connections: int = 0
    failed_connections: int = 0
    wait_time_ms: float = 0
    total_requests: int = 0
    cache_hits: int = 0
    timeouts: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'created_connections': self.created_connections,
            'active_connections': self.active_connections,
            'idle_connections': self.idle_connections,
            'failed_connections': self.failed_connections,
            'avg_wait_time_ms': self.wait_time_ms / self.total_requests if self.total_requests > 0 else 0,
            'total_requests': self.total_requests,
            'cache_hits': self.cache_hits,
            'timeouts': self.timeouts,
            'pool_efficiency': self.cache_hits / self.total_requests if self.total_requests > 0 else 0,
        }


@dataclass
class PooledConnection:
    """Wrapper for pooled connections."""
    connection: Any
    created_at: float = field(default_factory=time.time)
    last_used: float = field(default_factory=time.time)
    use_count: int = 0
    pool_ref: Optional[Any] = None
    
    def is_expired(self, max_lifetime: float) -> bool:
        """Check if connection has exceeded max lifetime."""
        return time.time() - self.created_at > max_lifetime
    
    def is_idle_too_long(self, max_idle_time: float) -> bool:
        """Check if connection has been idle too long."""
        return time.time() - self.last_used > max_idle_time
    
    def update_usage(self):
        """Update usage statistics."""
        self.last_used = time.time()
        self.use_count += 1


class ConnectionFactory(ABC):
    """Abstract factory for creating connections."""
    
    @abstractmethod
    def create_connection(self) -> Any:
        """Create a new connection."""
        pass
    
    @abstractmethod
    def validate_connection(self, connection: Any) -> bool:
        """Validate that a connection is still usable."""
        pass
    
    @abstractmethod
    def close_connection(self, connection: Any) -> None:
        """Close a connection."""
        pass


class GenericConnectionPool:
    """Generic connection pool implementation."""
    
    def __init__(
        self,
        factory: ConnectionFactory,
        min_size: int = 1,
        max_size: int = 10,
        max_overflow: int = 5,
        timeout: float = 30.0,
        max_lifetime: float = 3600.0,  # 1 hour
        max_idle_time: float = 600.0,  # 10 minutes
        validation_interval: float = 60.0,  # 1 minute
    ):
        self.factory = factory
        self.min_size = min_size
        self.max_size = max_size
        self.max_overflow = max_overflow
        self.timeout = timeout
        self.max_lifetime = max_lifetime
        self.max_idle_time = max_idle_time
        self.validation_interval = validation_interval
        
        self._pool: queue.Queue = queue.Queue(maxsize=max_size)
        self._overflow_connections: weakref.WeakSet = weakref.WeakSet()
        self._all_connections: List[PooledConnection] = []
        self._lock = threading.RLock()
        self._stats = PoolStats()
        self._closed = False
        
        # Validation thread
        self._stop_validation = threading.Event()
        self._validation_thread = threading.Thread(
            target=self._validation_loop,
            daemon=True,
            name="ConnectionPool-Validator"
        )
        
        # Initialize pool with minimum connections
        self._initialize_pool()
        
        # Start validation thread
        self._validation_thread.start()
    
    def _initialize_pool(self):
        """Create initial connections."""
        for _ in range(self.min_size):
            try:
                conn = self._create_connection()
                self._pool.put(conn, block=False)
            except Exception as e:
                logger.error(f"Failed to create initial connection: {e}")
                self._stats.failed_connections += 1
    
    def _create_connection(self) -> PooledConnection:
        """Create a new pooled connection."""
        start_time = time.time()
        
        try:
            raw_conn = self.factory.create_connection()
            pooled_conn = PooledConnection(
                connection=raw_conn,
                pool_ref=weakref.ref(self)
            )
            
            with self._lock:
                self._all_connections.append(pooled_conn)
                self._stats.created_connections += 1
            
            logger.debug(f"Created new connection (total: {len(self._all_connections)})")
            return pooled_conn
            
        except Exception as e:
            self._stats.failed_connections += 1
            raise
    
    @contextmanager
    def get_connection(self) -> ContextManager[Any]:
        """Get a connection from the pool."""
        if self._closed:
            raise RuntimeError("Connection pool is closed")
        
        start_time = time.time()
        self._stats.total_requests += 1
        pooled_conn = None
        from_overflow = False
        
        try:
            # Try to get from pool
            try:
                pooled_conn = self._pool.get(timeout=self.timeout)
                self._stats.cache_hits += 1
            except queue.Empty:
                # Pool is empty, try to create overflow connection
                with self._lock:
                    current_total = len(self._all_connections)
                    if current_total < self.max_size + self.max_overflow:
                        pooled_conn = self._create_connection()
                        from_overflow = True
                    else:
                        self._stats.timeouts += 1
                        raise TimeoutError(f"Connection pool timeout after {self.timeout}s")
            
            # Validate connection
            if not self.factory.validate_connection(pooled_conn.connection):
                logger.debug("Connection validation failed, creating new connection")
                self._close_connection(pooled_conn)
                pooled_conn = self._create_connection()
            
            # Update stats
            wait_time = (time.time() - start_time) * 1000
            self._stats.wait_time_ms += wait_time
            self._stats.active_connections += 1
            self._stats.idle_connections = self._pool.qsize()
            
            pooled_conn.update_usage()
            
            # Yield the raw connection
            yield pooled_conn.connection
            
        except Exception:
            # If we got a connection but failed, don't return it to pool
            if pooled_conn and not from_overflow:
                self._close_connection(pooled_conn)
                pooled_conn = None
            raise
            
        finally:
            # Return connection to pool
            if pooled_conn:
                self._stats.active_connections -= 1
                
                if from_overflow:
                    # Overflow connection, close it
                    self._close_connection(pooled_conn)
                else:
                    # Return to pool if healthy and not expired
                    if (not pooled_conn.is_expired(self.max_lifetime) and
                        self.factory.validate_connection(pooled_conn.connection)):
                        try:
                            self._pool.put(pooled_conn, block=False)
                            self._stats.idle_connections = self._pool.qsize()
                        except queue.Full:
                            # Pool is full, close connection
                            self._close_connection(pooled_conn)
                    else:
                        self._close_connection(pooled_conn)
    
    def _close_connection(self, pooled_conn: PooledConnection):
        """Close a pooled connection."""
        try:
            self.factory.close_connection(pooled_conn.connection)
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
        
        with self._lock:
            if pooled_conn in self._all_connections:
                self._all_connections.remove(pooled_conn)
    
    def _validation_loop(self):
        """Background thread to validate idle connections."""
        while not self._stop_validation.wait(self.validation_interval):
            self._validate_idle_connections()
    
    def _validate_idle_connections(self):
        """Validate and clean up idle connections."""
        validated = []
        to_close = []
        
        # Get all idle connections
        while True:
            try:
                conn = self._pool.get_nowait()
                
                # Check if connection is still valid
                if (conn.is_expired(self.max_lifetime) or
                    conn.is_idle_too_long(self.max_idle_time) or
                    not self.factory.validate_connection(conn.connection)):
                    to_close.append(conn)
                else:
                    validated.append(conn)
                    
            except queue.Empty:
                break
        
        # Close invalid connections
        for conn in to_close:
            self._close_connection(conn)
        
        # Return valid connections to pool
        for conn in validated:
            try:
                self._pool.put(conn, block=False)
            except queue.Full:
                self._close_connection(conn)
        
        # Ensure minimum pool size
        with self._lock:
            current_size = self._pool.qsize()
            if current_size < self.min_size:
                for _ in range(self.min_size - current_size):
                    try:
                        conn = self._create_connection()
                        self._pool.put(conn, block=False)
                    except Exception as e:
                        logger.error(f"Failed to maintain minimum pool size: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics."""
        return self._stats.to_dict()
    
    def close(self):
        """Close the connection pool."""
        self._closed = True
        self._stop_validation.set()
        
        # Close all connections
        connections_to_close = []
        
        # Get idle connections
        while True:
            try:
                conn = self._pool.get_nowait()
                connections_to_close.append(conn)
            except queue.Empty:
                break
        
        # Get active connections
        with self._lock:
            connections_to_close.extend(self._all_connections)
        
        # Close all
        for conn in connections_to_close:
            self._close_connection(conn)
        
        # Wait for validation thread
        if self._validation_thread.is_alive():
            self._validation_thread.join(timeout=5)


# Database-specific implementations

class PostgreSQLConnectionFactory(ConnectionFactory):
    """Connection factory for PostgreSQL."""
    
    def __init__(self, **connection_params):
        self.connection_params = connection_params
        
        try:
            import psycopg2
            self.psycopg2 = psycopg2
        except ImportError:
            raise ImportError("psycopg2 is required for PostgreSQL. Install with: pip install psycopg2-binary")
    
    def create_connection(self) -> Any:
        return self.psycopg2.connect(**self.connection_params)
    
    def validate_connection(self, connection: Any) -> bool:
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
        except:
            return False
    
    def close_connection(self, connection: Any) -> None:
        connection.close()


class MySQLConnectionFactory(ConnectionFactory):
    """Connection factory for MySQL."""
    
    def __init__(self, **connection_params):
        self.connection_params = connection_params
        
        try:
            import mysql.connector
            self.mysql = mysql.connector
        except ImportError:
            raise ImportError("mysql-connector-python is required. Install with: pip install mysql-connector-python")
    
    def create_connection(self) -> Any:
        return self.mysql.connect(**self.connection_params)
    
    def validate_connection(self, connection: Any) -> bool:
        try:
            connection.ping(reconnect=False, attempts=1)
            return True
        except:
            return False
    
    def close_connection(self, connection: Any) -> None:
        connection.close()


class SQLiteConnectionFactory(ConnectionFactory):
    """Connection factory for SQLite."""
    
    def __init__(self, database: str, **connection_params):
        import sqlite3
        self.sqlite3 = sqlite3
        self.database = database
        self.connection_params = connection_params
    
    def create_connection(self) -> Any:
        conn = self.sqlite3.connect(self.database, **self.connection_params)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn
    
    def validate_connection(self, connection: Any) -> bool:
        try:
            connection.execute("SELECT 1").fetchone()
            return True
        except:
            return False
    
    def close_connection(self, connection: Any) -> None:
        connection.close()


class MongoDBConnectionFactory(ConnectionFactory):
    """Connection factory for MongoDB."""
    
    def __init__(self, connection_string: str, **client_params):
        self.connection_string = connection_string
        self.client_params = client_params
        
        try:
            import pymongo
            self.pymongo = pymongo
        except ImportError:
            raise ImportError("pymongo is required. Install with: pip install pymongo")
    
    def create_connection(self) -> Any:
        return self.pymongo.MongoClient(self.connection_string, **self.client_params)
    
    def validate_connection(self, connection: Any) -> bool:
        try:
            connection.admin.command('ping')
            return True
        except:
            return False
    
    def close_connection(self, connection: Any) -> None:
        connection.close()


# Pooled backend wrapper
class PooledBackend:
    """Wrapper to add connection pooling to any backend."""
    
    def __init__(self, backend_class, pool: GenericConnectionPool, *args, **kwargs):
        self.backend_class = backend_class
        self.pool = pool
        self.backend_args = args
        self.backend_kwargs = kwargs
    
    def __getattr__(self, name):
        """Proxy method calls to backend with pooled connection."""
        def method(*args, **kwargs):
            with self.pool.get_connection() as conn:
                # Create temporary backend instance with pooled connection
                backend = self.backend_class(
                    *self.backend_args,
                    connection=conn,
                    **self.backend_kwargs
                )
                return getattr(backend, name)(*args, **kwargs)
        
        return method
    
    def close(self):
        """Close the connection pool."""
        self.pool.close()