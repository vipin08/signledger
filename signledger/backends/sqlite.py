"""SQLite backend for SignLedger."""

import sqlite3
import json
from datetime import datetime
from typing import Optional, Iterator, Dict, Any
from pathlib import Path
import logging
import threading

from .base import StorageBackend
from ..core.exceptions import StorageError
from ..core.ledger import Entry

logger = logging.getLogger(__name__)


class SQLiteBackend(StorageBackend):
    """SQLite storage backend for SignLedger."""
    
    def __init__(
        self,
        db_path: str = "signledger.db",
        table_name: str = "ledger_entries",
        wal_mode: bool = True,
        **kwargs
    ):
        super().__init__(**kwargs)
        
        self.db_path = db_path
        self.table_name = table_name
        self.wal_mode = wal_mode
        
        # Thread-local storage for connections
        self._local = threading.local()
        
        # Create database and tables
        self._init_database()
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local database connection."""
        if not hasattr(self._local, 'connection') or self._local.connection is None:
            self._local.connection = sqlite3.connect(
                self.db_path,
                isolation_level=None,  # Autocommit mode
                check_same_thread=False
            )
            self._local.connection.row_factory = sqlite3.Row
            
            # Enable foreign keys
            self._local.connection.execute("PRAGMA foreign_keys = ON")
            
            # Set WAL mode for better concurrency
            if self.wal_mode:
                self._local.connection.execute("PRAGMA journal_mode = WAL")
            
            # Optimize for performance
            self._local.connection.execute("PRAGMA synchronous = NORMAL")
            self._local.connection.execute("PRAGMA cache_size = -64000")  # 64MB
            self._local.connection.execute("PRAGMA temp_store = MEMORY")
        
        return self._local.connection
    
    def _init_database(self):
        """Initialize database schema."""
        conn = self._get_connection()
        
        # Create table
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                data TEXT NOT NULL,
                hash TEXT NOT NULL UNIQUE,
                previous_hash TEXT,
                signature TEXT,
                metadata TEXT,
                nonce INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes
        conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_timestamp 
            ON {self.table_name} (timestamp DESC)
        """)
        conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_hash 
            ON {self.table_name} (hash)
        """)
        conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_previous_hash 
            ON {self.table_name} (previous_hash)
        """)
        
        conn.commit()
    
    def append_entry(self, entry: Entry) -> None:
        """Append entry to the ledger."""
        conn = self._get_connection()
        
        try:
            conn.execute(f"""
                INSERT INTO {self.table_name} 
                (id, timestamp, data, hash, previous_hash, signature, metadata, nonce)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                entry.id,
                entry.timestamp.isoformat(),
                json.dumps(entry.data),
                entry.hash,
                entry.previous_hash,
                entry.signature,
                json.dumps(entry.metadata) if entry.metadata else None,
                entry.nonce,
            ))
            
        except sqlite3.IntegrityError as e:
            if 'UNIQUE constraint failed' in str(e):
                raise StorageError(
                    f"Entry with ID {entry.id} already exists",
                    "append",
                    self.name
                )
            raise StorageError(f"Failed to append entry: {e}", "append", self.name)
        except Exception as e:
            raise StorageError(f"Failed to append entry: {e}", "append", self.name)
    
    def get_entry(self, entry_id: str) -> Optional[Entry]:
        """Get entry by ID."""
        conn = self._get_connection()
        
        cursor = conn.execute(f"""
            SELECT * FROM {self.table_name} WHERE id = ?
        """, (entry_id,))
        
        row = cursor.fetchone()
        
        if row:
            return self._row_to_entry(row)
        
        return None
    
    def get_entries(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> Iterator[Entry]:
        """Get entries within time range."""
        conn = self._get_connection()
        
        # Build query
        query = f"SELECT * FROM {self.table_name}"
        params = []
        conditions = []
        
        if start_time:
            conditions.append("timestamp >= ?")
            params.append(start_time.isoformat())
        
        if end_time:
            conditions.append("timestamp <= ?")
            params.append(end_time.isoformat())
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY timestamp ASC"
        
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        
        if offset:
            query += " OFFSET ?"
            params.append(offset)
        
        cursor = conn.execute(query, params)
        
        for row in cursor:
            yield self._row_to_entry(row)
    
    def get_latest_entry(self) -> Optional[Entry]:
        """Get the most recent entry."""
        conn = self._get_connection()
        
        cursor = conn.execute(f"""
            SELECT * FROM {self.table_name} 
            ORDER BY timestamp DESC LIMIT 1
        """)
        
        row = cursor.fetchone()
        
        if row:
            return self._row_to_entry(row)
        
        return None
    
    def get_oldest_entry(self) -> Optional[Entry]:
        """Get the oldest entry."""
        conn = self._get_connection()
        
        cursor = conn.execute(f"""
            SELECT * FROM {self.table_name} 
            ORDER BY timestamp ASC LIMIT 1
        """)
        
        row = cursor.fetchone()
        
        if row:
            return self._row_to_entry(row)
        
        return None
    
    def count_entries(self) -> int:
        """Get total number of entries."""
        conn = self._get_connection()
        
        cursor = conn.execute(f"SELECT COUNT(*) FROM {self.table_name}")
        return cursor.fetchone()[0]
    
    def get_size(self) -> int:
        """Get approximate storage size in bytes."""
        # Get file size
        if Path(self.db_path).exists():
            db_size = Path(self.db_path).stat().st_size
            
            # Include WAL file if it exists
            wal_path = Path(f"{self.db_path}-wal")
            if wal_path.exists():
                db_size += wal_path.stat().st_size
            
            return db_size
        
        return 0
    
    def close(self) -> None:
        """Close database connection."""
        if hasattr(self._local, 'connection') and self._local.connection:
            self._local.connection.close()
            self._local.connection = None
    
    def verify_storage(self) -> bool:
        """Verify storage integrity."""
        try:
            conn = self._get_connection()
            
            # Run integrity check
            cursor = conn.execute("PRAGMA integrity_check")
            result = cursor.fetchone()[0]
            
            if result != "ok":
                logger.error(f"SQLite integrity check failed: {result}")
                return False
            
            # Check if table exists
            cursor = conn.execute(f"""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (self.table_name,))
            
            if not cursor.fetchone():
                logger.error(f"Table {self.table_name} does not exist")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Storage verification failed: {e}")
            return False
    
    def _row_to_entry(self, row) -> Entry:
        """Convert database row to Entry object."""
        data = dict(row)
        
        # Parse JSON fields
        data['data'] = json.loads(data['data'])
        if data['metadata']:
            data['metadata'] = json.loads(data['metadata'])
        
        # Parse timestamp
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        
        # Remove SQLite-specific fields
        data.pop('created_at', None)
        
        return Entry.from_dict(data)
    
    # SQLite-specific methods
    
    def vacuum(self):
        """Vacuum database to reclaim space."""
        conn = self._get_connection()
        conn.execute("VACUUM")
    
    def backup(self, backup_path: str):
        """Create backup of the database."""
        conn = self._get_connection()
        backup_conn = sqlite3.connect(backup_path)
        
        with backup_conn:
            conn.backup(backup_conn)
        
        backup_conn.close()
    
    def analyze(self):
        """Update SQLite statistics for query optimization."""
        conn = self._get_connection()
        conn.execute("ANALYZE")