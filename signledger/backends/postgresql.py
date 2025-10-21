"""PostgreSQL backend for SignLedger."""

from datetime import datetime
from typing import Optional, Iterator, Dict, Any
import json
import logging

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor, Json
    from psycopg2.pool import SimpleConnectionPool
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

try:
    from sqlalchemy import create_engine, MetaData, Table, Column, String, DateTime, Integer, JSON, Index, text
    from sqlalchemy.dialects.postgresql import UUID
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False

from .base import StorageBackend
from ..core.exceptions import StorageError
from ..core.ledger import Entry

logger = logging.getLogger(__name__)


class PostgreSQLBackend(StorageBackend):
    """PostgreSQL storage backend for SignLedger."""
    
    def __init__(
        self,
        connection_string: str,
        table_name: str = "ledger_entries",
        pool_size: int = 5,
        max_overflow: int = 10,
        use_sqlalchemy: bool = False,
        **kwargs
    ):
        super().__init__(**kwargs)
        
        if use_sqlalchemy and not HAS_SQLALCHEMY:
            raise ImportError("SQLAlchemy is required. Install with: pip install signledger[postgresql]")
        elif not use_sqlalchemy and not HAS_PSYCOPG2:
            raise ImportError("psycopg2 is required. Install with: pip install signledger[postgresql]")
        
        self.connection_string = connection_string
        self.table_name = table_name
        self.use_sqlalchemy = use_sqlalchemy
        
        if use_sqlalchemy:
            self._setup_sqlalchemy(pool_size, max_overflow)
        else:
            self._setup_psycopg2(pool_size)
        
        # Create table if it doesn't exist
        self.create_indexes()
    
    def _setup_psycopg2(self, pool_size: int):
        """Setup psycopg2 connection pool."""
        self._pool = SimpleConnectionPool(
            1,
            pool_size,
            self.connection_string
        )
    
    def _setup_sqlalchemy(self, pool_size: int, max_overflow: int):
        """Setup SQLAlchemy engine and metadata."""
        self._engine = create_engine(
            self.connection_string,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True,
            json_serializer=lambda obj: json.dumps(obj, default=str),
            json_deserializer=json.loads,
        )
        
        self._metadata = MetaData()
        self._table = Table(
            self.table_name,
            self._metadata,
            Column('id', UUID(as_uuid=False), primary_key=True),
            Column('timestamp', DateTime(timezone=True), nullable=False, index=True),
            Column('data', JSON, nullable=False),
            Column('hash', String(64), nullable=False, unique=True, index=True),
            Column('previous_hash', String(64), index=True),
            Column('signature', String),
            Column('metadata', JSON),
            Column('nonce', Integer, default=0),
            Column('created_at', DateTime(timezone=True), server_default=text('CURRENT_TIMESTAMP')),
            Index('idx_timestamp_desc', 'timestamp', postgresql_using='btree', postgresql_ops={'timestamp': 'DESC'}),
        )
    
    def _get_connection(self):
        """Get database connection."""
        if self.use_sqlalchemy:
            return self._engine.connect()
        else:
            return self._pool.getconn()
    
    def _put_connection(self, conn):
        """Return connection to pool."""
        if not self.use_sqlalchemy:
            self._pool.putconn(conn)
    
    def create_indexes(self) -> None:
        """Create table and indexes."""
        if self.use_sqlalchemy:
            self._metadata.create_all(self._engine)
        else:
            conn = self._get_connection()
            try:
                with conn.cursor() as cur:
                    # Create table
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {self.table_name} (
                            id UUID PRIMARY KEY,
                            timestamp TIMESTAMPTZ NOT NULL,
                            data JSONB NOT NULL,
                            hash VARCHAR(64) NOT NULL UNIQUE,
                            previous_hash VARCHAR(64),
                            signature TEXT,
                            metadata JSONB,
                            nonce INTEGER DEFAULT 0,
                            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    # Create indexes
                    cur.execute(f"""
                        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_timestamp 
                        ON {self.table_name} (timestamp DESC)
                    """)
                    cur.execute(f"""
                        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_hash 
                        ON {self.table_name} (hash)
                    """)
                    cur.execute(f"""
                        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_previous_hash 
                        ON {self.table_name} (previous_hash)
                    """)
                    
                    # Create GIN index for JSONB data
                    cur.execute(f"""
                        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_data_gin 
                        ON {self.table_name} USING GIN (data)
                    """)
                    
                    conn.commit()
            finally:
                self._put_connection(conn)
    
    def append_entry(self, entry: Entry) -> None:
        """Append entry to the ledger."""
        if self.use_sqlalchemy:
            self._append_entry_sqlalchemy(entry)
        else:
            self._append_entry_psycopg2(entry)
    
    def _append_entry_psycopg2(self, entry: Entry) -> None:
        """Append entry using psycopg2."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {self.table_name} 
                    (id, timestamp, data, hash, previous_hash, signature, metadata, nonce)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    entry.id,
                    entry.timestamp,
                    Json(entry.data),
                    entry.hash,
                    entry.previous_hash,
                    entry.signature,
                    Json(entry.metadata) if entry.metadata else None,
                    entry.nonce,
                ))
                conn.commit()
        except psycopg2.IntegrityError as e:
            conn.rollback()
            if 'duplicate key value violates unique constraint' in str(e):
                raise StorageError(f"Entry with ID {entry.id} already exists", "append", self.name)
            raise StorageError(f"Failed to append entry: {e}", "append", self.name)
        except Exception as e:
            conn.rollback()
            raise StorageError(f"Failed to append entry: {e}", "append", self.name)
        finally:
            self._put_connection(conn)
    
    def _append_entry_sqlalchemy(self, entry: Entry) -> None:
        """Append entry using SQLAlchemy."""
        with self._engine.begin() as conn:
            try:
                conn.execute(self._table.insert().values(
                    id=entry.id,
                    timestamp=entry.timestamp,
                    data=entry.data,
                    hash=entry.hash,
                    previous_hash=entry.previous_hash,
                    signature=entry.signature,
                    metadata=entry.metadata,
                    nonce=entry.nonce,
                ))
            except Exception as e:
                raise StorageError(f"Failed to append entry: {e}", "append", self.name)
    
    def get_entry(self, entry_id: str) -> Optional[Entry]:
        """Get entry by ID."""
        if self.use_sqlalchemy:
            return self._get_entry_sqlalchemy(entry_id)
        else:
            return self._get_entry_psycopg2(entry_id)
    
    def _get_entry_psycopg2(self, entry_id: str) -> Optional[Entry]:
        """Get entry using psycopg2."""
        conn = self._get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"""
                    SELECT * FROM {self.table_name} WHERE id = %s
                """, (entry_id,))
                row = cur.fetchone()
                
                if row:
                    return Entry.from_dict(self._row_to_dict(row))
                return None
        finally:
            self._put_connection(conn)
    
    def _get_entry_sqlalchemy(self, entry_id: str) -> Optional[Entry]:
        """Get entry using SQLAlchemy."""
        with self._engine.connect() as conn:
            result = conn.execute(
                self._table.select().where(self._table.c.id == entry_id)
            ).first()
            
            if result:
                return Entry.from_dict(self._row_to_dict(result))
            return None
    
    def get_entries(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> Iterator[Entry]:
        """Get entries within time range."""
        if self.use_sqlalchemy:
            yield from self._get_entries_sqlalchemy(start_time, end_time, limit, offset)
        else:
            yield from self._get_entries_psycopg2(start_time, end_time, limit, offset)
    
    def _get_entries_psycopg2(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> Iterator[Entry]:
        """Get entries using psycopg2."""
        conn = self._get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Build query
                query = f"SELECT * FROM {self.table_name}"
                params = []
                conditions = []
                
                if start_time:
                    conditions.append("timestamp >= %s")
                    params.append(start_time)
                
                if end_time:
                    conditions.append("timestamp <= %s")
                    params.append(end_time)
                
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
                
                query += " ORDER BY timestamp ASC"
                
                if limit:
                    query += " LIMIT %s"
                    params.append(limit)
                
                if offset:
                    query += " OFFSET %s"
                    params.append(offset)
                
                cur.execute(query, params)
                
                for row in cur:
                    yield Entry.from_dict(self._row_to_dict(row))
        finally:
            self._put_connection(conn)
    
    def _get_entries_sqlalchemy(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> Iterator[Entry]:
        """Get entries using SQLAlchemy."""
        with self._engine.connect() as conn:
            query = self._table.select()
            
            if start_time:
                query = query.where(self._table.c.timestamp >= start_time)
            
            if end_time:
                query = query.where(self._table.c.timestamp <= end_time)
            
            query = query.order_by(self._table.c.timestamp.asc())
            
            if limit:
                query = query.limit(limit)
            
            if offset:
                query = query.offset(offset)
            
            result = conn.execute(query)
            
            for row in result:
                yield Entry.from_dict(self._row_to_dict(row))
    
    def get_latest_entry(self) -> Optional[Entry]:
        """Get the most recent entry."""
        if self.use_sqlalchemy:
            with self._engine.connect() as conn:
                result = conn.execute(
                    self._table.select()
                    .order_by(self._table.c.timestamp.desc())
                    .limit(1)
                ).first()
                
                if result:
                    return Entry.from_dict(self._row_to_dict(result))
        else:
            conn = self._get_connection()
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(f"""
                        SELECT * FROM {self.table_name} 
                        ORDER BY timestamp DESC LIMIT 1
                    """)
                    row = cur.fetchone()
                    
                    if row:
                        return Entry.from_dict(self._row_to_dict(row))
            finally:
                self._put_connection(conn)
        
        return None
    
    def get_oldest_entry(self) -> Optional[Entry]:
        """Get the oldest entry."""
        if self.use_sqlalchemy:
            with self._engine.connect() as conn:
                result = conn.execute(
                    self._table.select()
                    .order_by(self._table.c.timestamp.asc())
                    .limit(1)
                ).first()
                
                if result:
                    return Entry.from_dict(self._row_to_dict(result))
        else:
            conn = self._get_connection()
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(f"""
                        SELECT * FROM {self.table_name} 
                        ORDER BY timestamp ASC LIMIT 1
                    """)
                    row = cur.fetchone()
                    
                    if row:
                        return Entry.from_dict(self._row_to_dict(row))
            finally:
                self._put_connection(conn)
        
        return None
    
    def count_entries(self) -> int:
        """Get total number of entries."""
        if self.use_sqlalchemy:
            with self._engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT COUNT(*) FROM {self.table_name}")
                ).scalar()
                return result or 0
        else:
            conn = self._get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                    return cur.fetchone()[0]
            finally:
                self._put_connection(conn)
    
    def get_size(self) -> int:
        """Get approximate storage size in bytes."""
        if self.use_sqlalchemy:
            with self._engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT pg_total_relation_size('{self.table_name}')")
                ).scalar()
                return result or 0
        else:
            conn = self._get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT pg_total_relation_size('{self.table_name}')")
                    return cur.fetchone()[0]
            finally:
                self._put_connection(conn)
    
    def close(self) -> None:
        """Close database connections."""
        if self.use_sqlalchemy:
            self._engine.dispose()
        else:
            self._pool.closeall()
    
    def _row_to_dict(self, row) -> Dict[str, Any]:
        """Convert database row to dictionary."""
        if isinstance(row, dict):
            # psycopg2 RealDictCursor
            result = dict(row)
        else:
            # SQLAlchemy row
            result = dict(row._mapping)
        
        # Remove created_at as it's not part of Entry
        result.pop('created_at', None)
        
        return result