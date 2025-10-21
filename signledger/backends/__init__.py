"""Storage backend implementations for SignLedger."""

from .base import StorageBackend, InMemoryBackend

# Lazy imports to avoid circular dependencies
def __getattr__(name):
    if name == 'MemoryBackend':
        from .memory import MemoryBackend
        return MemoryBackend
    elif name == 'SQLiteBackend':
        from .sqlite import SQLiteBackend
        return SQLiteBackend
    elif name == 'PostgreSQLBackend':
        try:
            from .postgresql import PostgreSQLBackend
            return PostgreSQLBackend
        except ImportError:
            return None
    elif name == 'MongoDBBackend':
        try:
            from .mongodb import MongoDBBackend
            return MongoDBBackend
        except ImportError:
            return None
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

__all__ = [
    'StorageBackend',
    'InMemoryBackend',
    'MemoryBackend',
    'SQLiteBackend',
    'PostgreSQLBackend',
    'MongoDBBackend',
]