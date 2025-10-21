"""Base storage backend interface for SignLedger."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, List, Iterator, Dict, Any
import json

from ..core.exceptions import StorageError


class StorageBackend(ABC):
    """Abstract base class for ledger storage backends."""
    
    def __init__(self, **kwargs):
        self.name = self.__class__.__name__
        self._config = kwargs
    
    @abstractmethod
    def append_entry(self, entry: Any) -> None:
        """Append an entry to the ledger.
        
        Args:
            entry: Entry to append
            
        Raises:
            StorageError: If append fails
        """
        pass
    
    @abstractmethod
    def get_entry(self, entry_id: str) -> Optional[Any]:
        """Get entry by ID.
        
        Args:
            entry_id: Entry ID
            
        Returns:
            Entry if found, None otherwise
        """
        pass
    
    @abstractmethod
    def get_entries(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> Iterator[Any]:
        """Get entries within time range.
        
        Args:
            start_time: Start time filter
            end_time: End time filter
            limit: Maximum number of entries
            offset: Number of entries to skip
            
        Yields:
            Matching entries
        """
        pass
    
    @abstractmethod
    def get_latest_entry(self) -> Optional[Any]:
        """Get the most recent entry.
        
        Returns:
            Latest entry if exists, None otherwise
        """
        pass
    
    @abstractmethod
    def get_oldest_entry(self) -> Optional[Any]:
        """Get the oldest entry.
        
        Returns:
            Oldest entry if exists, None otherwise
        """
        pass
    
    @abstractmethod
    def count_entries(self) -> int:
        """Get total number of entries.
        
        Returns:
            Total entry count
        """
        pass
    
    @abstractmethod
    def get_size(self) -> int:
        """Get approximate storage size in bytes.
        
        Returns:
            Storage size in bytes
        """
        pass
    
    def close(self) -> None:
        """Close storage connection."""
        pass
    
    def create_indexes(self) -> None:
        """Create necessary indexes for performance."""
        pass
    
    def verify_storage(self) -> bool:
        """Verify storage integrity.
        
        Returns:
            True if storage is healthy
        """
        return True


class InMemoryBackend(StorageBackend):
    """In-memory storage backend for testing and development."""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._entries: Dict[str, Any] = {}
        self._ordered_ids: List[str] = []
        self._size = 0
    
    def append_entry(self, entry: Any) -> None:
        """Append an entry to memory storage."""
        if hasattr(entry, 'id'):
            entry_id = entry.id
        else:
            entry_id = entry.get('id')
        
        if not entry_id:
            raise StorageError("Entry must have an ID", "append", self.name)
        
        if entry_id in self._entries:
            raise StorageError(f"Entry {entry_id} already exists", "append", self.name)
        
        self._entries[entry_id] = entry
        self._ordered_ids.append(entry_id)
        
        # Update size estimate
        if hasattr(entry, 'to_json'):
            self._size += len(entry.to_json())
        else:
            self._size += len(json.dumps(entry, default=str))
    
    def get_entry(self, entry_id: str) -> Optional[Any]:
        """Get entry by ID from memory."""
        return self._entries.get(entry_id)
    
    def get_entries(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> Iterator[Any]:
        """Get entries within time range from memory."""
        count = 0
        returned = 0
        
        for entry_id in self._ordered_ids:
            entry = self._entries[entry_id]
            
            # Skip offset
            if count < offset:
                count += 1
                continue
            
            # Check time range
            entry_time = entry.timestamp if hasattr(entry, 'timestamp') else entry.get('timestamp')
            
            if start_time and entry_time < start_time:
                continue
            
            if end_time and entry_time > end_time:
                continue
            
            yield entry
            returned += 1
            
            # Check limit
            if limit and returned >= limit:
                break
    
    def get_latest_entry(self) -> Optional[Any]:
        """Get the most recent entry from memory."""
        if self._ordered_ids:
            return self._entries[self._ordered_ids[-1]]
        return None
    
    def get_oldest_entry(self) -> Optional[Any]:
        """Get the oldest entry from memory."""
        if self._ordered_ids:
            return self._entries[self._ordered_ids[0]]
        return None
    
    def count_entries(self) -> int:
        """Get total number of entries in memory."""
        return len(self._entries)
    
    def get_size(self) -> int:
        """Get approximate storage size in bytes."""
        return self._size
    
    def clear(self) -> None:
        """Clear all entries (for testing)."""
        self._entries.clear()
        self._ordered_ids.clear()
        self._size = 0