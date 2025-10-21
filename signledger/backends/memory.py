"""In-memory storage backend for SignLedger."""

from typing import Optional, List, Iterator, Dict, Any
from datetime import datetime
import threading

from .base import StorageBackend
from ..core.exceptions import StorageError


class MemoryBackend(StorageBackend):
    """Simple in-memory storage backend for testing."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._entries = []
        self._entries_by_id = {}
        self._lock = threading.RLock()
        self._next_sequence = 0

    def append_entry(self, entry: Any) -> None:
        """Append an entry to the ledger."""
        with self._lock:
            # Add sequence number
            if hasattr(entry, 'sequence'):
                entry.sequence = self._next_sequence

            self._entries.append(entry)

            # Store by ID if available
            if hasattr(entry, 'entry_id'):
                self._entries_by_id[entry.entry_id] = entry

            self._next_sequence += 1

    def get_entry(self, entry_id: str) -> Optional[Any]:
        """Get entry by ID."""
        with self._lock:
            return self._entries_by_id.get(entry_id)

    def get_entries(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> Iterator[Any]:
        """Get entries within time range."""
        with self._lock:
            entries = self._entries[offset:]

            # Filter by time if needed
            if start_time or end_time:
                filtered = []
                for entry in entries:
                    if hasattr(entry, 'timestamp'):
                        if start_time and entry.timestamp < start_time:
                            continue
                        if end_time and entry.timestamp > end_time:
                            continue
                    filtered.append(entry)
                entries = filtered

            # Apply limit
            if limit:
                entries = entries[:limit]

            for entry in entries:
                yield entry

    def get_all_entries(self) -> List[Any]:
        """Get all entries."""
        with self._lock:
            return list(self._entries)

    def get_latest_entry(self) -> Optional[Any]:
        """Get the latest entry."""
        with self._lock:
            return self._entries[-1] if self._entries else None

    def get_entry_count(self) -> int:
        """Get total number of entries."""
        with self._lock:
            return len(self._entries)

    def clear(self) -> None:
        """Clear all entries."""
        with self._lock:
            self._entries.clear()
            self._entries_by_id.clear()
            self._next_sequence = 0

    def close(self) -> None:
        """Close the backend."""
        pass

    # Additional methods for compatibility
    def append(self, entry_data: Dict[str, Any]) -> int:
        """Append entry and return sequence number."""
        with self._lock:
            sequence = self._next_sequence
            entry_data['sequence'] = sequence
            self._entries.append(entry_data)
            self._next_sequence += 1
            return sequence

    def get(self, sequence: int) -> Optional[Dict[str, Any]]:
        """Get entry by sequence number."""
        with self._lock:
            if 0 <= sequence < len(self._entries):
                return self._entries[sequence]
            return None

    def get_latest(self) -> Optional[Dict[str, Any]]:
        """Get latest entry."""
        with self._lock:
            return self._entries[-1] if self._entries else None

    def get_range(self, start: int, end: int) -> List[Dict[str, Any]]:
        """Get range of entries by sequence."""
        with self._lock:
            return self._entries[start:end + 1]