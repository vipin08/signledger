"""Core ledger implementation for SignLedger."""

import asyncio
import json
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Callable, Union, Iterator
from collections import OrderedDict
import logging

from pydantic import BaseModel, Field, field_validator

from .exceptions import IntegrityError, ValidationError, StorageError
from ..crypto.hashing import HashChain
from ..backends.base import StorageBackend, InMemoryBackend

logger = logging.getLogger(__name__)


class Entry(BaseModel):
    """Immutable ledger entry."""

    model_config = {"frozen": True}  # Make entries immutable after creation

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    data: Dict[str, Any]
    hash: Optional[str] = None
    previous_hash: Optional[str] = None
    signature: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    nonce: int = 0

    @field_validator("data")
    @classmethod
    def validate_data(cls, v):
        """Ensure data is not empty."""
        if not v:
            raise ValueError("Entry data cannot be empty")
        return v
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert entry to dictionary."""
        return {
            "id": self.id,
            "timestamp": self.timestamp.isoformat(),
            "data": self.data,
            "hash": self.hash,
            "previous_hash": self.previous_hash,
            "signature": self.signature,
            "metadata": self.metadata,
            "nonce": self.nonce,
        }
    
    def to_json(self) -> str:
        """Convert entry to JSON string."""
        return json.dumps(self.to_dict(), sort_keys=True)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Entry":
        """Create entry from dictionary."""
        if isinstance(data.get("timestamp"), str):
            data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        return cls(**data)


class LedgerStats(BaseModel):
    """Ledger statistics."""
    
    total_entries: int = 0
    first_entry_time: Optional[datetime] = None
    last_entry_time: Optional[datetime] = None
    total_size_bytes: int = 0
    hash_algorithm: str = "sha256"
    integrity_verified: bool = True
    last_verification_time: Optional[datetime] = None


class Ledger:
    """Main ledger class for SignLedger."""
    
    def __init__(
        self,
        backend: Optional[StorageBackend] = None,
        hash_algorithm: str = "sha256",
        enable_signatures: bool = False,
        auto_verify: bool = True,
        verify_interval: int = 3600,  # seconds
        max_entries_memory: int = 1000,
    ):
        self.backend = backend or InMemoryBackend()
        self.hash_chain = HashChain(algorithm=hash_algorithm)
        self.enable_signatures = enable_signatures
        self.auto_verify = auto_verify
        self.verify_interval = verify_interval
        self.max_entries_memory = max_entries_memory
        
        # Thread safety
        self._lock = threading.RLock()
        self._write_lock = threading.Lock()
        
        # Cache recent entries
        self._cache = OrderedDict()
        self._cache_size = 0
        
        # Verification thread
        self._verify_thread: Optional[threading.Thread] = None
        self._stop_verify = threading.Event()
        
        # Subscribers
        self._subscribers: List[Callable[[Entry], None]] = []
        
        # Initialize
        self._initialize()
    
    def _initialize(self) -> None:
        """Initialize the ledger."""
        # Start verification thread if enabled
        if self.auto_verify:
            self._start_verification_thread()
        
        # Load latest entries into cache
        self._load_cache()
    
    def _load_cache(self) -> None:
        """Load recent entries into cache."""
        try:
            entries = list(self.backend.get_entries(limit=self.max_entries_memory))
            for entry in reversed(entries):  # Add in chronological order
                self._add_to_cache(entry)
        except Exception as e:
            logger.warning(f"Failed to load cache: {e}")
    
    def _add_to_cache(self, entry: Entry) -> None:
        """Add entry to cache with size limit."""
        self._cache[entry.id] = entry
        self._cache_size += len(entry.to_json())
        
        # Remove oldest entries if cache is too large
        while len(self._cache) > self.max_entries_memory:
            removed_id, removed_entry = self._cache.popitem(last=False)
            self._cache_size -= len(removed_entry.to_json())
    
    def _start_verification_thread(self) -> None:
        """Start background verification thread."""
        if self._verify_thread and self._verify_thread.is_alive():
            return
        
        self._stop_verify.clear()
        self._verify_thread = threading.Thread(
            target=self._verification_loop,
            daemon=True
        )
        self._verify_thread.start()
    
    def _verification_loop(self) -> None:
        """Background verification loop."""
        while not self._stop_verify.wait(self.verify_interval):
            try:
                logger.info("Running scheduled integrity verification...")
                self.verify_integrity()
            except Exception as e:
                logger.error(f"Scheduled verification failed: {e}")
    
    def append(
        self,
        data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
        sign: bool = False,
        signer: Optional[Callable[[str], str]] = None,
    ) -> Entry:
        """Append a new entry to the ledger.
        
        Args:
            data: Entry data to append
            metadata: Optional metadata
            sign: Whether to sign the entry
            signer: Optional signing function
            
        Returns:
            The created entry
            
        Raises:
            ValidationError: If entry validation fails
            StorageError: If storage operation fails
        """
        with self._write_lock:
            # Get previous hash
            previous_hash = None
            try:
                last_entry = self.backend.get_latest_entry()
                if last_entry:
                    previous_hash = last_entry.hash
            except Exception as e:
                logger.warning(f"Failed to get last entry: {e}")
            
            # Create entry
            entry = Entry(
                data=data,
                metadata=metadata or {},
                previous_hash=previous_hash,
            )
            
            # Calculate hash
            entry_dict = entry.to_dict()
            entry_dict["hash"] = self.hash_chain.calculate_hash(entry)
            
            # Sign if requested or if signatures are enabled
            if sign or self.enable_signatures:
                if not signer:
                    raise ValidationError("Signer function required for signing")
                entry_dict["signature"] = signer(entry_dict["hash"])
            
            # Create final entry
            final_entry = Entry.from_dict(entry_dict)
            
            # Store entry
            try:
                self.backend.append_entry(final_entry)
            except Exception as e:
                raise StorageError(f"Failed to append entry: {e}", "append", self.backend.name)
            
            # Update cache
            with self._lock:
                self._add_to_cache(final_entry)
            
            # Notify subscribers
            self._notify_subscribers(final_entry)
            
            return final_entry
    
    def get_entry(self, entry_id: str) -> Optional[Entry]:
        """Get entry by ID.
        
        Args:
            entry_id: Entry ID
            
        Returns:
            Entry if found, None otherwise
        """
        # Check cache first
        with self._lock:
            if entry_id in self._cache:
                return self._cache[entry_id]
        
        # Fetch from backend
        try:
            return self.backend.get_entry(entry_id)
        except Exception as e:
            logger.error(f"Failed to get entry {entry_id}: {e}")
            return None
    
    def get_entries(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> Iterator[Entry]:
        """Get entries within time range.
        
        Args:
            start_time: Start time filter
            end_time: End time filter
            limit: Maximum number of entries
            offset: Number of entries to skip
            
        Yields:
            Matching entries
        """
        return self.backend.get_entries(
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            offset=offset,
        )
    
    def query(
        self,
        filter_func: Callable[[Entry], bool],
        limit: Optional[int] = None,
    ) -> List[Entry]:
        """Query entries with custom filter.
        
        Args:
            filter_func: Function to filter entries
            limit: Maximum number of results
            
        Returns:
            List of matching entries
        """
        results = []
        for entry in self.get_entries():
            if filter_func(entry):
                results.append(entry)
                if limit and len(results) >= limit:
                    break
        return results
    
    def verify_integrity(
        self,
        start_entry: Optional[str] = None,
        end_entry: Optional[str] = None,
    ) -> bool:
        """Verify ledger integrity.
        
        Args:
            start_entry: Start from this entry ID (None = beginning)
            end_entry: End at this entry ID (None = end)
            
        Returns:
            True if integrity is valid
            
        Raises:
            IntegrityError: If integrity check fails
        """
        logger.info("Starting integrity verification...")
        
        previous_hash = None
        entry_count = 0
        
        # Get entry iterator
        entries = self.backend.get_entries()
        
        # Skip to start entry if specified
        if start_entry:
            for entry in entries:
                if entry.id == start_entry:
                    break
                previous_hash = entry.hash
        
        # Verify each entry
        for entry in entries:
            entry_count += 1
            
            # Verify previous hash link
            if entry.previous_hash != previous_hash:
                raise IntegrityError(
                    f"Hash chain broken at entry {entry.id}",
                    entry_id=entry.id,
                    expected_hash=previous_hash,
                    actual_hash=entry.previous_hash,
                )
            
            # Verify entry hash
            calculated_hash = self.hash_chain.calculate_hash(entry)
            if entry.hash != calculated_hash:
                raise IntegrityError(
                    f"Invalid hash for entry {entry.id}",
                    entry_id=entry.id,
                    expected_hash=calculated_hash,
                    actual_hash=entry.hash,
                )
            
            # Update for next iteration
            previous_hash = entry.hash
            
            # Stop at end entry if specified
            if end_entry and entry.id == end_entry:
                break
        
        logger.info(f"Integrity verification complete. Verified {entry_count} entries.")
        return True
    
    def verify_entry(self, entry_id: str) -> bool:
        """Verify a single entry.
        
        Args:
            entry_id: Entry ID to verify
            
        Returns:
            True if entry is valid
        """
        entry = self.get_entry(entry_id)
        if not entry:
            return False
        
        # Verify hash
        calculated_hash = self.hash_chain.calculate_hash(entry)
        if entry.hash != calculated_hash:
            return False
        
        # Verify chain if not first entry
        if entry.previous_hash:
            # Find previous entry
            found_previous = False
            for e in self.get_entries(end_time=entry.timestamp):
                if e.hash == entry.previous_hash:
                    found_previous = True
                    break
            
            if not found_previous:
                return False
        
        return True
    
    def get_stats(self) -> LedgerStats:
        """Get ledger statistics.
        
        Returns:
            Ledger statistics
        """
        stats = LedgerStats(hash_algorithm=self.hash_chain.algorithm)
        
        try:
            # Get total entries
            stats.total_entries = self.backend.count_entries()
            
            # Get time range
            first_entry = self.backend.get_oldest_entry()
            if first_entry:
                stats.first_entry_time = first_entry.timestamp
            
            last_entry = self.backend.get_latest_entry()
            if last_entry:
                stats.last_entry_time = last_entry.timestamp
            
            # Estimate size
            stats.total_size_bytes = self.backend.get_size()
            
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
        
        return stats
    
    def subscribe(self, callback: Callable[[Entry], None]) -> None:
        """Subscribe to new entries.
        
        Args:
            callback: Function called with new entries
        """
        self._subscribers.append(callback)
    
    def unsubscribe(self, callback: Callable[[Entry], None]) -> None:
        """Unsubscribe from new entries.
        
        Args:
            callback: Callback to remove
        """
        if callback in self._subscribers:
            self._subscribers.remove(callback)
    
    def _notify_subscribers(self, entry: Entry) -> None:
        """Notify subscribers of new entry."""
        for callback in self._subscribers:
            try:
                callback(entry)
            except Exception as e:
                logger.error(f"Subscriber callback failed: {e}")
    
    def close(self) -> None:
        """Close the ledger and cleanup resources."""
        # Stop verification thread
        if self._verify_thread:
            self._stop_verify.set()
            self._verify_thread.join(timeout=5)
        
        # Close backend
        self.backend.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
    
    # Async methods for frameworks like FastAPI
    
    async def append_async(
        self,
        data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
        sign: bool = False,
        signer: Optional[Callable[[str], str]] = None,
    ) -> Entry:
        """Async version of append."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.append,
            data,
            metadata,
            sign,
            signer,
        )
    
    async def get_entry_async(self, entry_id: str) -> Optional[Entry]:
        """Async version of get_entry."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.get_entry, entry_id)
    
    async def verify_integrity_async(
        self,
        start_entry: Optional[str] = None,
        end_entry: Optional[str] = None,
    ) -> bool:
        """Async version of verify_integrity."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.verify_integrity,
            start_entry,
            end_entry,
        )