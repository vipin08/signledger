"""Caching layer implementation for SignLedger."""

import time
import threading
import hashlib
import json
from typing import Any, Dict, Optional, List, Tuple, Callable
from collections import OrderedDict
from dataclasses import dataclass, field
import logging
import weakref

from ..core.entry import Entry
from ..core.exceptions import CacheError

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """Cached ledger entry with metadata."""
    entry: Entry
    cached_at: float = field(default_factory=time.time)
    access_count: int = 0
    last_accessed: float = field(default_factory=time.time)
    size_bytes: int = 0
    
    def accessed(self):
        """Update access statistics."""
        self.access_count += 1
        self.last_accessed = time.time()
    
    def age(self) -> float:
        """Get age of cache entry in seconds."""
        return time.time() - self.cached_at
    
    def is_stale(self, max_age: float) -> bool:
        """Check if entry is stale."""
        return self.age() > max_age


@dataclass
class CacheStats:
    """Cache performance statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    total_size_bytes: int = 0
    entry_count: int = 0
    
    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': self.hit_rate,
            'evictions': self.evictions,
            'total_size_bytes': self.total_size_bytes,
            'entry_count': self.entry_count,
            'avg_entry_size': self.total_size_bytes / self.entry_count if self.entry_count > 0 else 0,
        }


class LRUCache:
    """LRU cache for ledger entries."""
    
    def __init__(
        self,
        max_entries: int = 10000,
        max_size_mb: int = 100,
        ttl_seconds: float = 3600
    ):
        self.max_entries = max_entries
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.ttl_seconds = ttl_seconds
        
        self._cache: OrderedDict[int, CacheEntry] = OrderedDict()
        self._lock = threading.RLock()
        self._stats = CacheStats()
    
    def get(self, sequence: int) -> Optional[Entry]:
        """Get entry from cache."""
        with self._lock:
            if sequence in self._cache:
                cache_entry = self._cache[sequence]
                
                # Check if stale
                if cache_entry.is_stale(self.ttl_seconds):
                    del self._cache[sequence]
                    self._stats.misses += 1
                    return None
                
                # Move to end (most recently used)
                self._cache.move_to_end(sequence)
                cache_entry.accessed()
                
                self._stats.hits += 1
                return cache_entry.entry
            
            self._stats.misses += 1
            return None
    
    def put(self, entry: Entry) -> None:
        """Add entry to cache."""
        with self._lock:
            # Calculate size
            entry_json = json.dumps(entry.to_dict())
            size_bytes = len(entry_json.encode('utf-8'))
            
            # Check if we need to evict
            while self._should_evict(size_bytes):
                self._evict_oldest()
            
            # Add to cache
            cache_entry = CacheEntry(
                entry=entry,
                size_bytes=size_bytes
            )
            
            self._cache[entry.sequence] = cache_entry
            self._stats.total_size_bytes += size_bytes
            self._stats.entry_count = len(self._cache)
    
    def invalidate(self, sequence: int) -> bool:
        """Remove entry from cache."""
        with self._lock:
            if sequence in self._cache:
                cache_entry = self._cache.pop(sequence)
                self._stats.total_size_bytes -= cache_entry.size_bytes
                self._stats.entry_count = len(self._cache)
                return True
            return False
    
    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()
            self._stats.total_size_bytes = 0
            self._stats.entry_count = 0
    
    def _should_evict(self, new_size: int) -> bool:
        """Check if eviction is needed."""
        if len(self._cache) >= self.max_entries:
            return True
        
        if self._stats.total_size_bytes + new_size > self.max_size_bytes:
            return True
        
        return False
    
    def _evict_oldest(self) -> None:
        """Evict least recently used entry."""
        if self._cache:
            sequence, cache_entry = self._cache.popitem(last=False)
            self._stats.evictions += 1
            self._stats.total_size_bytes -= cache_entry.size_bytes
            self._stats.entry_count = len(self._cache)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return self._stats.to_dict()


class RangeCache:
    """Cache for range queries."""
    
    def __init__(self, max_ranges: int = 100):
        self.max_ranges = max_ranges
        self._ranges: OrderedDict[Tuple[int, int], List[Entry]] = OrderedDict()
        self._lock = threading.RLock()
    
    def get_range(self, start: int, end: int) -> Optional[List[Entry]]:
        """Get cached range."""
        with self._lock:
            key = (start, end)
            if key in self._ranges:
                # Move to end
                self._ranges.move_to_end(key)
                return self._ranges[key].copy()
            return None
    
    def put_range(self, start: int, end: int, entries: List[Entry]) -> None:
        """Cache a range of entries."""
        with self._lock:
            # Evict if needed
            while len(self._ranges) >= self.max_ranges:
                self._ranges.popitem(last=False)
            
            self._ranges[(start, end)] = entries.copy()
    
    def invalidate_overlapping(self, sequence: int) -> int:
        """Invalidate ranges containing a sequence."""
        with self._lock:
            to_remove = []
            
            for (start, end) in self._ranges:
                if start <= sequence <= end:
                    to_remove.append((start, end))
            
            for key in to_remove:
                del self._ranges[key]
            
            return len(to_remove)
    
    def clear(self) -> None:
        """Clear all cached ranges."""
        with self._lock:
            self._ranges.clear()


class QueryCache:
    """Cache for search queries."""
    
    def __init__(self, max_queries: int = 500, ttl_seconds: float = 300):
        self.max_queries = max_queries
        self.ttl_seconds = ttl_seconds
        self._queries: OrderedDict[str, Tuple[List[Entry], float]] = OrderedDict()
        self._lock = threading.RLock()
    
    def _get_query_key(self, criteria: Dict[str, Any]) -> str:
        """Generate cache key for query criteria."""
        # Sort keys for consistent hashing
        sorted_criteria = json.dumps(criteria, sort_keys=True)
        return hashlib.sha256(sorted_criteria.encode()).hexdigest()
    
    def get(self, criteria: Dict[str, Any]) -> Optional[List[Entry]]:
        """Get cached query results."""
        with self._lock:
            key = self._get_query_key(criteria)
            
            if key in self._queries:
                entries, cached_at = self._queries[key]
                
                # Check if stale
                if time.time() - cached_at > self.ttl_seconds:
                    del self._queries[key]
                    return None
                
                # Move to end
                self._queries.move_to_end(key)
                return entries.copy()
            
            return None
    
    def put(self, criteria: Dict[str, Any], entries: List[Entry]) -> None:
        """Cache query results."""
        with self._lock:
            # Evict if needed
            while len(self._queries) >= self.max_queries:
                self._queries.popitem(last=False)
            
            key = self._get_query_key(criteria)
            self._queries[key] = (entries.copy(), time.time())
    
    def invalidate_all(self) -> None:
        """Invalidate all cached queries."""
        with self._lock:
            self._queries.clear()


class CachedLedger:
    """Ledger wrapper with caching."""
    
    def __init__(
        self,
        ledger,
        enable_entry_cache: bool = True,
        enable_range_cache: bool = True,
        enable_query_cache: bool = True,
        **cache_config
    ):
        self.ledger = ledger
        
        # Initialize caches
        self.entry_cache = LRUCache(**cache_config) if enable_entry_cache else None
        self.range_cache = RangeCache() if enable_range_cache else None
        self.query_cache = QueryCache() if enable_query_cache else None
        
        # Cache invalidation callbacks
        self._invalidation_callbacks = []
    
    def append(self, data: Dict[str, Any]) -> Entry:
        """Append entry and update caches."""
        # Append to underlying ledger
        entry = self.ledger.append(data)
        
        # Cache the new entry
        if self.entry_cache:
            self.entry_cache.put(entry)
        
        # Invalidate affected caches
        if self.range_cache:
            self.range_cache.invalidate_overlapping(entry.sequence)
        
        if self.query_cache:
            self.query_cache.invalidate_all()
        
        # Notify callbacks
        for callback in self._invalidation_callbacks:
            try:
                callback(entry)
            except Exception as e:
                logger.error(f"Invalidation callback error: {e}")
        
        return entry
    
    def get(self, sequence: int) -> Optional[Entry]:
        """Get entry with caching."""
        # Check cache first
        if self.entry_cache:
            cached = self.entry_cache.get(sequence)
            if cached:
                return cached
        
        # Get from ledger
        entry = self.ledger.get(sequence)
        
        # Cache if found
        if entry and self.entry_cache:
            self.entry_cache.put(entry)
        
        return entry
    
    def get_range(self, start: int, end: int) -> List[Entry]:
        """Get range with caching."""
        # Check range cache
        if self.range_cache:
            cached = self.range_cache.get_range(start, end)
            if cached:
                return cached
        
        # Get from ledger
        entries = self.ledger.get_range(start, end)
        
        # Cache the range
        if self.range_cache:
            self.range_cache.put_range(start, end, entries)
        
        # Also cache individual entries
        if self.entry_cache:
            for entry in entries:
                self.entry_cache.put(entry)
        
        return entries
    
    def search(self, criteria: Dict[str, Any]) -> List[Entry]:
        """Search with caching."""
        # Check query cache
        if self.query_cache:
            cached = self.query_cache.get(criteria)
            if cached:
                return cached
        
        # Search in ledger
        entries = self.ledger.search(criteria)
        
        # Cache results
        if self.query_cache:
            self.query_cache.put(criteria, entries)
        
        # Also cache individual entries
        if self.entry_cache:
            for entry in entries:
                self.entry_cache.put(entry)
        
        return entries
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        stats = {}
        
        if self.entry_cache:
            stats['entry_cache'] = self.entry_cache.get_stats()
        
        if self.range_cache:
            stats['range_cache'] = {
                'cached_ranges': len(self.range_cache._ranges)
            }
        
        if self.query_cache:
            stats['query_cache'] = {
                'cached_queries': len(self.query_cache._queries)
            }
        
        return stats
    
    def clear_caches(self) -> None:
        """Clear all caches."""
        if self.entry_cache:
            self.entry_cache.clear()
        
        if self.range_cache:
            self.range_cache.clear()
        
        if self.query_cache:
            self.query_cache.invalidate_all()
    
    def add_invalidation_callback(self, callback: Callable[[Entry], None]) -> None:
        """Add cache invalidation callback."""
        self._invalidation_callbacks.append(callback)
    
    # Proxy other methods to underlying ledger
    def __getattr__(self, name):
        return getattr(self.ledger, name)


class WriteThroughCache:
    """Write-through cache for ledger backends."""
    
    def __init__(self, backend, cache_size: int = 10000):
        self.backend = backend
        self.cache = LRUCache(max_entries=cache_size)
    
    def append(self, entry_data: Dict[str, Any]) -> int:
        """Append with write-through caching."""
        # Write to backend
        sequence = self.backend.append(entry_data)
        
        # Create entry object for cache
        entry = Entry.from_dict(entry_data)
        
        # Cache it
        self.cache.put(entry)
        
        return sequence
    
    def get(self, sequence: int) -> Optional[Dict[str, Any]]:
        """Get with caching."""
        # Check cache
        cached_entry = self.cache.get(sequence)
        if cached_entry:
            return cached_entry.to_dict()
        
        # Get from backend
        entry_data = self.backend.get(sequence)
        
        if entry_data:
            # Cache it
            entry = Entry.from_dict(entry_data)
            self.cache.put(entry)
        
        return entry_data
    
    # Proxy other methods
    def __getattr__(self, name):
        return getattr(self.backend, name)