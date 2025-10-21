"""Batch operations for SignLedger."""

import time
import threading
from typing import List, Dict, Any, Optional, Callable, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
import queue
from contextlib import contextmanager

from .ledger import Entry
from .exceptions import ValidationError, StorageError

logger = logging.getLogger(__name__)


@dataclass
class BatchOperation:
    """Represents a single operation in a batch."""
    operation_id: str
    data: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: Optional[datetime] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)


@dataclass
class BatchResult:
    """Result of a batch operation."""
    successful_entries: List[Entry]
    failed_operations: List[Tuple[BatchOperation, Exception]]
    total_operations: int
    execution_time_ms: float
    
    @property
    def success_count(self) -> int:
        return len(self.successful_entries)
    
    @property
    def failure_count(self) -> int:
        return len(self.failed_operations)
    
    @property
    def success_rate(self) -> float:
        if self.total_operations == 0:
            return 0.0
        return self.success_count / self.total_operations


class BatchProcessor:
    """Handles batch operations for the ledger."""
    
    def __init__(
        self,
        ledger,
        max_batch_size: int = 1000,
        auto_commit_threshold: int = 100,
        auto_commit_interval: float = 5.0,
        parallel_processing: bool = False,
        num_workers: int = 4
    ):
        self.ledger = ledger
        self.max_batch_size = max_batch_size
        self.auto_commit_threshold = auto_commit_threshold
        self.auto_commit_interval = auto_commit_interval
        self.parallel_processing = parallel_processing
        self.num_workers = num_workers
        
        self._batch_queue: queue.Queue = queue.Queue(maxsize=max_batch_size)
        self._lock = threading.RLock()
        self._auto_commit_thread = None
        self._stop_auto_commit = threading.Event()
        self._processing = False
        
        # Statistics
        self._total_processed = 0
        self._total_failed = 0
        
        # Start auto-commit thread if enabled
        if auto_commit_interval > 0:
            self._start_auto_commit()
    
    def add_operation(self, operation: BatchOperation) -> None:
        """Add an operation to the batch queue."""
        with self._lock:
            if self._batch_queue.qsize() >= self.max_batch_size:
                raise ValidationError("Batch queue is full")
            
            self._batch_queue.put(operation)
            
            # Check if we should auto-commit
            if self._batch_queue.qsize() >= self.auto_commit_threshold:
                self._process_batch()
    
    def add_data(self, data: Dict[str, Any], **metadata) -> None:
        """Convenience method to add data to batch."""
        operation = BatchOperation(
            operation_id=f"batch_{int(time.time() * 1000000)}",
            data=data,
            metadata=metadata
        )
        self.add_operation(operation)
    
    def process(self) -> BatchResult:
        """Process all pending operations in the batch."""
        with self._lock:
            return self._process_batch()
    
    def _process_batch(self) -> BatchResult:
        """Internal method to process the batch."""
        if self._processing:
            raise ValidationError("Batch processing already in progress")
        
        self._processing = True
        start_time = time.time()
        
        # Collect all operations
        operations = []
        while not self._batch_queue.empty():
            try:
                operations.append(self._batch_queue.get_nowait())
            except queue.Empty:
                break
        
        if not operations:
            self._processing = False
            return BatchResult(
                successful_entries=[],
                failed_operations=[],
                total_operations=0,
                execution_time_ms=0
            )
        
        try:
            if self.parallel_processing:
                return self._process_parallel(operations, start_time)
            else:
                return self._process_sequential(operations, start_time)
        finally:
            self._processing = False
    
    def _process_sequential(self, operations: List[BatchOperation], start_time: float) -> BatchResult:
        """Process operations sequentially."""
        successful_entries = []
        failed_operations = []
        
        for operation in operations:
            try:
                # Prepare entry data
                entry_data = {
                    'batch_id': operation.operation_id,
                    'timestamp': operation.timestamp.isoformat(),
                    **operation.data
                }
                
                # Add metadata if present
                if operation.metadata:
                    entry_data['batch_metadata'] = operation.metadata
                
                # Append to ledger
                entry = self.ledger.append(entry_data)
                successful_entries.append(entry)
                self._total_processed += 1
                
            except Exception as e:
                failed_operations.append((operation, e))
                self._total_failed += 1
                logger.error(f"Failed to process batch operation {operation.operation_id}: {e}")
        
        execution_time = (time.time() - start_time) * 1000
        
        return BatchResult(
            successful_entries=successful_entries,
            failed_operations=failed_operations,
            total_operations=len(operations),
            execution_time_ms=execution_time
        )
    
    def _process_parallel(self, operations: List[BatchOperation], start_time: float) -> BatchResult:
        """Process operations in parallel using thread pool."""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        successful_entries = []
        failed_operations = []
        
        def process_single(operation: BatchOperation) -> Tuple[Optional[Entry], Optional[Exception]]:
            try:
                entry_data = {
                    'batch_id': operation.operation_id,
                    'timestamp': operation.timestamp.isoformat(),
                    **operation.data
                }
                
                if operation.metadata:
                    entry_data['batch_metadata'] = operation.metadata
                
                entry = self.ledger.append(entry_data)
                return entry, None
            except Exception as e:
                return None, e
        
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            # Submit all operations
            future_to_operation = {
                executor.submit(process_single, op): op
                for op in operations
            }
            
            # Collect results
            for future in as_completed(future_to_operation):
                operation = future_to_operation[future]
                try:
                    entry, error = future.result()
                    if error:
                        failed_operations.append((operation, error))
                        self._total_failed += 1
                    else:
                        successful_entries.append(entry)
                        self._total_processed += 1
                except Exception as e:
                    failed_operations.append((operation, e))
                    self._total_failed += 1
        
        execution_time = (time.time() - start_time) * 1000
        
        return BatchResult(
            successful_entries=successful_entries,
            failed_operations=failed_operations,
            total_operations=len(operations),
            execution_time_ms=execution_time
        )
    
    def _start_auto_commit(self):
        """Start auto-commit thread."""
        def auto_commit_loop():
            while not self._stop_auto_commit.wait(self.auto_commit_interval):
                with self._lock:
                    if not self._batch_queue.empty():
                        try:
                            self._process_batch()
                        except Exception as e:
                            logger.error(f"Auto-commit failed: {e}")
        
        self._auto_commit_thread = threading.Thread(
            target=auto_commit_loop,
            daemon=True,
            name="BatchProcessor-AutoCommit"
        )
        self._auto_commit_thread.start()
    
    def stop(self):
        """Stop the batch processor."""
        # Process any remaining operations
        with self._lock:
            if not self._batch_queue.empty():
                self._process_batch()
        
        # Stop auto-commit thread
        if self._auto_commit_thread:
            self._stop_auto_commit.set()
            self._auto_commit_thread.join(timeout=5)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get batch processor statistics."""
        with self._lock:
            return {
                'total_processed': self._total_processed,
                'total_failed': self._total_failed,
                'success_rate': self._total_processed / (self._total_processed + self._total_failed) if (self._total_processed + self._total_failed) > 0 else 0,
                'queue_size': self._batch_queue.qsize(),
                'is_processing': self._processing,
            }
    
    @contextmanager
    def batch_context(self, auto_process: bool = True):
        """Context manager for batch operations."""
        # Temporarily disable auto-commit
        old_threshold = self.auto_commit_threshold
        self.auto_commit_threshold = float('inf')
        
        try:
            yield self
        finally:
            # Restore auto-commit threshold
            self.auto_commit_threshold = old_threshold
            
            # Process batch if requested
            if auto_process:
                self.process()


class TransactionalBatch:
    """Transactional batch operations with rollback support."""
    
    def __init__(self, ledger):
        self.ledger = ledger
        self._operations: List[BatchOperation] = []
        self._checkpoint = None
        self._committed = False
    
    def add(self, data: Dict[str, Any], **metadata) -> None:
        """Add operation to transaction."""
        if self._committed:
            raise ValidationError("Transaction already committed")
        
        operation = BatchOperation(
            operation_id=f"tx_{len(self._operations)}_{int(time.time() * 1000000)}",
            data=data,
            metadata=metadata
        )
        self._operations.append(operation)
    
    def commit(self) -> BatchResult:
        """Commit all operations in the transaction."""
        if self._committed:
            raise ValidationError("Transaction already committed")
        
        if not self._operations:
            raise ValidationError("No operations to commit")
        
        # Save checkpoint
        latest = self.ledger.get_latest()
        self._checkpoint = latest.sequence if latest else -1
        
        start_time = time.time()
        successful_entries = []
        failed_operations = []
        
        try:
            # Process all operations
            for operation in self._operations:
                try:
                    entry_data = {
                        'transaction_id': f"tx_{self._checkpoint}_{operation.operation_id}",
                        'timestamp': operation.timestamp.isoformat(),
                        **operation.data
                    }
                    
                    if operation.metadata:
                        entry_data['transaction_metadata'] = operation.metadata
                    
                    entry = self.ledger.append(entry_data)
                    successful_entries.append(entry)
                    
                except Exception as e:
                    # On any failure, rollback
                    self._rollback()
                    raise StorageError(f"Transaction failed: {e}")
            
            self._committed = True
            execution_time = (time.time() - start_time) * 1000
            
            return BatchResult(
                successful_entries=successful_entries,
                failed_operations=failed_operations,
                total_operations=len(self._operations),
                execution_time_ms=execution_time
            )
            
        except Exception:
            # Ensure rollback on any error
            self._rollback()
            raise
    
    def _rollback(self):
        """Rollback transaction (if backend supports it)."""
        # This is a simplified rollback that works with backends
        # that support deletion or have transaction support
        logger.warning("Transaction rollback requested - manual intervention may be required")
        # In a real implementation, this would depend on backend capabilities
    
    def abort(self):
        """Abort the transaction."""
        self._operations.clear()
        self._committed = True


class BatchValidator:
    """Validates batch operations before processing."""
    
    def __init__(self, validators: Optional[List[Callable]] = None):
        self.validators = validators or []
    
    def add_validator(self, validator: Callable[[BatchOperation], bool]) -> None:
        """Add a validation function."""
        self.validators.append(validator)
    
    def validate_batch(self, operations: List[BatchOperation]) -> Tuple[List[BatchOperation], List[Tuple[BatchOperation, str]]]:
        """Validate all operations in a batch."""
        valid_operations = []
        invalid_operations = []
        
        for operation in operations:
            errors = []
            
            for validator in self.validators:
                try:
                    if not validator(operation):
                        errors.append(f"Validation failed: {validator.__name__}")
                except Exception as e:
                    errors.append(f"Validator error: {e}")
            
            if errors:
                invalid_operations.append((operation, "; ".join(errors)))
            else:
                valid_operations.append(operation)
        
        return valid_operations, invalid_operations