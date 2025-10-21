"""Exception classes for SignLedger."""

from typing import Optional, Any, List


class SignLedgerError(Exception):
    """Base exception for all SignLedger errors."""
    
    def __init__(self, message: str, details: Optional[dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class IntegrityError(SignLedgerError):
    """Raised when ledger integrity is compromised."""
    
    def __init__(
        self,
        message: str,
        entry_id: Optional[str] = None,
        expected_hash: Optional[str] = None,
        actual_hash: Optional[str] = None,
    ):
        details = {
            "entry_id": entry_id,
            "expected_hash": expected_hash,
            "actual_hash": actual_hash,
        }
        super().__init__(message, details)
        self.entry_id = entry_id
        self.expected_hash = expected_hash
        self.actual_hash = actual_hash


class ValidationError(SignLedgerError):
    """Raised when entry validation fails."""
    
    def __init__(self, message: str, field: Optional[str] = None, value: Any = None):
        details = {"field": field, "value": value}
        super().__init__(message, details)
        self.field = field
        self.value = value


class StorageError(SignLedgerError):
    """Raised when storage operations fail."""
    
    def __init__(self, message: str, operation: str, backend: Optional[str] = None):
        details = {"operation": operation, "backend": backend}
        super().__init__(message, details)
        self.operation = operation
        self.backend = backend


class SignatureError(SignLedgerError):
    """Raised when signature verification fails."""
    
    def __init__(
        self,
        message: str,
        entry_id: Optional[str] = None,
        signature: Optional[str] = None,
    ):
        details = {"entry_id": entry_id, "signature": signature}
        super().__init__(message, details)
        self.entry_id = entry_id
        self.signature = signature


class ConsensusError(SignLedgerError):
    """Raised when consensus cannot be reached."""
    
    def __init__(self, message: str, nodes: Optional[List[str]] = None):
        details = {"nodes": nodes}
        super().__init__(message, details)
        self.nodes = nodes or []