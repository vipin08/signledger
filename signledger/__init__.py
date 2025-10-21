"""SignLedger - Blockchain-Inspired Audit Log Library.

A lightweight, high-performance Python library that implements blockchain-inspired
immutable audit logging for applications with cryptographically secure,
tamper-evident logs and built-in integrity verification.
"""

from signledger.core.ledger import Ledger, Entry
from signledger.core.exceptions import (
    SignLedgerError,
    IntegrityError,
    ValidationError,
    StorageError,
)

__version__ = "1.0.0"
__author__ = "Vipin Kumar"
__email__ = "vipin08@example.com"

__all__ = [
    "Ledger",
    "Entry",
    "SignLedgerError",
    "IntegrityError",
    "ValidationError",
    "StorageError",
]