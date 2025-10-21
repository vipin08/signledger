"""Cryptographic hashing for SignLedger."""

import hashlib
import json
from typing import Any, Dict, Union
from datetime import datetime

from ..core.exceptions import ValidationError


class HashChain:
    """Implements cryptographic hash chain for ledger entries."""
    
    SUPPORTED_ALGORITHMS = {
        "sha256": hashlib.sha256,
        "sha512": hashlib.sha512,
        "sha3_256": hashlib.sha3_256,
        "sha3_512": hashlib.sha3_512,
    }
    
    def __init__(self, algorithm: str = "sha256"):
        if algorithm not in self.SUPPORTED_ALGORITHMS:
            raise ValueError(
                f"Unsupported algorithm: {algorithm}. "
                f"Supported: {list(self.SUPPORTED_ALGORITHMS.keys())}"
            )
        
        self.algorithm = algorithm
        self._hash_func = self.SUPPORTED_ALGORITHMS[algorithm]
    
    def calculate_hash(self, entry: Any) -> str:
        """Calculate hash for a ledger entry.
        
        Args:
            entry: Entry object with to_dict() method or dict
            
        Returns:
            Hex-encoded hash string
        """
        # Convert entry to dict if needed
        if hasattr(entry, "to_dict"):
            entry_dict = entry.to_dict()
        else:
            entry_dict = entry
        
        # Create a copy without the hash field
        data_to_hash = {
            k: v for k, v in entry_dict.items()
            if k not in ("hash", "signature")
        }
        
        # Convert datetime objects to ISO format
        data_to_hash = self._prepare_for_hashing(data_to_hash)
        
        # Create canonical JSON representation
        canonical_json = json.dumps(data_to_hash, sort_keys=True, separators=(',', ':'))
        
        # Calculate hash
        hasher = self._hash_func()
        hasher.update(canonical_json.encode('utf-8'))
        
        return hasher.hexdigest()
    
    def _prepare_for_hashing(self, data: Any) -> Any:
        """Prepare data for hashing by converting special types."""
        if isinstance(data, datetime):
            return data.isoformat()
        elif isinstance(data, dict):
            return {k: self._prepare_for_hashing(v) for k, v in data.items()}
        elif isinstance(data, (list, tuple)):
            return [self._prepare_for_hashing(item) for item in data]
        else:
            return data
    
    def verify_hash(self, entry: Any, expected_hash: str) -> bool:
        """Verify the hash of an entry.
        
        Args:
            entry: Entry to verify
            expected_hash: Expected hash value
            
        Returns:
            True if hash matches
        """
        calculated_hash = self.calculate_hash(entry)
        return calculated_hash == expected_hash
    
    def create_genesis_hash(self) -> str:
        """Create hash for genesis block."""
        genesis_data = {
            "genesis": True,
            "algorithm": self.algorithm,
            "timestamp": "1970-01-01T00:00:00+00:00",
        }
        
        canonical_json = json.dumps(genesis_data, sort_keys=True)
        hasher = self._hash_func()
        hasher.update(canonical_json.encode('utf-8'))
        
        return hasher.hexdigest()


class MerkleTree:
    """Merkle tree implementation for efficient verification."""
    
    def __init__(self, hash_algorithm: str = "sha256"):
        self.hash_chain = HashChain(hash_algorithm)
        self._tree = []
        self._leaves = []
    
    def build(self, entries: list) -> str:
        """Build Merkle tree from entries and return root hash.
        
        Args:
            entries: List of entries
            
        Returns:
            Root hash of the Merkle tree
        """
        if not entries:
            return self.hash_chain.create_genesis_hash()
        
        # Create leaf nodes
        self._leaves = [
            self.hash_chain.calculate_hash(entry)
            for entry in entries
        ]
        
        # Build tree bottom-up
        self._tree = [self._leaves]
        current_level = self._leaves
        
        while len(current_level) > 1:
            next_level = []
            
            # Process pairs
            for i in range(0, len(current_level), 2):
                if i + 1 < len(current_level):
                    # Hash pair
                    combined = current_level[i] + current_level[i + 1]
                else:
                    # Odd number, duplicate last hash
                    combined = current_level[i] + current_level[i]
                
                hasher = self.hash_chain._hash_func()
                hasher.update(combined.encode('utf-8'))
                next_level.append(hasher.hexdigest())
            
            self._tree.append(next_level)
            current_level = next_level
        
        return current_level[0] if current_level else ""
    
    def get_proof(self, index: int) -> list:
        """Get Merkle proof for entry at given index.
        
        Args:
            index: Index of entry in the original list
            
        Returns:
            List of hashes forming the proof path
        """
        if not self._tree or index < 0 or index >= len(self._leaves):
            return []
        
        proof = []
        current_index = index
        
        for level in self._tree[:-1]:  # Exclude root level
            # Determine sibling index
            if current_index % 2 == 0:
                sibling_index = current_index + 1
            else:
                sibling_index = current_index - 1
            
            # Add sibling hash if it exists
            if sibling_index < len(level):
                proof.append({
                    "hash": level[sibling_index],
                    "position": "right" if current_index % 2 == 0 else "left"
                })
            
            # Move to next level
            current_index //= 2
        
        return proof
    
    def verify_proof(self, entry_hash: str, proof: list, root_hash: str) -> bool:
        """Verify Merkle proof for an entry.
        
        Args:
            entry_hash: Hash of the entry to verify
            proof: Merkle proof path
            root_hash: Expected root hash
            
        Returns:
            True if proof is valid
        """
        current_hash = entry_hash
        
        for proof_element in proof:
            sibling_hash = proof_element["hash"]
            position = proof_element["position"]
            
            if position == "left":
                combined = sibling_hash + current_hash
            else:
                combined = current_hash + sibling_hash
            
            hasher = self.hash_chain._hash_func()
            hasher.update(combined.encode('utf-8'))
            current_hash = hasher.hexdigest()
        
        return current_hash == root_hash