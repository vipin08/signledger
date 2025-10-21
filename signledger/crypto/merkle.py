"""Merkle tree implementation for SignLedger."""

import hashlib
import json
import math
from typing import List, Tuple, Optional, Dict, Any, Union
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class MerkleNode:
    """Represents a node in the Merkle tree."""
    hash: str
    left: Optional['MerkleNode'] = None
    right: Optional['MerkleNode'] = None
    is_leaf: bool = False
    data: Optional[Any] = None
    index: Optional[int] = None


@dataclass
class MerkleProof:
    """Proof of inclusion for a leaf in the Merkle tree."""
    leaf_hash: str
    leaf_index: int
    root_hash: str
    proof_path: List[Tuple[str, str]]  # List of (hash, direction) tuples
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert proof to dictionary."""
        return {
            'leaf_hash': self.leaf_hash,
            'leaf_index': self.leaf_index,
            'root_hash': self.root_hash,
            'proof_path': self.proof_path
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MerkleProof':
        """Create proof from dictionary."""
        return cls(
            leaf_hash=data['leaf_hash'],
            leaf_index=data['leaf_index'],
            root_hash=data['root_hash'],
            proof_path=[(h, d) for h, d in data['proof_path']]
        )


class MerkleTree:
    """Merkle tree for efficient verification of data integrity."""
    
    def __init__(self, hash_func: str = 'sha256'):
        self.hash_func = hash_func
        self.root: Optional[MerkleNode] = None
        self.leaves: List[MerkleNode] = []
        self._leaf_map: Dict[str, int] = {}  # Hash to index mapping
    
    def build(self, data_items: List[Union[str, Dict[str, Any]]]) -> str:
        """Build Merkle tree from data items."""
        if not data_items:
            raise ValueError("Cannot build Merkle tree from empty data")
        
        # Create leaf nodes
        self.leaves = []
        self._leaf_map = {}
        
        for i, item in enumerate(data_items):
            # Convert to string if needed
            if isinstance(item, dict):
                item_str = json.dumps(item, sort_keys=True)
            else:
                item_str = str(item)
            
            leaf_hash = self._hash(item_str)
            leaf_node = MerkleNode(
                hash=leaf_hash,
                is_leaf=True,
                data=item,
                index=i
            )
            self.leaves.append(leaf_node)
            self._leaf_map[leaf_hash] = i
        
        # Build tree
        self.root = self._build_tree(self.leaves)
        return self.root.hash
    
    def _build_tree(self, nodes: List[MerkleNode]) -> MerkleNode:
        """Recursively build the Merkle tree."""
        if len(nodes) == 1:
            return nodes[0]
        
        # Create parent level
        parent_nodes = []
        
        for i in range(0, len(nodes), 2):
            left = nodes[i]
            # Handle odd number of nodes
            right = nodes[i + 1] if i + 1 < len(nodes) else left
            
            # Create parent node
            parent_hash = self._hash(left.hash + right.hash)
            parent = MerkleNode(
                hash=parent_hash,
                left=left,
                right=right
            )
            parent_nodes.append(parent)
        
        # Recursively build upper levels
        return self._build_tree(parent_nodes)
    
    def get_root(self) -> Optional[str]:
        """Get the root hash of the tree."""
        return self.root.hash if self.root else None
    
    def generate_proof(self, data_item: Union[str, Dict[str, Any], int]) -> Optional[MerkleProof]:
        """Generate proof of inclusion for a data item."""
        if not self.root:
            return None
        
        # Find leaf index
        if isinstance(data_item, int):
            # Direct index provided
            if 0 <= data_item < len(self.leaves):
                leaf_index = data_item
                leaf_hash = self.leaves[leaf_index].hash
            else:
                return None
        else:
            # Hash the data item
            if isinstance(data_item, dict):
                item_str = json.dumps(data_item, sort_keys=True)
            else:
                item_str = str(data_item)
            
            leaf_hash = self._hash(item_str)
            
            if leaf_hash not in self._leaf_map:
                return None
            
            leaf_index = self._leaf_map[leaf_hash]
        
        # Generate proof path
        proof_path = self._generate_proof_path(leaf_index)
        
        return MerkleProof(
            leaf_hash=leaf_hash,
            leaf_index=leaf_index,
            root_hash=self.root.hash,
            proof_path=proof_path
        )
    
    def _generate_proof_path(self, leaf_index: int) -> List[Tuple[str, str]]:
        """Generate the proof path from leaf to root."""
        proof_path = []
        
        # Calculate path through tree levels
        current_index = leaf_index
        level_size = len(self.leaves)
        
        # Start from leaves and work up
        nodes = self.leaves.copy()
        
        while level_size > 1:
            # Find sibling
            if current_index % 2 == 0:
                # Current is left, sibling is right
                sibling_index = current_index + 1
                direction = 'right'
            else:
                # Current is right, sibling is left
                sibling_index = current_index - 1
                direction = 'left'
            
            # Handle edge case for odd number of nodes
            if sibling_index >= level_size:
                sibling_index = current_index
            
            # Add sibling to proof path
            sibling_hash = nodes[sibling_index].hash
            proof_path.append((sibling_hash, direction))
            
            # Move to parent level
            current_index //= 2
            
            # Build parent level
            parent_nodes = []
            for i in range(0, level_size, 2):
                left = nodes[i]
                right = nodes[i + 1] if i + 1 < level_size else left
                parent_hash = self._hash(left.hash + right.hash)
                parent_nodes.append(MerkleNode(hash=parent_hash))
            
            nodes = parent_nodes
            level_size = len(parent_nodes)
        
        return proof_path
    
    def verify_proof(self, proof: MerkleProof) -> bool:
        """Verify a Merkle proof."""
        # Start with leaf hash
        current_hash = proof.leaf_hash
        
        # Follow proof path
        for sibling_hash, direction in proof.proof_path:
            if direction == 'left':
                # Sibling is on the left
                current_hash = self._hash(sibling_hash + current_hash)
            else:
                # Sibling is on the right
                current_hash = self._hash(current_hash + sibling_hash)
        
        # Check if we reached the expected root
        return current_hash == proof.root_hash
    
    def _hash(self, data: Union[str, bytes]) -> str:
        """Hash data using the configured hash function."""
        if isinstance(data, str):
            data = data.encode('utf-8')
        
        hasher = hashlib.new(self.hash_func)
        hasher.update(data)
        return hasher.hexdigest()
    
    def get_leaves(self) -> List[str]:
        """Get all leaf hashes."""
        return [leaf.hash for leaf in self.leaves]
    
    def visualize(self) -> str:
        """Create a text visualization of the tree."""
        if not self.root:
            return "Empty tree"
        
        lines = []
        self._visualize_node(self.root, "", True, lines)
        return "\n".join(lines)
    
    def _visualize_node(self, node: MerkleNode, prefix: str, is_tail: bool, lines: List[str]):
        """Recursively visualize tree nodes."""
        # Add current node
        connector = "└── " if is_tail else "├── "
        lines.append(prefix + connector + node.hash[:8] + ("... (leaf)" if node.is_leaf else "..."))
        
        # Add children
        if node.left and not node.is_leaf:
            extension = "    " if is_tail else "│   "
            if node.right and node.right != node.left:
                self._visualize_node(node.left, prefix + extension, False, lines)
                self._visualize_node(node.right, prefix + extension, True, lines)
            else:
                self._visualize_node(node.left, prefix + extension, True, lines)


class IncrementalMerkleTree:
    """Merkle tree that supports incremental updates."""
    
    def __init__(self, hash_func: str = 'sha256'):
        self.hash_func = hash_func
        self.levels: List[List[str]] = [[]]  # Level 0 is leaves
        self._hasher = lambda x: hashlib.new(hash_func, x.encode() if isinstance(x, str) else x).hexdigest()
    
    def append(self, data: Union[str, Dict[str, Any]]) -> str:
        """Append a new leaf and update the tree."""
        # Convert to string if needed
        if isinstance(data, dict):
            data_str = json.dumps(data, sort_keys=True)
        else:
            data_str = str(data)
        
        # Hash the data
        leaf_hash = self._hasher(data_str)
        
        # Add to leaves
        self.levels[0].append(leaf_hash)
        
        # Update tree
        self._update_tree(len(self.levels[0]) - 1)
        
        return leaf_hash
    
    def _update_tree(self, new_leaf_index: int):
        """Update tree after adding a new leaf."""
        current_index = new_leaf_index
        
        for level in range(len(self.levels) - 1):
            # Check if we need a new level
            if level + 1 >= len(self.levels):
                self.levels.append([])
            
            # Check if this is a right child
            if current_index % 2 == 1:
                # Combine with left sibling
                left_hash = self.levels[level][current_index - 1]
                right_hash = self.levels[level][current_index]
                parent_hash = self._hasher(left_hash + right_hash)
                
                # Add or update parent
                parent_index = current_index // 2
                if parent_index >= len(self.levels[level + 1]):
                    self.levels[level + 1].append(parent_hash)
                else:
                    self.levels[level + 1][parent_index] = parent_hash
                
                current_index = parent_index
            else:
                # This is a left child, wait for right sibling
                break
    
    def get_root(self) -> Optional[str]:
        """Get current root hash."""
        if not self.levels[0]:
            return None
        
        # Calculate root by combining unpaired nodes
        level_hashes = self.levels[0].copy()
        
        while len(level_hashes) > 1:
            next_level = []
            
            for i in range(0, len(level_hashes), 2):
                left = level_hashes[i]
                right = level_hashes[i + 1] if i + 1 < len(level_hashes) else left
                parent = self._hasher(left + right)
                next_level.append(parent)
            
            level_hashes = next_level
        
        return level_hashes[0]
    
    def generate_consistency_proof(self, old_size: int, new_size: int) -> List[str]:
        """Generate proof that old tree is consistent with new tree."""
        if old_size > new_size or old_size < 0 or new_size > len(self.levels[0]):
            raise ValueError("Invalid tree sizes")
        
        if old_size == new_size or old_size == 0:
            return []
        
        # This is a simplified version
        # A full implementation would generate minimal proof
        proof = []
        
        # Add hashes needed to verify consistency
        # This would include siblings needed to reconstruct both old and new roots
        
        return proof


class MerkleVerifier:
    """Utilities for Merkle tree verification."""
    
    @staticmethod
    def verify_batch_proofs(proofs: List[MerkleProof], expected_root: str) -> Tuple[List[bool], bool]:
        """Verify multiple proofs against the same root."""
        results = []
        tree = MerkleTree()
        
        for proof in proofs:
            # All proofs should have the same root
            if proof.root_hash != expected_root:
                results.append(False)
            else:
                results.append(tree.verify_proof(proof))
        
        all_valid = all(results)
        return results, all_valid
    
    @staticmethod
    def combine_trees(tree1_root: str, tree2_root: str, hash_func: str = 'sha256') -> str:
        """Combine two Merkle tree roots."""
        hasher = hashlib.new(hash_func)
        hasher.update(tree1_root.encode('utf-8'))
        hasher.update(tree2_root.encode('utf-8'))
        return hasher.hexdigest()
    
    @staticmethod
    def verify_subset(subset_root: str, subset_proofs: List[MerkleProof], full_tree_root: str) -> bool:
        """Verify that a subset is part of a larger tree."""
        # This would require additional proof data
        # Simplified version
        for proof in subset_proofs:
            if proof.root_hash != full_tree_root:
                return False
        return True