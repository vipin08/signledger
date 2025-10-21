"""Compression implementations for SignLedger."""

import zlib
import gzip
import bz2
import lzma
import base64
import json
import pickle
from typing import Any, Dict, Union, Optional, Tuple, List
from abc import ABC, abstractmethod
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class CompressionType(Enum):
    """Supported compression types."""
    NONE = "none"
    ZLIB = "zlib"
    GZIP = "gzip"
    BZIP2 = "bzip2"
    LZMA = "lzma"
    LZ4 = "lz4"  # Optional, requires lz4 package
    ZSTD = "zstd"  # Optional, requires zstandard package


class CompressionStats:
    """Statistics for compression operations."""
    
    def __init__(self):
        self.total_compressed = 0
        self.total_decompressed = 0
        self.bytes_before_compression = 0
        self.bytes_after_compression = 0
        self.compression_errors = 0
        self.decompression_errors = 0
    
    @property
    def compression_ratio(self) -> float:
        """Calculate average compression ratio."""
        if self.bytes_before_compression == 0:
            return 0.0
        return 1.0 - (self.bytes_after_compression / self.bytes_before_compression)
    
    @property
    def space_saved(self) -> int:
        """Calculate total space saved."""
        return self.bytes_before_compression - self.bytes_after_compression
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary."""
        return {
            'total_compressed': self.total_compressed,
            'total_decompressed': self.total_decompressed,
            'bytes_before_compression': self.bytes_before_compression,
            'bytes_after_compression': self.bytes_after_compression,
            'compression_ratio': self.compression_ratio,
            'space_saved': self.space_saved,
            'compression_errors': self.compression_errors,
            'decompression_errors': self.decompression_errors,
        }


class Compressor(ABC):
    """Abstract base class for compressors."""
    
    @abstractmethod
    def compress(self, data: bytes) -> bytes:
        """Compress data."""
        pass
    
    @abstractmethod
    def decompress(self, data: bytes) -> bytes:
        """Decompress data."""
        pass
    
    @property
    @abstractmethod
    def type(self) -> CompressionType:
        """Get compression type."""
        pass


class NoCompressor(Compressor):
    """No compression (passthrough)."""
    
    def compress(self, data: bytes) -> bytes:
        return data
    
    def decompress(self, data: bytes) -> bytes:
        return data
    
    @property
    def type(self) -> CompressionType:
        return CompressionType.NONE


class ZlibCompressor(Compressor):
    """Zlib compression."""
    
    def __init__(self, level: int = 6):
        self.level = max(0, min(9, level))
    
    def compress(self, data: bytes) -> bytes:
        return zlib.compress(data, self.level)
    
    def decompress(self, data: bytes) -> bytes:
        return zlib.decompress(data)
    
    @property
    def type(self) -> CompressionType:
        return CompressionType.ZLIB


class GzipCompressor(Compressor):
    """Gzip compression."""
    
    def __init__(self, level: int = 6):
        self.level = max(0, min(9, level))
    
    def compress(self, data: bytes) -> bytes:
        return gzip.compress(data, compresslevel=self.level)
    
    def decompress(self, data: bytes) -> bytes:
        return gzip.decompress(data)
    
    @property
    def type(self) -> CompressionType:
        return CompressionType.GZIP


class Bzip2Compressor(Compressor):
    """Bzip2 compression."""
    
    def __init__(self, level: int = 9):
        self.level = max(1, min(9, level))
    
    def compress(self, data: bytes) -> bytes:
        return bz2.compress(data, compresslevel=self.level)
    
    def decompress(self, data: bytes) -> bytes:
        return bz2.decompress(data)
    
    @property
    def type(self) -> CompressionType:
        return CompressionType.BZIP2


class LzmaCompressor(Compressor):
    """LZMA compression."""
    
    def __init__(self, preset: int = 6):
        self.preset = max(0, min(9, preset))
    
    def compress(self, data: bytes) -> bytes:
        return lzma.compress(data, preset=self.preset)
    
    def decompress(self, data: bytes) -> bytes:
        return lzma.decompress(data)
    
    @property
    def type(self) -> CompressionType:
        return CompressionType.LZMA


class Lz4Compressor(Compressor):
    """LZ4 compression (optional, fast compression)."""
    
    def __init__(self, level: int = 0):
        try:
            import lz4.frame
            self.lz4 = lz4.frame
            self.level = level
        except ImportError:
            raise ImportError("lz4 is required for LZ4 compression. Install with: pip install lz4")
    
    def compress(self, data: bytes) -> bytes:
        return self.lz4.compress(data, compression_level=self.level)
    
    def decompress(self, data: bytes) -> bytes:
        return self.lz4.decompress(data)
    
    @property
    def type(self) -> CompressionType:
        return CompressionType.LZ4


class ZstdCompressor(Compressor):
    """Zstandard compression (optional, modern compression)."""
    
    def __init__(self, level: int = 3):
        try:
            import zstandard as zstd
            self.zstd = zstd
            self.level = level
        except ImportError:
            raise ImportError("zstandard is required for ZSTD compression. Install with: pip install zstandard")
    
    def compress(self, data: bytes) -> bytes:
        cctx = self.zstd.ZstdCompressor(level=self.level)
        return cctx.compress(data)
    
    def decompress(self, data: bytes) -> bytes:
        dctx = self.zstd.ZstdDecompressor()
        return dctx.decompress(data)
    
    @property
    def type(self) -> CompressionType:
        return CompressionType.ZSTD


class CompressionManager:
    """Manages compression for SignLedger entries."""
    
    def __init__(
        self,
        default_type: CompressionType = CompressionType.ZLIB,
        compression_threshold: int = 1024,  # Don't compress below this size
        auto_select: bool = False
    ):
        self.default_type = default_type
        self.compression_threshold = compression_threshold
        self.auto_select = auto_select
        self.stats = CompressionStats()
        
        # Initialize compressors
        self._compressors = {
            CompressionType.NONE: NoCompressor(),
            CompressionType.ZLIB: ZlibCompressor(),
            CompressionType.GZIP: GzipCompressor(),
            CompressionType.BZIP2: Bzip2Compressor(),
            CompressionType.LZMA: LzmaCompressor(),
        }
        
        # Try to add optional compressors
        try:
            self._compressors[CompressionType.LZ4] = Lz4Compressor()
        except ImportError:
            logger.debug("LZ4 compression not available")
        
        try:
            self._compressors[CompressionType.ZSTD] = ZstdCompressor()
        except ImportError:
            logger.debug("Zstandard compression not available")
    
    def compress_entry(self, entry_data: Dict[str, Any]) -> Tuple[bytes, Dict[str, Any]]:
        """Compress an entry and return compressed data with metadata."""
        # Serialize entry data
        serialized = json.dumps(entry_data, separators=(',', ':')).encode('utf-8')
        original_size = len(serialized)
        
        # Check if compression is worthwhile
        if original_size < self.compression_threshold:
            self.stats.total_compressed += 1
            self.stats.bytes_before_compression += original_size
            self.stats.bytes_after_compression += original_size
            
            return serialized, {
                'compression': CompressionType.NONE.value,
                'original_size': original_size,
                'compressed_size': original_size,
            }
        
        # Select compression type
        if self.auto_select:
            compression_type = self._auto_select_compression(serialized)
        else:
            compression_type = self.default_type
        
        # Compress
        try:
            compressor = self._compressors.get(compression_type)
            if not compressor:
                raise ValueError(f"Compressor not available: {compression_type}")
            
            compressed = compressor.compress(serialized)
            compressed_size = len(compressed)
            
            # Update stats
            self.stats.total_compressed += 1
            self.stats.bytes_before_compression += original_size
            self.stats.bytes_after_compression += compressed_size
            
            # Return compressed data with metadata
            return compressed, {
                'compression': compression_type.value,
                'original_size': original_size,
                'compressed_size': compressed_size,
                'compression_ratio': 1.0 - (compressed_size / original_size),
            }
            
        except Exception as e:
            logger.error(f"Compression failed: {e}")
            self.stats.compression_errors += 1
            
            # Fall back to uncompressed
            return serialized, {
                'compression': CompressionType.NONE.value,
                'original_size': original_size,
                'compressed_size': original_size,
                'error': str(e),
            }
    
    def decompress_entry(self, compressed_data: bytes, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Decompress an entry using metadata."""
        compression_type_str = metadata.get('compression', CompressionType.NONE.value)
        
        try:
            # Get compression type
            compression_type = CompressionType(compression_type_str)
            
            # Get compressor
            compressor = self._compressors.get(compression_type)
            if not compressor:
                raise ValueError(f"Compressor not available: {compression_type}")
            
            # Decompress
            decompressed = compressor.decompress(compressed_data)
            
            # Update stats
            self.stats.total_decompressed += 1
            
            # Parse JSON
            return json.loads(decompressed.decode('utf-8'))
            
        except Exception as e:
            logger.error(f"Decompression failed: {e}")
            self.stats.decompression_errors += 1
            raise
    
    def _auto_select_compression(self, data: bytes) -> CompressionType:
        """Automatically select best compression type."""
        # Simple heuristic: test a few algorithms on a sample
        sample_size = min(len(data), 10240)  # Test on first 10KB
        sample = data[:sample_size]
        
        best_type = CompressionType.NONE
        best_ratio = 0.0
        
        # Test each available compressor
        for comp_type, compressor in self._compressors.items():
            if comp_type == CompressionType.NONE:
                continue
            
            try:
                compressed = compressor.compress(sample)
                ratio = 1.0 - (len(compressed) / len(sample))
                
                # Prefer faster algorithms for small improvements
                if comp_type == CompressionType.LZ4:
                    ratio *= 1.1  # 10% bonus for speed
                elif comp_type == CompressionType.ZLIB:
                    ratio *= 1.05  # 5% bonus for good balance
                
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_type = comp_type
                    
            except Exception:
                continue
        
        # Only compress if we get at least 20% reduction
        if best_ratio < 0.2:
            return CompressionType.NONE
        
        return best_type
    
    def get_stats(self) -> Dict[str, Any]:
        """Get compression statistics."""
        return self.stats.to_dict()
    
    def reset_stats(self):
        """Reset statistics."""
        self.stats = CompressionStats()


class CompressedStorageWrapper:
    """Wrapper for storage backends to add compression support."""
    
    def __init__(self, backend, compression_manager: Optional[CompressionManager] = None):
        self.backend = backend
        self.compression_manager = compression_manager or CompressionManager()
    
    def append(self, entry_data: Dict[str, Any]) -> int:
        """Append compressed entry."""
        # Compress entry
        compressed_data, compression_metadata = self.compression_manager.compress_entry(entry_data)
        
        # Create wrapper entry
        wrapped_entry = {
            'compressed_data': base64.b64encode(compressed_data).decode('utf-8'),
            'compression_metadata': compression_metadata,
            'sequence': entry_data.get('sequence'),
            'timestamp': entry_data.get('timestamp'),
        }
        
        # Store wrapped entry
        return self.backend.append(wrapped_entry)
    
    def get(self, sequence: int) -> Optional[Dict[str, Any]]:
        """Get and decompress entry."""
        wrapped_entry = self.backend.get(sequence)
        if not wrapped_entry:
            return None
        
        # Extract compressed data
        compressed_data = base64.b64decode(wrapped_entry['compressed_data'])
        compression_metadata = wrapped_entry['compression_metadata']
        
        # Decompress
        return self.compression_manager.decompress_entry(compressed_data, compression_metadata)
    
    def get_latest(self) -> Optional[Dict[str, Any]]:
        """Get latest entry."""
        wrapped_entry = self.backend.get_latest()
        if not wrapped_entry:
            return None
        
        # Extract and decompress
        compressed_data = base64.b64decode(wrapped_entry['compressed_data'])
        compression_metadata = wrapped_entry['compression_metadata']
        
        return self.compression_manager.decompress_entry(compressed_data, compression_metadata)
    
    def get_range(self, start: int, end: int) -> List[Dict[str, Any]]:
        """Get range of entries."""
        wrapped_entries = self.backend.get_range(start, end)
        
        decompressed_entries = []
        for wrapped in wrapped_entries:
            compressed_data = base64.b64decode(wrapped['compressed_data'])
            compression_metadata = wrapped['compression_metadata']
            
            try:
                entry = self.compression_manager.decompress_entry(compressed_data, compression_metadata)
                decompressed_entries.append(entry)
            except Exception as e:
                logger.error(f"Failed to decompress entry: {e}")
        
        return decompressed_entries
    
    def close(self):
        """Close backend."""
        if hasattr(self.backend, 'close'):
            self.backend.close()