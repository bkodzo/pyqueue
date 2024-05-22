"""Message compression utilities."""

import gzip
import zstandard as zstd
from typing import Optional
from .config import Config


class Compressor:
    """Handles message compression and decompression."""
    
    def __init__(self, config: Config):
        self.enabled = config.get("compression", "enabled", True)
        self.algorithm = config.get("compression", "algorithm", "gzip")
        self.min_size = config.get("compression", "min_size_bytes", 1024)
        self.zstd_compressor = None
        self.zstd_decompressor = None
        
        if self.algorithm == "zstd":
            self.zstd_compressor = zstd.ZstdCompressor()
            self.zstd_decompressor = zstd.ZstdDecompressor()
    
    def compress(self, data: bytes) -> tuple[bytes, bool]:
        """Compress data if it exceeds minimum size. Returns (data, was_compressed)."""
        if not self.enabled or len(data) < self.min_size:
            return data, False
        
        try:
            if self.algorithm == "gzip":
                compressed = gzip.compress(data)
            elif self.algorithm == "zstd":
                compressed = self.zstd_compressor.compress(data)
            else:
                return data, False
            
            if len(compressed) < len(data):
                return compressed, True
            return data, False
        except Exception:
            return data, False
    
    def decompress(self, data: bytes, was_compressed: bool) -> bytes:
        """Decompress data if it was compressed."""
        if not was_compressed:
            return data
        
        try:
            if self.algorithm == "gzip":
                return gzip.decompress(data)
            elif self.algorithm == "zstd":
                return self.zstd_decompressor.decompress(data)
            return data
        except Exception:
            return data

