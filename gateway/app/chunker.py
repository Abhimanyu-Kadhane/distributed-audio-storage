"""Chunk split and merge helpers for the gateway.

LAB TOPIC 2: multithreading — chunker produces the list of (idx, data)
pairs that are then uploaded in parallel by the ThreadPoolExecutor in main.py.
"""
import hashlib
from typing import List, Tuple

from app.config import CHUNK_SIZE_BYTES


def split(data: bytes, chunk_size: int = CHUNK_SIZE_BYTES) -> List[Tuple[int, bytes]]:
    """Split *data* into a list of (index, chunk_bytes) tuples.

    The last chunk may be smaller than chunk_size.
    """
    chunks: List[Tuple[int, bytes]] = []
    idx = 0
    offset = 0
    while offset < len(data):
        chunk = data[offset : offset + chunk_size]
        chunks.append((idx, chunk))
        offset += chunk_size
        idx += 1
    return chunks


def sha256_of(data: bytes) -> str:
    """Return the hex SHA-256 digest of *data*."""
    return hashlib.sha256(data).hexdigest()


def merge(chunks_in_order: List[bytes]) -> bytes:
    """Concatenate chunk bytes in index order to reconstruct the original file."""
    return b"".join(chunks_in_order)
