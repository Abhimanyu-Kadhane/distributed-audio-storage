"""Disk-based chunk storage for one storage node.

Each chunk is written as two files:
  <DATA_DIR>/<chunk_id>.bin  — raw bytes
  <DATA_DIR>/<chunk_id>.json — sidecar {size, sha256}

All mutations are protected by a single threading.Lock so concurrent
FastAPI handler threads (Lab Topic 2) never race on the in-memory counters.
"""
import hashlib
import json
import logging
import os
import re
import tempfile
import threading
from typing import Optional

from app.config import DATA_DIR, NODE_ID

logger = logging.getLogger(__name__)

# Only safe characters allowed in chunk ids to prevent path traversal
_CHUNK_ID_RE = re.compile(r"^[A-Za-z0-9_\-]+$")

# LAB TOPIC 2: multithreading — all shared counters protected by this lock
_lock = threading.Lock()
_chunk_count: int = 0
_bytes_used: int = 0


def _chunk_path(chunk_id: str) -> str:
    return os.path.join(DATA_DIR, f"{chunk_id}.bin")


def _meta_path(chunk_id: str) -> str:
    return os.path.join(DATA_DIR, f"{chunk_id}.json")


def validate_chunk_id(chunk_id: str) -> bool:
    """Return True if chunk_id contains only safe characters."""
    return bool(_CHUNK_ID_RE.match(chunk_id))


def init_storage() -> None:
    """Create DATA_DIR and rebuild in-memory counters from disk on startup."""
    global _chunk_count, _bytes_used
    os.makedirs(DATA_DIR, exist_ok=True)
    count = 0
    total = 0
    for fname in os.listdir(DATA_DIR):
        if fname.endswith(".bin"):
            size = os.path.getsize(os.path.join(DATA_DIR, fname))
            count += 1
            total += size
    with _lock:
        _chunk_count = count
        _bytes_used = total
    logger.info("storage init: %d chunks, %d bytes", count, total)


def store_chunk(chunk_id: str, data: bytes, expected_sha256: str) -> dict:
    """Write chunk atomically; raise ValueError on checksum mismatch."""
    actual = hashlib.sha256(data).hexdigest()
    if actual != expected_sha256:
        raise ValueError(
            f"SHA-256 mismatch: expected {expected_sha256}, got {actual}"
        )

    bin_path = _chunk_path(chunk_id)
    meta_path = _meta_path(chunk_id)

    # Determine if this is an overwrite so we can adjust counters correctly
    old_size: int = 0
    is_overwrite = os.path.exists(bin_path)
    if is_overwrite:
        old_size = os.path.getsize(bin_path)

    # Atomic write: write to temp then os.replace (POSIX-atomic)
    dir_ = DATA_DIR
    with tempfile.NamedTemporaryFile(delete=False, dir=dir_, suffix=".tmp") as tf:
        tf.write(data)
        tmp_bin = tf.name
    os.replace(tmp_bin, bin_path)

    meta = {"size": len(data), "sha256": actual}
    with tempfile.NamedTemporaryFile(
        delete=False, dir=dir_, mode="w", suffix=".tmp"
    ) as tf:
        json.dump(meta, tf)
        tmp_meta = tf.name
    os.replace(tmp_meta, meta_path)

    # Update in-memory counters under lock  (LAB TOPIC 2)
    with _lock:
        global _chunk_count, _bytes_used
        if not is_overwrite:
            _chunk_count += 1
        else:
            _bytes_used -= old_size
        _bytes_used += len(data)

    logger.debug("stored chunk %s (%d bytes)", chunk_id, len(data))
    return {"chunk_id": chunk_id, "size": len(data), "sha256": actual, "node_id": NODE_ID}


def get_chunk(chunk_id: str) -> Optional[bytes]:
    """Return raw bytes or None if chunk does not exist."""
    path = _chunk_path(chunk_id)
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return f.read()


def delete_chunk(chunk_id: str) -> None:
    """Remove chunk files; no-op if chunk is missing (idempotent)."""
    bin_path = _chunk_path(chunk_id)
    meta_path = _meta_path(chunk_id)

    removed = False
    old_size = 0
    if os.path.exists(bin_path):
        old_size = os.path.getsize(bin_path)
        os.remove(bin_path)
        removed = True
    if os.path.exists(meta_path):
        os.remove(meta_path)

    if removed:
        with _lock:
            global _chunk_count, _bytes_used
            _chunk_count = max(0, _chunk_count - 1)
            _bytes_used = max(0, _bytes_used - old_size)
        logger.debug("deleted chunk %s", chunk_id)


def list_chunks() -> list[dict]:
    """Return inventory of all stored chunks."""
    result = []
    for fname in os.listdir(DATA_DIR):
        if not fname.endswith(".bin"):
            continue
        chunk_id = fname[:-4]
        meta_path = _meta_path(chunk_id)
        if os.path.exists(meta_path):
            with open(meta_path) as f:
                meta = json.load(f)
            result.append(
                {
                    "chunk_id": chunk_id,
                    "size": meta["size"],
                    "sha256": meta["sha256"],
                }
            )
    return result


def get_stats() -> tuple[int, int]:
    """Return (chunk_count, bytes_used) from the in-memory counters."""
    with _lock:
        return _chunk_count, _bytes_used
