"""Gateway configuration — all values from environment variables.

No value is hardcoded; everything has a documented default.
"""
import os
from typing import Dict


def _parse_nodes(raw: str) -> Dict[int, str]:
    """Parse 'id=url,id=url,...' into {id: url}."""
    nodes: Dict[int, str] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        node_id_str, url = part.split("=", 1)
        nodes[int(node_id_str.strip())] = url.strip()
    return nodes


_RAW_NODES = os.environ.get(
    "NODES",
    "1=http://node1:8000,2=http://node2:8000,3=http://node3:8000,4=http://node4:8000",
)

# {node_id: url}
NODES: Dict[int, str] = _parse_nodes(_RAW_NODES)

# Size of each chunk in bytes (512 KB default — small to force multi-chunk splits)
CHUNK_SIZE_BYTES: int = int(os.environ.get("CHUNK_SIZE_BYTES", "524288"))

# Phase 2: replication factor defaults to 2
REPLICATION_FACTOR: int = int(os.environ.get("REPLICATION_FACTOR", "2"))

# LAB TOPIC 6: Load balancing algorithm — "least_loaded" (default) or "round_robin"
LB_ALGORITHM: str = os.environ.get("LB_ALGORITHM", "least_loaded").lower()

# Thread pool size for parallel chunk upload / download
MAX_WORKERS: int = int(os.environ.get("MAX_WORKERS", "8"))

# Maximum accepted upload size in bytes (50 MB)
MAX_UPLOAD_BYTES: int = int(os.environ.get("MAX_UPLOAD_BYTES", "52428800"))

# SQLite database path
DB_PATH: str = os.environ.get("DB_PATH", "/data/metadata.db")

# Health monitor settings
HEALTH_INTERVAL_SECONDS: float = float(os.environ.get("HEALTH_INTERVAL_SECONDS", "2"))
HEALTH_FAIL_THRESHOLD: int = int(os.environ.get("HEALTH_FAIL_THRESHOLD", "2"))

# requests timeouts: (connect_seconds, read_seconds)
REQUEST_CONNECT_TIMEOUT: float = float(os.environ.get("REQUEST_CONNECT_TIMEOUT", "2"))
REQUEST_READ_TIMEOUT: float = float(os.environ.get("REQUEST_READ_TIMEOUT", "10"))

TIMEOUT = (REQUEST_CONNECT_TIMEOUT, REQUEST_READ_TIMEOUT)

# Allowed audio file extensions and their MIME types
ALLOWED_EXTENSIONS: Dict[str, str] = {
    "mp3": "audio/mpeg",
    "wav": "audio/wav",
    "ogg": "audio/ogg",
    "flac": "audio/flac",
    "m4a": "audio/mp4",
}
