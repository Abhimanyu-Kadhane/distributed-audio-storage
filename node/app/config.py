"""Node configuration — all values read from environment variables."""
import os
from typing import Dict


# LAB TOPIC 1: RPC mechanism — node identity used in all responses
NODE_ID: int = int(os.environ.get("NODE_ID", "0"))

# Directory where chunk files are persisted
DATA_DIR: str = os.environ.get("DATA_DIR", "/data/chunks")


def _parse_peers(raw: str) -> Dict[int, str]:
    """Parse 'id=url,id=url,...' into {id: url}."""
    peers: Dict[int, str] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        nid_str, url = part.split("=", 1)
        peers[int(nid_str.strip())] = url.strip()
    return peers


_RAW_PEERS = os.environ.get(
    "PEERS",
    "1=http://node1:8000,2=http://node2:8000,3=http://node3:8000,4=http://node4:8000",
)

# All storage nodes in the cluster
ALL_NODES: Dict[int, str] = _parse_peers(_RAW_PEERS)

# LAB TOPIC 4: Bully election — peer nodes (excluding self)
PEERS: Dict[int, str] = {nid: url for nid, url in ALL_NODES.items() if nid != NODE_ID}

# Gateway endpoint
GATEWAY_URL: str = os.environ.get("GATEWAY_URL", "http://gateway:8000").rstrip("/")

# Election & heartbeat parameters (LAB TOPIC 4)
HEARTBEAT_INTERVAL_SECONDS: float = float(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", "2"))
LEADER_TIMEOUT_SECONDS: float = float(os.environ.get("LEADER_TIMEOUT_SECONDS", "6"))
ELECTION_TIMEOUT_SECONDS: float = float(os.environ.get("ELECTION_TIMEOUT_SECONDS", "2"))

# Repair parameters (LAB TOPIC 8)
REPLICATION_FACTOR: int = int(os.environ.get("REPLICATION_FACTOR", "2"))
REPAIR_INTERVAL_SECONDS: float = float(os.environ.get("REPAIR_INTERVAL_SECONDS", "5"))
REPAIR_BATCH: int = int(os.environ.get("REPAIR_BATCH", "20"))

