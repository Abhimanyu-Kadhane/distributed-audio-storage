"""RPC wrappers around the storage node HTTP API.

LAB TOPIC 1: RPC via REST/HTTP — every function here is one remote procedure call.
LAB TOPIC 3: Lamport logical clocks — clock.tick() on send, clock.update() on receive.

All calls use requests with (connect, read) timeouts so they never block forever.
A NodeError is raised on any transport or HTTP error, letting callers handle
node failures uniformly (retry on another node, mark node dead, etc.).
"""
import logging
from typing import Optional

import requests

from app.config import NODES, TIMEOUT
from app.lamport import LamportClock

logger = logging.getLogger(__name__)

# LAB TOPIC 3: Gateway Lamport clock instance
gateway_clock = LamportClock()


class NodeError(Exception):
    """Raised when a node call fails for any reason."""


def _url(node_id: int, path: str) -> str:
    base = NODES[node_id].rstrip("/")
    return f"{base}{path}"


def _update_clock_from_response(resp: requests.Response) -> None:
    """Update gateway clock if node response includes X-Lamport-Time header."""
    hdr = resp.headers.get("X-Lamport-Time")
    if hdr:
        try:
            gateway_clock.update(int(hdr))
        except ValueError:
            pass


def put_chunk(
    node_id: int,
    chunk_id: str,
    data: bytes,
    sha256: str,
    role: str = "primary",
    backup_url: Optional[str] = None,
    backup_node_id: Optional[int] = None,
) -> dict:
    """LAB TOPIC 1: RPC — PUT /chunks/{chunk_id} on the target node.
    
    LAB TOPIC 5: Replication — sends X-Role and backup destination headers.
    LAB TOPIC 3: Lamport logical clocks — includes X-Lamport-Time header.
    """
    url = _url(node_id, f"/chunks/{chunk_id}")
    send_time = gateway_clock.tick()
    headers = {
        "Content-Type": "application/octet-stream",
        "X-Chunk-Sha256": sha256,
        "X-Role": role,
        "X-Lamport-Time": str(send_time),
    }
    if backup_url:
        headers["X-Backup-Url"] = backup_url
    if backup_node_id is not None:
        headers["X-Backup-Node-Id"] = str(backup_node_id)

    try:
        resp = requests.put(
            url,
            data=data,
            headers=headers,
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        _update_clock_from_response(resp)
        return resp.json()
    except requests.RequestException as exc:
        raise NodeError(f"put_chunk node={node_id} chunk={chunk_id}: {exc}") from exc


def get_chunk(node_id: int, chunk_id: str) -> bytes:
    """LAB TOPIC 1: RPC — GET /chunks/{chunk_id} from a node.
    
    LAB TOPIC 3: Lamport logical clocks — includes X-Lamport-Time header.
    """
    url = _url(node_id, f"/chunks/{chunk_id}")
    send_time = gateway_clock.tick()
    try:
        resp = requests.get(
            url,
            headers={"X-Lamport-Time": str(send_time)},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        _update_clock_from_response(resp)
        return resp.content
    except requests.RequestException as exc:
        raise NodeError(f"get_chunk node={node_id} chunk={chunk_id}: {exc}") from exc


def delete_chunk(node_id: int, chunk_id: str) -> None:
    """LAB TOPIC 1: RPC — DELETE /chunks/{chunk_id} on a node (best-effort).
    
    LAB TOPIC 3: Lamport logical clocks — includes X-Lamport-Time header.
    """
    url = _url(node_id, f"/chunks/{chunk_id}")
    send_time = gateway_clock.tick()
    try:
        resp = requests.delete(
            url,
            headers={"X-Lamport-Time": str(send_time)},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        _update_clock_from_response(resp)
    except requests.RequestException as exc:
        raise NodeError(f"delete_chunk node={node_id} chunk={chunk_id}: {exc}") from exc


def get_health(node_id: int) -> dict:
    """LAB TOPIC 1: RPC — GET /health from a node.
    
    LAB TOPIC 3: Lamport logical clocks — includes X-Lamport-Time header.
    """
    url = _url(node_id, "/health")
    send_time = gateway_clock.tick()
    try:
        resp = requests.get(
            url,
            headers={"X-Lamport-Time": str(send_time)},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        _update_clock_from_response(resp)
        return resp.json()
    except requests.RequestException as exc:
        raise NodeError(f"get_health node={node_id}: {exc}") from exc
