"""Chunk placement and read-node selection.

LAB TOPIC 6: Load balancing
Supports "least_loaded" (default) and "round_robin" algorithms.

Placement (least_loaded):
Scores alive nodes by (reported chunk_count + unpolled acknowledged chunks + in-flight chunks)
ascending, tie-breaking by bytes_used, then node_id.
Maintains thread-safe in-memory reservation counters per node so parallel uploads
distribute chunks evenly across all nodes.

Read selection:
Chooses the replica with the lowest (active_requests from health poll +
gateway-local in-flight counter).
"""
import logging
import threading
from typing import Iterable, List, Optional, Set

from app.config import LB_ALGORITHM, NODES
from app.health_monitor import get_all_status, is_alive

logger = logging.getLogger(__name__)

# LAB TOPIC 2: multithreading — lock protects reservations, download counters, and RR index
_lock = threading.Lock()
_in_flight: dict[int, int] = {nid: 0 for nid in NODES}
_pending_ack: dict[int, int] = {nid: 0 for nid in NODES}
_in_flight_downloads: dict[int, int] = {nid: 0 for nid in NODES}
_rr_counter: int = 0


def get_active_algorithm() -> str:
    """Return the active load balancing algorithm name."""
    return LB_ALGORITHM


def choose_nodes(count: int, exclude: Optional[Set[int]] = None) -> List[int]:
    """Return up to *count* distinct alive node IDs based on active LB algorithm.

    Primary node is index 0, backup is index 1 (if available).
    Increments gateway-side in-flight counters for chosen nodes under lock.
    """
    global _rr_counter
    if exclude is None:
        exclude = set()

    all_node_ids = sorted(NODES.keys())
    with _lock:
        alive_candidates = [
            nid for nid in all_node_ids if is_alive(nid) and nid not in exclude
        ]

        if not alive_candidates:
            raise RuntimeError(
                f"No alive nodes available for placement (exclude={exclude})"
            )

        k = min(count, len(alive_candidates))

        if LB_ALGORITHM == "round_robin":
            # LAB TOPIC 6: Round-robin placement
            n = len(alive_candidates)
            chosen: List[int] = []
            seen: Set[int] = set()
            start = _rr_counter
            _rr_counter = (_rr_counter + k) % max(1, n)
            for i in range(k):
                node = alive_candidates[(start + i) % n]
                attempt = 0
                while node in seen and attempt < n:
                    node = alive_candidates[(start + i + attempt) % n]
                    attempt += 1
                chosen.append(node)
                seen.add(node)
        else:
            # LAB TOPIC 6: Least-loaded placement
            # Score = (reported chunk_count + pending unpolled + in-flight chunks)
            # Tie-break by bytes_used, then node_id
            status_map = get_all_status()

            def _score(nid: int) -> tuple[int, int, int]:
                s = status_map.get(nid, {})
                reported_chunks = s.get("chunk_count", 0)
                pending = _pending_ack.get(nid, 0)
                in_flight = _in_flight.get(nid, 0)
                bytes_used = s.get("bytes_used", 0)
                return (reported_chunks + pending + in_flight, bytes_used, nid)

            sorted_candidates = sorted(alive_candidates, key=_score)
            chosen = sorted_candidates[:k]

        # Increment in-flight reservation counter for chosen nodes
        for nid in chosen:
            _in_flight[nid] = _in_flight.get(nid, 0) + 1

        return chosen


def chunk_upload_succeeded(stored_node_ids: Iterable[int], unack_node_ids: Optional[Iterable[int]] = None) -> None:
    """Transition in-flight chunk reservation to pending unpolled chunks."""
    with _lock:
        for nid in stored_node_ids:
            if nid in _in_flight:
                _in_flight[nid] = max(0, _in_flight[nid] - 1)
            _pending_ack[nid] = _pending_ack.get(nid, 0) + 1
        if unack_node_ids:
            for nid in unack_node_ids:
                if nid in _in_flight:
                    _in_flight[nid] = max(0, _in_flight[nid] - 1)


def chunk_upload_failed(node_ids: Iterable[int]) -> None:
    """Release in-flight reservation on failed upload."""
    with _lock:
        for nid in node_ids:
            if nid in _in_flight:
                _in_flight[nid] = max(0, _in_flight[nid] - 1)


def release_reservation(node_ids: Iterable[int]) -> None:
    """Release reservation (alias for chunk_upload_failed)."""
    chunk_upload_failed(node_ids)


def on_node_health_polled(node_id: int) -> None:
    """Reset pending unpolled count when health monitor gets fresh count from node."""
    with _lock:
        _pending_ack[node_id] = 0


def order_read_replicas(replica_node_ids: List[int]) -> List[int]:
    """Order alive replicas of a chunk for reading (least-loaded active requests first)."""
    alive_replicas = [nid for nid in replica_node_ids if is_alive(nid)]
    if not alive_replicas:
        return []

    with _lock:
        status_map = get_all_status()

        def _read_score(nid: int) -> tuple[int, int]:
            s = status_map.get(nid, {})
            active_reported = s.get("active_requests", 0)
            in_flight = _in_flight_downloads.get(nid, 0)
            return (active_reported + in_flight, nid)

        return sorted(alive_replicas, key=_read_score)


def acquire_download(node_id: int) -> None:
    """Increment gateway-local in-flight download counter for node."""
    with _lock:
        _in_flight_downloads[node_id] = _in_flight_downloads.get(node_id, 0) + 1


def release_download(node_id: int) -> None:
    """Decrement gateway-local in-flight download counter for node."""
    with _lock:
        if node_id in _in_flight_downloads:
            _in_flight_downloads[node_id] = max(0, _in_flight_downloads[node_id] - 1)
