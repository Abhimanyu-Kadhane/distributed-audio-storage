"""Background health monitor for the gateway.

LAB TOPIC 2: multithreading — a daemon thread runs independently of request
handlers, polling every node at a fixed interval and updating a shared
thread-safe state dictionary.

LAB TOPIC 8: Failover & Primary Promotion
A node is marked alive=False after HEALTH_FAIL_THRESHOLD consecutive failures
and alive=True after any one success. When transitioning alive->dead, the gateway
promotes alive backups to primaries for chunks owned by the dead node.
"""
import logging
import threading
import time
from typing import Dict, Optional, Set

from app.config import (
    HEALTH_FAIL_THRESHOLD,
    HEALTH_INTERVAL_SECONDS,
    NODES,
)
from app.events import record_event
from app.node_client import NodeError, gateway_clock, get_health

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared state — protected by _lock (LAB TOPIC 2)
# ---------------------------------------------------------------------------
_lock = threading.Lock()

_state: Dict[int, dict] = {
    node_id: {
        "alive": True,
        "fail_streak": 0,
        "chunk_count": 0,
        "bytes_used": 0,
        "active_requests": 0,
        "leader_id": None,
    }
    for node_id in NODES
}


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def is_alive(node_id: int) -> bool:
    with _lock:
        return _state[node_id]["alive"]


def get_alive_node_ids() -> Set[int]:
    """Return set of currently alive node IDs."""
    with _lock:
        return {nid for nid, s in _state.items() if s["alive"]}


def mark_dead(node_id: int) -> None:
    """Called by upload/download code when a node I/O call fails or admin fail."""
    transitioned = False
    with _lock:
        if _state[node_id]["alive"]:
            logger.warning("Marking node %d dead", node_id)
            transitioned = True
            _state[node_id]["alive"] = False
            _state[node_id]["fail_streak"] = HEALTH_FAIL_THRESHOLD
        else:
            _state[node_id]["fail_streak"] = HEALTH_FAIL_THRESHOLD

    if transitioned:
        record_event(
            actor="gateway",
            event_type="failure",
            detail=f"Node {node_id} marked dead",
            lamport=gateway_clock.value(),
        )
        # LAB TOPIC 8: promote backups for any chunks where dead node was primary
        _run_promotion(node_id)


def mark_alive_immediate(node_id: int) -> None:
    """Directly probe node after admin recover to restore liveness without delay."""
    try:
        payload = get_health(node_id)
        transitioned = False
        with _lock:
            was_alive = _state[node_id]["alive"]
            _state[node_id].update(
                {
                    "alive": True,
                    "fail_streak": 0,
                    "chunk_count": payload.get("chunk_count", 0),
                    "bytes_used": payload.get("bytes_used", 0),
                    "active_requests": payload.get("active_requests", 0),
                    "leader_id": payload.get("leader_id"),
                }
            )
            if not was_alive:
                transitioned = True

        if transitioned:
            record_event(
                actor="gateway",
                event_type="recovery",
                detail=f"Node {node_id} is now ALIVE",
                lamport=gateway_clock.value(),
            )
        try:
            from app.balancer import on_node_health_polled
            on_node_health_polled(node_id)
        except Exception:
            pass
    except Exception as exc:
        logger.debug("Immediate health probe after recover on node %d failed: %s", node_id, exc)


def _run_promotion(dead_node_id: int) -> None:
    """Execute primary promotion outside locks."""
    try:
        from app.db import promote_primaries
        alive_ids = get_alive_node_ids()
        promoted = promote_primaries(dead_node_id, alive_ids)
        if promoted > 0:
            logger.info("Promoted %d chunks from dead node %d", promoted, dead_node_id)
            record_event(
                actor="gateway",
                event_type="promotion",
                detail=f"promoted {promoted} chunks from node {dead_node_id}",
                lamport=gateway_clock.value(),
            )
    except Exception as exc:
        logger.error("Error during primary promotion for node %d: %s", dead_node_id, exc)


def get_all_status() -> Dict[int, dict]:
    """Return a snapshot of the entire node state dict."""
    with _lock:
        return {nid: dict(s) for nid, s in _state.items()}


# ---------------------------------------------------------------------------
# Background polling loop
# ---------------------------------------------------------------------------

def _poll_once() -> None:
    """Poll each node once; update alive/fail_streak under the lock."""
    for node_id in NODES:
        transition_dead = False
        transition_alive = False
        try:
            payload = get_health(node_id)
            with _lock:
                was_alive = _state[node_id]["alive"]
                _state[node_id].update(
                    {
                        "alive": True,
                        "fail_streak": 0,
                        "chunk_count": payload.get("chunk_count", 0),
                        "bytes_used": payload.get("bytes_used", 0),
                        "active_requests": payload.get("active_requests", 0),
                        "leader_id": payload.get("leader_id"),
                    }
                )
                if not was_alive:
                    transition_alive = True
            try:
                from app.balancer import on_node_health_polled
                on_node_health_polled(node_id)
            except Exception:
                pass
        except NodeError:
            with _lock:
                s = _state[node_id]
                s["fail_streak"] += 1
                if s["fail_streak"] >= HEALTH_FAIL_THRESHOLD and s["alive"]:
                    logger.warning(
                        "Node %d is now DEAD (fail_streak=%d)",
                        node_id,
                        s["fail_streak"],
                    )
                    s["alive"] = False
                    transition_dead = True

        if transition_alive:
            record_event(
                actor="gateway",
                event_type="recovery",
                detail=f"Node {node_id} is now ALIVE",
                lamport=gateway_clock.value(),
            )

        if transition_dead:
            record_event(
                actor="gateway",
                event_type="failure",
                detail=f"Node {node_id} marked dead (health poll failed)",
                lamport=gateway_clock.value(),
            )
            _run_promotion(node_id)


def _monitor_loop() -> None:
    """Daemon loop — runs forever, polling every HEALTH_INTERVAL_SECONDS."""
    logger.info(
        "Health monitor started: interval=%.1fs threshold=%d",
        HEALTH_INTERVAL_SECONDS,
        HEALTH_FAIL_THRESHOLD,
    )
    while True:
        _poll_once()
        time.sleep(HEALTH_INTERVAL_SECONDS)


def start_monitor() -> None:
    """Spawn the daemon thread. Called once at gateway startup."""
    t = threading.Thread(target=_monitor_loop, daemon=True, name="health-monitor")
    t.start()
    logger.info("Health monitor thread started")
