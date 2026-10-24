"""Leader-run failure detector.

LAB TOPIC 8: Failure detection
Active only while this node is leader and not failed.
Every 2s probes GET /health on all peers concurrently.
A peer is marked DEAD after 3 consecutive failures and ALIVE after 1 success.
Maintains a thread-safe liveness table and reports state transitions to gateway.
"""
from concurrent.futures import ThreadPoolExecutor
import logging
import threading
import time
from typing import Dict, Optional

import requests

from app.config import ALL_NODES, NODE_ID, PEERS
from app.election import is_leader, send_gateway_event
from app.state import is_failed
from app.storage import get_stats

logger = logging.getLogger(__name__)

# LAB TOPIC 8: thread-safe liveness table
_lock = threading.Lock()
_liveness: Dict[int, dict] = {
    nid: {
        "alive": True,
        "fail_streak": 0,
        "health_payload": {},
    }
    for nid in ALL_NODES
}


def is_node_alive(node_id: int) -> bool:
    """Return True if node_id is considered alive by this detector."""
    if node_id == NODE_ID:
        return not is_failed()
    with _lock:
        info = _liveness.get(node_id)
        return info["alive"] if info else False


def get_node_health_info(node_id: int) -> dict:
    """Return the latest cached health payload or live stats for self."""
    if node_id == NODE_ID:
        from app.main import _get_active
        chunks, bytes_used = get_stats()
        return {
            "chunk_count": chunks,
            "bytes_used": bytes_used,
            "active_requests": _get_active(),
        }
    with _lock:
        info = _liveness.get(node_id)
        return dict(info.get("health_payload", {})) if info else {}


def _probe_peer(peer_id: int, peer_url: str) -> None:
    """Probe a single peer and update its liveness under lock."""
    url = f"{peer_url.rstrip('/')}/health"
    try:
        resp = requests.get(url, timeout=1.5)
        if resp.status_code == 200:
            payload = resp.json()
            transition_alive = False
            with _lock:
                entry = _liveness[peer_id]
                was_alive = entry["alive"]
                entry["alive"] = True
                entry["fail_streak"] = 0
                entry["health_payload"] = payload
                if not was_alive:
                    transition_alive = True

            if transition_alive:
                logger.info("Leader node-%d: peer node-%d is now ALIVE", NODE_ID, peer_id)
                send_gateway_event(
                    "failure",
                    f"Leader node-{NODE_ID} detected node-{peer_id} transitioned to ALIVE",
                )
            return
    except Exception:
        pass

    # Probe failed or returned error status
    transition_dead = False
    with _lock:
        entry = _liveness[peer_id]
        entry["fail_streak"] += 1
        if entry["fail_streak"] >= 3 and entry["alive"]:
            entry["alive"] = False
            transition_dead = True

    if transition_dead:
        logger.warning(
            "Leader node-%d: peer node-%d is now DEAD (fail_streak=%d)",
            NODE_ID,
            peer_id,
            entry["fail_streak"],
        )
        send_gateway_event(
            "failure",
            f"Leader node-{NODE_ID} detected node-{peer_id} transitioned to DEAD (fail_streak=3)",
        )


def _detector_loop() -> None:
    """Loop running every 2s while this node is leader and not failed."""
    logger.info("node-%d: failure detector thread started", NODE_ID)
    while True:
        time.sleep(2.0)
        if is_failed() or not is_leader():
            continue

        if not PEERS:
            continue

        with ThreadPoolExecutor(max_workers=len(PEERS)) as pool:
            for nid, url in PEERS.items():
                pool.submit(_probe_peer, nid, url)


def start_failure_detector() -> None:
    """Start failure detector daemon thread at FastAPI startup."""
    t = threading.Thread(target=_detector_loop, daemon=True, name=f"failure-detector-{NODE_ID}")
    t.start()
