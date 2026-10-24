"""Bully election algorithm implementation for storage nodes.

LAB TOPIC 4: Bully election
Nodes have unique integer IDs 1..4. The node with the highest active ID wins
the election and becomes the coordinator (leader).

Duties:
- Follower: polls leader liveness via heartbeat monitor; starts election on timeout.
- Leader: sends periodic heartbeats to all peers; runs failure detector and repair.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import random
import threading
import time
from typing import Optional

import requests

from app.config import (
    ELECTION_TIMEOUT_SECONDS,
    GATEWAY_URL,
    HEARTBEAT_INTERVAL_SECONDS,
    LEADER_TIMEOUT_SECONDS,
    NODE_ID,
    PEERS,
)
from app.state import is_failed

logger = logging.getLogger(__name__)

# LAB TOPIC 4: shared election state protected by _lock
_lock = threading.Lock()
_leader_id: Optional[int] = None
_last_heartbeat: float = time.monotonic()
_election_in_progress: bool = False
_coordinator_event: threading.Event = threading.Event()


def get_leader_id() -> Optional[int]:
    """Return current known leader_id or None."""
    with _lock:
        return _leader_id


def is_leader() -> bool:
    """Return True if this node is currently the leader and not failed."""
    if is_failed():
        return False
    with _lock:
        return _leader_id == NODE_ID


def send_gateway_event(event_type: str, detail: str) -> None:
    """Best-effort report of node events to gateway event log."""
    try:
        requests.post(
            f"{GATEWAY_URL}/internal/event",
            json={
                "actor": f"node-{NODE_ID}",
                "type": event_type,
                "detail": detail,
            },
            timeout=2.0,
        )
    except Exception:
        pass


def _broadcast_coordinator() -> None:
    """Send POST /coordinator {leader_id: me} to ALL peers (best effort, concurrent)."""
    def _send_one(peer_id: int, peer_url: str) -> None:
        try:
            requests.post(
                f"{peer_url.rstrip('/')}/coordinator",
                json={"leader_id": NODE_ID},
                timeout=ELECTION_TIMEOUT_SECONDS,
            )
        except Exception:
            pass

    with ThreadPoolExecutor(max_workers=max(1, len(PEERS))) as pool:
        for nid, url in PEERS.items():
            pool.submit(_send_one, nid, url)


def _send_election_message(peer_id: int, peer_url: str) -> bool:
    """Send POST /election {from_id: NODE_ID} to a higher peer."""
    try:
        resp = requests.post(
            f"{peer_url.rstrip('/')}/election",
            json={"from_id": NODE_ID},
            timeout=ELECTION_TIMEOUT_SECONDS,
        )
        if resp.status_code == 200:
            return bool(resp.json().get("ok", False))
    except Exception:
        pass
    return False


def _run_election_thread() -> None:
    """Execute Bully election steps in a background thread."""
    global _leader_id, _election_in_progress

    while True:
        if is_failed():
            with _lock:
                _election_in_progress = False
            return

        higher_peers = {nid: url for nid, url in PEERS.items() if nid > NODE_ID}
        higher_ids = sorted(higher_peers.keys())

        if higher_ids:
            logger.info("node-%d: sent ELECTION to %s", NODE_ID, higher_ids)
            send_gateway_event("election", f"Node {NODE_ID} sent ELECTION to {higher_ids}")
        else:
            logger.info("node-%d: no higher peers, becoming leader", NODE_ID)

        if not higher_peers:
            # Step 2: No higher peer exists, I win immediately
            with _lock:
                _leader_id = NODE_ID
                _election_in_progress = False
                _coordinator_event.set()
            send_gateway_event("election", f"Node {NODE_ID} won election and became leader")
            _broadcast_coordinator()
            return

        # Step 1: Send POST /election to all peers with ID > mine concurrently
        answered_ok = False
        with ThreadPoolExecutor(max_workers=len(higher_peers)) as pool:
            futures = {
                pool.submit(_send_election_message, nid, url): nid
                for nid, url in higher_peers.items()
            }
            for fut in as_completed(futures):
                try:
                    if fut.result():
                        answered_ok = True
                except Exception:
                    pass

        if is_failed():
            with _lock:
                _election_in_progress = False
            return

        if not answered_ok:
            # Step 2: If NO higher peer answered ok=true, I win
            logger.info("node-%d: no answer, becoming leader", NODE_ID)
            with _lock:
                _leader_id = NODE_ID
                _election_in_progress = False
                _coordinator_event.set()
            send_gateway_event("election", f"Node {NODE_ID} won election and became leader")
            _broadcast_coordinator()
            return
        else:
            # Step 3: Higher peer answered ok=true, wait up to ELECTION_TIMEOUT_SECONDS * 2 for coordinator
            logger.info("node-%d: higher peer answered ok, waiting for COORDINATOR", NODE_ID)
            received = _coordinator_event.wait(timeout=ELECTION_TIMEOUT_SECONDS * 2.0)
            if received or is_failed():
                with _lock:
                    _election_in_progress = False
                return
            # If none arrives within timeout, restart the election
            logger.warning("node-%d: coordinator wait timed out, restarting election", NODE_ID)
            # Loop continues to run next round of election


def start_election() -> None:
    """Trigger a Bully election if not already running and not failed."""
    # LAB TOPIC 4: Bully election
    global _election_in_progress
    if is_failed():
        return

    with _lock:
        if _election_in_progress:
            return
        _election_in_progress = True
        _coordinator_event.clear()

    t = threading.Thread(target=_run_election_thread, daemon=True, name=f"election-{NODE_ID}")
    t.start()


def handle_election(from_id: int) -> dict:
    """Handle POST /election {from_id}."""
    # LAB TOPIC 4: Bully election
    if from_id < NODE_ID:
        logger.info("node-%d: received ELECTION from lower node %d -> reply ok=true and start election", NODE_ID, from_id)
        # Start own election in background thread unless already running
        start_election()
        return {"ok": True}
    else:
        return {"ok": False}


def handle_coordinator(leader_id: int) -> dict:
    """Handle POST /coordinator {leader_id}."""
    # LAB TOPIC 4: Bully election
    global _leader_id, _last_heartbeat, _election_in_progress
    with _lock:
        was_leader = (_leader_id == NODE_ID)
        _leader_id = leader_id
        _last_heartbeat = time.monotonic()
        _coordinator_event.set()
        _election_in_progress = False

    if was_leader and leader_id != NODE_ID:
        logger.info("node-%d: stepped down as leader (new leader: %d)", NODE_ID, leader_id)

    logger.info("node-%d: received COORDINATOR from %d", NODE_ID, leader_id)
    send_gateway_event("election", f"received COORDINATOR from {leader_id}")
    return {"status": "ok", "leader_id": leader_id}


def handle_heartbeat(leader_id: int) -> dict:
    """Handle POST /heartbeat {leader_id}."""
    # LAB TOPIC 4: Bully election
    global _leader_id, _last_heartbeat
    with _lock:
        if _leader_id is not None and leader_id < _leader_id:
            # Ignore heartbeat from lower node if higher leader is known
            return {"status": "ignored", "leader_id": _leader_id}

        if _leader_id == NODE_ID and leader_id > NODE_ID:
            logger.info("node-%d: received HEARTBEAT from higher node %d, stepping down as leader", NODE_ID, leader_id)

        _leader_id = leader_id
        _last_heartbeat = time.monotonic()

    return {"status": "ok", "leader_id": leader_id}


# ---------------------------------------------------------------------------
# Background Daemon Threads: Heartbeat sender, Follower monitor, Startup delay
# ---------------------------------------------------------------------------

def _heartbeat_sender_loop() -> None:
    """Leader duties: every HEARTBEAT_INTERVAL_SECONDS send POST /heartbeat to all peers."""
    logger.info("node-%d: heartbeat sender thread started", NODE_ID)
    while True:
        time.sleep(HEARTBEAT_INTERVAL_SECONDS)
        if is_failed() or not is_leader():
            continue

        def _send_hb(peer_url: str) -> None:
            try:
                requests.post(
                    f"{peer_url.rstrip('/')}/heartbeat",
                    json={"leader_id": NODE_ID},
                    timeout=ELECTION_TIMEOUT_SECONDS,
                )
            except Exception:
                pass

        with ThreadPoolExecutor(max_workers=max(1, len(PEERS))) as pool:
            for url in PEERS.values():
                pool.submit(_send_hb, url)


def _follower_monitor_loop() -> None:
    """Follower duties: check every second for leader heartbeat timeout."""
    logger.info("node-%d: follower monitor thread started", NODE_ID)
    while True:
        time.sleep(1.0)
        if is_failed():
            continue

        with _lock:
            curr_leader = _leader_id
            elapsed = time.monotonic() - _last_heartbeat
            in_progress = _election_in_progress

        if curr_leader != NODE_ID:
            if elapsed > LEADER_TIMEOUT_SECONDS and not in_progress:
                logger.warning(
                    "node-%d: leader heartbeat timed out (%.1fs > %.1fs) -> starting election",
                    NODE_ID,
                    elapsed,
                    LEADER_TIMEOUT_SECONDS,
                )
                send_gateway_event(
                    "election",
                    f"Node {NODE_ID} detected leader timeout ({elapsed:.1f}s), initiating election",
                )
                start_election()


def _startup_election_delay() -> None:
    """Start an election shortly after startup (random delay 1-3s, skipped if leader known)."""
    delay = random.uniform(1.0, 3.0)
    time.sleep(delay)
    if is_failed():
        return
    with _lock:
        if _leader_id is not None or _election_in_progress:
            return
    logger.info("node-%d: initial startup delay elapsed (%.2fs) with no leader -> starting election", NODE_ID, delay)
    start_election()


def start_election_subsystem() -> None:
    """Start all election background threads at FastAPI startup."""
    t_hb = threading.Thread(target=_heartbeat_sender_loop, daemon=True, name=f"heartbeat-{NODE_ID}")
    t_hb.start()

    t_mon = threading.Thread(target=_follower_monitor_loop, daemon=True, name=f"follower-mon-{NODE_ID}")
    t_mon.start()

    t_start = threading.Thread(target=_startup_election_delay, daemon=True, name=f"startup-elect-{NODE_ID}")
    t_start.start()
