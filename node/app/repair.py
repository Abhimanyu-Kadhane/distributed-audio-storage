"""Leader-run repair loop.

LAB TOPIC 8: Re-replication & Repair
Runs every REPAIR_INTERVAL_SECONDS (default 5s) in a daemon thread.
Active only while this node is leader and not failed.
Identifies chunks with fewer alive replicas than REPLICATION_FACTOR.
Instructs candidate target nodes to pull from alive sources, then notifies
gateway to register the new backup replica.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import logging
import threading
import time
from typing import List, Set, Tuple

import requests

from app.config import (
    ALL_NODES,
    GATEWAY_URL,
    NODE_ID,
    REPAIR_BATCH,
    REPAIR_INTERVAL_SECONDS,
    REPLICATION_FACTOR,
)
from app.election import is_leader, send_gateway_event
from app.failure_detector import get_node_health_info, is_node_alive
from app.state import is_failed
from app.storage import store_chunk

logger = logging.getLogger(__name__)

_lost_lock = threading.Lock()
_lost_chunks: Set[str] = set()


def _do_single_repair(task: Tuple[str, str, int, str, int, str]) -> bool:
    """Execute repair for one chunk: instruct target to pull, then record replica with gateway."""
    chunk_id, sha256, source_id, source_url, target_id, target_url = task

    if is_failed() or not is_leader():
        return False

    try:
        if target_id == NODE_ID:
            # Local pull directly to avoid unnecessary self-HTTP overhead
            get_resp = requests.get(f"{source_url.rstrip('/')}/chunks/{chunk_id}", timeout=(2, 10))
            if get_resp.status_code != 200:
                logger.warning("Local pull failed to fetch chunk %s from %s: %d", chunk_id, source_url, get_resp.status_code)
                return False
            data = get_resp.content
            if hashlib.sha256(data).hexdigest() != sha256:
                logger.warning("Local pull SHA mismatch for chunk %s", chunk_id)
                return False
            store_chunk(chunk_id, data, sha256)
        else:
            # Instruct remote target node to pull chunk from source
            pull_resp = requests.post(
                f"{target_url.rstrip('/')}/chunks/{chunk_id}/pull",
                json={"source_url": source_url, "sha256": sha256},
                timeout=(3, 15),
            )
            if pull_resp.status_code != 200:
                logger.warning(
                    "Target node %d returned status %d on pull for chunk %s",
                    target_id,
                    pull_resp.status_code,
                    chunk_id,
                )
                return False

        if is_failed() or not is_leader():
            return False

        # LAB TOPIC 8: Register new replica with the gateway
        add_resp = requests.post(
            f"{GATEWAY_URL}/internal/replica-added",
            json={"chunk_id": chunk_id, "node_id": target_id, "role": "backup"},
            timeout=5.0,
        )
        if add_resp.status_code == 200:
            send_gateway_event(
                "repair",
                f"Repaired chunk {chunk_id}: replicated from node {source_id} to node {target_id}",
            )
            return True
        else:
            logger.warning(
                "Gateway rejected replica-added for chunk %s on node %d: status %d",
                chunk_id,
                target_id,
                add_resp.status_code,
            )
    except Exception as exc:
        logger.warning("Repair error for chunk %s -> node %d: %s", chunk_id, target_id, exc)

    return False


def _repair_cycle() -> None:
    """Execute one pass of the leader repair loop."""
    try:
        report_resp = requests.get(f"{GATEWAY_URL}/internal/replication-report", timeout=5.0)
        if report_resp.status_code != 200:
            return
        report: List[dict] = report_resp.json()
    except Exception as exc:
        logger.debug("Leader could not fetch replication report: %s", exc)
        return

    # Find under-replicated chunks
    under_replicated: List[Tuple[dict, list]] = []
    for chunk in report:
        chunk_id = chunk["chunk_id"]
        replicas = chunk.get("replicas", [])
        alive_replicas = [r for r in replicas if is_node_alive(r["node_id"])]

        if len(alive_replicas) >= REPLICATION_FACTOR:
            continue

        if len(alive_replicas) == 0:
            with _lost_lock:
                if chunk_id not in _lost_chunks:
                    _lost_chunks.add(chunk_id)
                    logger.warning("chunk lost: %s has 0 alive replicas", chunk_id)
            continue

        under_replicated.append((chunk, alive_replicas))

    if not under_replicated:
        return

    # Process up to REPAIR_BATCH chunks per cycle
    batch = under_replicated[:REPAIR_BATCH]
    repairs_assigned: dict[int, int] = {}
    tasks: List[Tuple[str, str, int, str, int, str]] = []

    for chunk, alive_replicas in batch:
        chunk_id = chunk["chunk_id"]
        sha256 = chunk["sha256"]

        # 3. Source: any alive replica holder (prefer lowest active_requests)
        source = min(
            alive_replicas,
            key=lambda r: get_node_health_info(r["node_id"]).get("active_requests", 0),
        )
        source_id = source["node_id"]
        source_url = ALL_NODES[source_id]

        # Target: alive node that does not already hold the chunk, with fewest chunks
        existing_holder_ids = {r["node_id"] for r in chunk.get("replicas", [])}
        candidates = [
            nid for nid in ALL_NODES
            if is_node_alive(nid) and nid not in existing_holder_ids
        ]

        if not candidates:
            continue

        target_id = min(
            candidates,
            key=lambda nid: get_node_health_info(nid).get("chunk_count", 0) + repairs_assigned.get(nid, 0),
        )
        repairs_assigned[target_id] = repairs_assigned.get(target_id, 0) + 1
        target_url = ALL_NODES[target_id]

        tasks.append((chunk_id, sha256, source_id, source_url, target_id, target_url))

    if not tasks:
        return

    repaired_count = 0
    # Process batch using ThreadPoolExecutor(4)
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(_do_single_repair, task) for task in tasks]
        for fut in as_completed(futures):
            if fut.result():
                repaired_count += 1

    remaining = len(under_replicated) - repaired_count
    logger.info("repaired %d chunks, %d remaining", repaired_count, remaining)


def _repair_loop() -> None:
    """Daemon thread loop running every REPAIR_INTERVAL_SECONDS."""
    logger.info("node-%d: repair loop started (interval=%.1fs)", NODE_ID, REPAIR_INTERVAL_SECONDS)
    while True:
        time.sleep(REPAIR_INTERVAL_SECONDS)
        if is_failed() or not is_leader():
            continue
        _repair_cycle()


def start_repair_loop() -> None:
    """Start repair loop daemon thread at FastAPI startup."""
    t = threading.Thread(target=_repair_loop, daemon=True, name=f"repair-{NODE_ID}")
    t.start()
