"""Storage node — FastAPI application.

LAB TOPIC 1: RPC via REST/HTTP
  All endpoints are the RPC interface that the gateway calls.

LAB TOPIC 2: Multithreading
  Plain `def` handlers run in FastAPI's default thread pool (concurrent
  request handling). The active_requests counter is protected by a Lock.
  The PUT handler is async only to read the body; the heavy work (SHA-256,
  disk write) is synchronous and runs in the thread pool via run_in_executor.

LAB TOPIC 4: Bully Election
  Nodes participate in leader election via /election, /coordinator, /heartbeat,
  and /leader endpoints.

LAB TOPIC 8: Failover, Failure Detection & Repair
  Simulated crash via /admin/fail and /admin/recover.
  Leader-driven failure detection and repair loop with /chunks/{id}/pull.
"""
import asyncio
import hashlib
import logging
import threading
from typing import Optional

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import requests

from app.config import NODE_ID
from app.election import (
    get_leader_id,
    handle_coordinator,
    handle_election,
    handle_heartbeat,
    start_election,
    start_election_subsystem,
)
from app.failure_detector import start_failure_detector
from app.lamport import LamportClock
from app.repair import start_repair_loop
from app.state import is_failed, set_failed
from app.storage import (
    delete_chunk,
    get_chunk,
    get_stats,
    init_storage,
    list_chunks,
    store_chunk,
    validate_chunk_id,
)

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [node-{NODE_ID}] %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title=f"Storage Node {NODE_ID}")

# LAB TOPIC 2: multithreading — in-flight request counter protected by a lock
_active_lock = threading.Lock()
_active_requests: int = 0

# LAB TOPIC 3: Lamport logical clocks — local clock instance
clock = LamportClock()


@app.on_event("startup")
def startup() -> None:
    init_storage()
    # LAB TOPIC 4: Bully election background monitor & heartbeat
    start_election_subsystem()
    # LAB TOPIC 8: Failure detector & repair loop
    start_failure_detector()
    start_repair_loop()


# ---------------------------------------------------------------------------
# Middleware: simulated failure check + active requests + Lamport clock
# LAB TOPIC 2: multithreading — shared counter under a lock
# LAB TOPIC 3: Lamport logical clocks — update clock on receive, set response header
# LAB TOPIC 8: simulated crash — return 503 for all endpoints except /admin/recover and /admin/status
# ---------------------------------------------------------------------------
@app.middleware("http")
async def process_request_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
    global _active_requests

    # LAB TOPIC 8: Simulated crash failure
    if is_failed() and request.url.path not in ("/admin/recover", "/admin/status"):
        return JSONResponse(
            status_code=503,
            content={"detail": f"Node {NODE_ID} simulated crash failure", "failed": True},
            headers={"X-Lamport-Time": str(clock.value())},
        )

    is_health = request.url.path == "/health"
    if not is_health:
        with _active_lock:
            _active_requests += 1

    # LAB TOPIC 3: update Lamport clock from incoming header
    lamport_header = request.headers.get("X-Lamport-Time")
    if lamport_header is not None:
        try:
            clock.update(int(lamport_header))
        except ValueError:
            clock.tick()
    else:
        clock.tick()

    try:
        response = await call_next(request)
    finally:
        if not is_health:
            with _active_lock:
                _active_requests -= 1

    # LAB TOPIC 3: put current Lamport clock value in response header
    response.headers["X-Lamport-Time"] = str(clock.value())
    return response


def _get_active() -> int:
    with _active_lock:
        return _active_requests


# ---------------------------------------------------------------------------
# Request Models
# ---------------------------------------------------------------------------

class ElectionPayload(BaseModel):
    from_id: int


class CoordinatorPayload(BaseModel):
    leader_id: int


class HeartbeatPayload(BaseModel):
    leader_id: int


class PullPayload(BaseModel):
    source_url: str
    sha256: str


# ---------------------------------------------------------------------------
# Admin Endpoints (LAB TOPIC 8: Simulated Failure)
# ---------------------------------------------------------------------------

@app.post("/admin/fail")
def admin_fail() -> dict:
    """LAB TOPIC 8: simulated crash -> node returns 503 to all calls except recover/status."""
    set_failed(True)
    logger.warning("Node %d simulated crash: failed=True", NODE_ID)
    return {"node_id": NODE_ID, "failed": True, "leader_id": get_leader_id()}


@app.post("/admin/recover")
def admin_recover() -> dict:
    """LAB TOPIC 8: recover node -> sets failed=False and triggers election."""
    set_failed(False)
    logger.info("Node %d recovered: failed=False -> triggering election", NODE_ID)
    start_election()
    return {"node_id": NODE_ID, "failed": False, "leader_id": get_leader_id()}


@app.get("/admin/status")
def admin_status() -> dict:
    """LAB TOPIC 8: return node failure and leader status."""
    return {
        "node_id": NODE_ID,
        "failed": is_failed(),
        "leader_id": get_leader_id(),
    }


# ---------------------------------------------------------------------------
# Election Endpoints (LAB TOPIC 4: Bully Election)
# ---------------------------------------------------------------------------

@app.post("/election")
def election_endpoint(payload: ElectionPayload) -> dict:
    """LAB TOPIC 4: handle incoming election probe."""
    return handle_election(payload.from_id)


@app.post("/coordinator")
def coordinator_endpoint(payload: CoordinatorPayload) -> dict:
    """LAB TOPIC 4: handle coordinator announcement from winner."""
    return handle_coordinator(payload.leader_id)


@app.post("/heartbeat")
def heartbeat_endpoint(payload: HeartbeatPayload) -> dict:
    """LAB TOPIC 4: handle heartbeat from leader."""
    return handle_heartbeat(payload.leader_id)


@app.get("/leader")
def leader_endpoint() -> dict:
    """LAB TOPIC 4: return current leader id."""
    return {"leader_id": get_leader_id()}


# ---------------------------------------------------------------------------
# Storage & Health Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health() -> dict:
    """LAB TOPIC 1: RPC — health probe called by gateway health monitor.
    
    LAB TOPIC 3: Lamport logical clocks — returns current node lamport timestamp.
    LAB TOPIC 4: Bully election — returns current known leader_id.
    """
    chunk_count, bytes_used = get_stats()
    return {
        "node_id": NODE_ID,
        "status": "ok",
        "chunk_count": chunk_count,
        "bytes_used": bytes_used,
        "active_requests": _get_active(),
        "lamport": clock.value(),
        "leader_id": get_leader_id(),
    }


@app.put("/chunks/{chunk_id}")
async def put_chunk(chunk_id: str, request: Request) -> JSONResponse:
    """LAB TOPIC 1: RPC — upload a chunk from the gateway.

    LAB TOPIC 5: Replication — primary synchronously forwards to backup.
    LAB TOPIC 8: Fault tolerance — degraded=true if backup times out or fails.
    """
    if not validate_chunk_id(chunk_id):
        raise HTTPException(status_code=400, detail="Invalid chunk_id format")

    expected_sha = request.headers.get("X-Chunk-Sha256", "")
    if not expected_sha:
        raise HTTPException(status_code=400, detail="Missing X-Chunk-Sha256 header")

    role = request.headers.get("X-Role", "primary")
    backup_url = request.headers.get("X-Backup-Url")
    backup_node_id_header = request.headers.get("X-Backup-Node-Id")

    data: bytes = await request.body()
    if not data:
        raise HTTPException(status_code=400, detail="Empty chunk body")

    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            None, store_chunk, chunk_id, data, expected_sha
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    replicated_to: list[int] = []
    degraded: bool = False

    # LAB TOPIC 5: replication & LAB TOPIC 8: primary-backup fault tolerance
    if role == "primary" and backup_url:
        backup_node_id: Optional[int] = None
        if backup_node_id_header is not None:
            try:
                backup_node_id = int(backup_node_id_header)
            except ValueError:
                pass

        send_lamport = clock.tick()
        backup_put_url = f"{backup_url.rstrip('/')}/chunks/{chunk_id}"
        try:
            resp = requests.put(
                backup_put_url,
                data=data,
                headers={
                    "Content-Type": "application/octet-stream",
                    "X-Chunk-Sha256": expected_sha,
                    "X-Role": "backup",
                    "X-Lamport-Time": str(send_lamport),
                },
                timeout=(2, 10),
            )
            if resp.status_code == 200:
                if backup_node_id is not None:
                    replicated_to.append(backup_node_id)
                if "X-Lamport-Time" in resp.headers:
                    try:
                        clock.update(int(resp.headers["X-Lamport-Time"]))
                    except ValueError:
                        pass
            else:
                logger.warning(
                    "Backup node at %s returned status %d for chunk %s",
                    backup_put_url,
                    resp.status_code,
                    chunk_id,
                )
                degraded = True
        except Exception as exc:
            logger.warning(
                "Backup forward to %s failed for chunk %s: %s",
                backup_put_url,
                chunk_id,
                exc,
            )
            degraded = True

    result["replicated_to"] = replicated_to
    result["degraded"] = degraded

    return JSONResponse(content=result, status_code=200)


@app.post("/chunks/{chunk_id}/pull")
def pull_chunk_endpoint(chunk_id: str, payload: PullPayload) -> dict:
    """LAB TOPIC 8: repair pull instruction from leader.
    
    GETs the chunk from source_url/chunks/{chunk_id}, verifies SHA-256, stores atomically.
    """
    if not validate_chunk_id(chunk_id):
        raise HTTPException(status_code=400, detail="Invalid chunk_id format")

    try:
        url = f"{payload.source_url.rstrip('/')}/chunks/{chunk_id}"
        resp = requests.get(url, timeout=(2, 10))
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Source node returned status {resp.status_code}")
        data = resp.content
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch chunk from {payload.source_url}: {exc}")

    actual_sha = hashlib.sha256(data).hexdigest()
    if actual_sha != payload.sha256:
        raise HTTPException(status_code=422, detail=f"SHA-256 mismatch: expected {payload.sha256}, got {actual_sha}")

    try:
        res = store_chunk(chunk_id, data, payload.sha256)
        return {
            "status": "ok",
            "chunk_id": chunk_id,
            "size": res["size"],
            "sha256": actual_sha,
            "node_id": NODE_ID,
        }
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@app.get("/chunks/{chunk_id}")
def get_chunk_endpoint(chunk_id: str) -> Response:
    """LAB TOPIC 1: RPC — download a single chunk."""
    if not validate_chunk_id(chunk_id):
        raise HTTPException(status_code=400, detail="Invalid chunk_id format")

    data = get_chunk(chunk_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Chunk not found")

    return Response(content=data, media_type="application/octet-stream")


@app.delete("/chunks/{chunk_id}")
def delete_chunk_endpoint(chunk_id: str) -> dict:
    """LAB TOPIC 1: RPC — delete a chunk (idempotent)."""
    if not validate_chunk_id(chunk_id):
        raise HTTPException(status_code=400, detail="Invalid chunk_id format")

    delete_chunk(chunk_id)
    return {"deleted": chunk_id}


@app.get("/chunks")
def list_chunks_endpoint() -> list:
    """LAB TOPIC 1: RPC — inventory all chunks on this node."""
    return list_chunks()
