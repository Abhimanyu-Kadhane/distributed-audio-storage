"""Gateway — FastAPI application.

This is the public-facing service that clients (and the Streamlit frontend) talk to.

LAB TOPIC 1: RPC via REST/HTTP — all routes here are the RPC interface for clients.
LAB TOPIC 2: multithreading — ThreadPoolExecutor for parallel chunk upload/download.
LAB TOPIC 3: Lamport logical clocks — timestamping, ordering, and conflict resolution.
LAB TOPIC 5: Replication — primary-backup synchronous write path.
LAB TOPIC 6: Load balancing — least_loaded / round_robin chunk placement and read selection.
LAB TOPIC 8: Primary-backup fault tolerance — degraded state handling and replica fallback.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import logging
import time
from typing import List, Optional
import uuid

from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel
import requests

from app.balancer import (
    acquire_download,
    choose_nodes,
    chunk_upload_failed,
    chunk_upload_succeeded,
    get_active_algorithm,
    order_read_replicas,
    release_download,
    release_reservation,
)
from app.chunker import merge, sha256_of, split
from app.config import (
    ALLOWED_EXTENSIONS,
    CHUNK_SIZE_BYTES,
    MAX_UPLOAD_BYTES,
    MAX_WORKERS,
    NODES,
    REPLICATION_FACTOR,
)
from app.db import (
    count_under_replicated_chunks,
    get_all_replicas_for_file,
    get_chunks_for_file,
    get_file,
    get_latest_version_for_name,
    get_replica_counts_per_node,
    get_replicas_for_chunk,
    get_replication_report,
    init_db,
    insert_file_with_chunks,
    list_files_with_latest,
    mark_file_deleted,
    upsert_replica,
)
from app.events import get_events, record_event
from app.health_monitor import (
    get_all_status,
    mark_alive_immediate,
    mark_dead,
    start_monitor,
)
from app.models import (
    ChunkInfo,
    ClusterStatus,
    EventRecord,
    FileListItem,
    FileRecord,
    NodeStatus,
    ReplicaNodeInfo,
)
from app.node_client import (
    NodeError,
    delete_chunk,
    gateway_clock,
    get_chunk,
    put_chunk,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [gateway] %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Distributed Audio Storage — Gateway")


@app.on_event("startup")
def startup() -> None:
    init_db()
    start_monitor()
    logger.info("Gateway started. Nodes: %s", list(NODES.keys()))


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Upload
# LAB TOPIC 2: parallel chunk upload via ThreadPoolExecutor
# LAB TOPIC 5: primary-backup replication
# LAB TOPIC 6: load balancing (least_loaded / round_robin)
# ---------------------------------------------------------------------------

def _upload_one_chunk(
    idx: int,
    chunk_id: str,
    data: bytes,
    sha: str,
) -> dict:
    """Upload a single chunk via primary-backup replication.

    The gateway picks a primary and optional backup node.
    The primary stores the chunk and synchronously forwards to backup.
    If primary fails, retry on a different candidate pair.
    """
    # LAB TOPIC 6: Load balancing — choose_nodes allocates candidate nodes
    try:
        candidate_nodes = choose_nodes(REPLICATION_FACTOR)
    except RuntimeError as exc:
        raise RuntimeError(f"No alive nodes for chunk {chunk_id}: {exc}") from exc

    primary_id = candidate_nodes[0]
    backup_id = candidate_nodes[1] if len(candidate_nodes) > 1 else None
    backup_url = NODES[backup_id] if backup_id is not None else None

    if backup_id is None and REPLICATION_FACTOR > 1:
        record_event(
            actor="gateway",
            event_type="replication degraded",
            detail=f"Chunk {chunk_id}: only 1 alive node ({primary_id}) for RF={REPLICATION_FACTOR}",
            lamport=gateway_clock.value(),
        )

    logger.info(
        "Placing chunk %s (idx=%d) primary=%d backup=%s",
        chunk_id,
        idx,
        primary_id,
        backup_id,
    )

    upload_done = False
    try:
        try:
            # LAB TOPIC 5: synchronous primary-backup forwarding
            resp = put_chunk(
                node_id=primary_id,
                chunk_id=chunk_id,
                data=data,
                sha256=sha,
                role="primary",
                backup_url=backup_url,
                backup_node_id=backup_id,
            )
            replicated_to = resp.get("replicated_to", [])
            degraded = resp.get("degraded", False)

            stored_nodes = [primary_id] + replicated_to
            unack_nodes = [n for n in candidate_nodes if n not in stored_nodes]
            chunk_upload_succeeded(stored_nodes, unack_node_ids=unack_nodes)
            upload_done = True

            if degraded:
                record_event(
                    actor=f"node-{primary_id}",
                    event_type="replication degraded",
                    detail=f"Chunk {chunk_id}: primary {primary_id} failed to replicate to backup {backup_id}",
                    lamport=gateway_clock.value(),
                )

            record_event(
                actor="gateway",
                event_type="chunk placed",
                detail=f"Chunk {chunk_id} placed on primary node-{primary_id}, replicated_to={replicated_to}",
                lamport=gateway_clock.value(),
            )

            return {
                "idx": idx,
                "chunk_id": chunk_id,
                "size_bytes": len(data),
                "sha256": sha,
                "primary_id": primary_id,
                "replicated_to": replicated_to,
                "degraded": degraded,
            }
        except NodeError as first_err:
            logger.warning(
                "Chunk %s failed on primary node %d: %s — retrying on alternative candidate pair",
                chunk_id,
                primary_id,
                first_err,
            )
            mark_dead(primary_id)
            chunk_upload_failed(candidate_nodes)

            # Retry on a different candidate pair excluding failed primary
            try:
                retry_candidates = choose_nodes(REPLICATION_FACTOR, exclude={primary_id})
            except RuntimeError:
                raise RuntimeError(
                    f"No alive nodes for chunk {chunk_id} after primary failure"
                ) from first_err

            candidate_nodes = retry_candidates
            primary_id = candidate_nodes[0]
            backup_id = candidate_nodes[1] if len(candidate_nodes) > 1 else None
            backup_url = NODES[backup_id] if backup_id is not None else None

            logger.info("Retry chunk %s primary=%d backup=%s", chunk_id, primary_id, backup_id)
            try:
                resp = put_chunk(
                    node_id=primary_id,
                    chunk_id=chunk_id,
                    data=data,
                    sha256=sha,
                    role="primary",
                    backup_url=backup_url,
                    backup_node_id=backup_id,
                )
                replicated_to = resp.get("replicated_to", [])
                degraded = resp.get("degraded", False)

                stored_nodes = [primary_id] + replicated_to
                unack_nodes = [n for n in candidate_nodes if n not in stored_nodes]
                chunk_upload_succeeded(stored_nodes, unack_node_ids=unack_nodes)
                upload_done = True

                if degraded:
                    record_event(
                        actor=f"node-{primary_id}",
                        event_type="replication degraded",
                        detail=f"Chunk {chunk_id}: retry primary {primary_id} failed to replicate to backup {backup_id}",
                        lamport=gateway_clock.value(),
                    )
                record_event(
                    actor="gateway",
                    event_type="chunk placed",
                    detail=f"Chunk {chunk_id} placed on retry primary node-{primary_id}, replicated_to={replicated_to}",
                    lamport=gateway_clock.value(),
                )
                return {
                    "idx": idx,
                    "chunk_id": chunk_id,
                    "size_bytes": len(data),
                    "sha256": sha,
                    "primary_id": primary_id,
                    "replicated_to": replicated_to,
                    "degraded": degraded,
                }
            except NodeError as exc:
                mark_dead(primary_id)
                raise RuntimeError(
                    f"Chunk {chunk_id} failed on retry primary node {primary_id}"
                ) from exc
    finally:
        if not upload_done:
            chunk_upload_failed(candidate_nodes)


@app.post("/files", status_code=201)
async def upload_file(
    file: UploadFile = File(...),
    client_id: Optional[str] = Form(default="default"),
    lamport: Optional[int] = Form(default=None),
) -> FileRecord:
    """Upload an audio file: validate → chunk → parallel PUT → commit to DB."""
    # 1. Validate extension
    filename = file.filename or ""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '.{ext}'. Allowed: {list(ALLOWED_EXTENSIONS)}",
        )

    # 2. Read and validate size
    raw: bytes = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(raw) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File too large ({len(raw)} bytes). Max: {MAX_UPLOAD_BYTES}",
        )

    content_type = ALLOWED_EXTENSIONS[ext]
    file_id = uuid.uuid4().hex
    whole_sha = sha256_of(raw)
    chunks_data = split(raw, CHUNK_SIZE_BYTES)
    effective_client_id = client_id or "default"

    # LAB TOPIC 3: Lamport logical clocks — client write timestamps
    if lamport is not None:
        file_lamport_ts = lamport
        gateway_lamport = gateway_clock.update(lamport)
    else:
        file_lamport_ts = gateway_clock.tick()
        gateway_lamport = gateway_clock.value()

    record_event(
        actor=effective_client_id,
        event_type="upload start",
        detail=f"Upload start for {filename} ({len(raw)} bytes, {len(chunks_data)} chunks)",
        lamport=gateway_lamport,
    )

    logger.info(
        "Upload start: file=%s id=%s size=%d chunks=%d client=%s lamport=%d",
        filename,
        file_id,
        len(raw),
        len(chunks_data),
        effective_client_id,
        file_lamport_ts,
    )
    t0 = time.monotonic()

    # 3. LAB TOPIC 2: parallel chunk upload
    upload_tasks = [
        (idx, f"{file_id}_{idx:05d}", data, sha256_of(data))
        for idx, data in chunks_data
    ]

    successful: List[dict] = []
    failed = False

    # LAB TOPIC 2: ThreadPoolExecutor — submit all chunk uploads simultaneously
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_map = {
            pool.submit(_upload_one_chunk, idx, chunk_id, data, sha): chunk_id
            for idx, chunk_id, data, sha in upload_tasks
        }
        for future in as_completed(future_map):
            chunk_id = future_map[future]
            try:
                result = future.result()
                successful.append(result)
            except RuntimeError as exc:
                logger.error("Chunk upload failed: %s", exc)
                failed = True
                break

    if failed:
        # Best-effort cleanup of already-stored chunks on primary and backup nodes
        _cleanup_stored_chunks(successful)
        raise HTTPException(
            status_code=503,
            detail="Could not store all chunks — not enough available nodes",
        )

    # 4. Commit metadata in one transaction
    now = datetime.now(timezone.utc).isoformat()
    chunks_meta = [
        {
            "chunk_id": r["chunk_id"],
            "idx": r["idx"],
            "size_bytes": r["size_bytes"],
            "sha256": r["sha256"],
        }
        for r in successful
    ]

    # Replicas rows: primary + acked backups
    replicas_meta: List[dict] = []
    for r in successful:
        replicas_meta.append(
            {
                "chunk_id": r["chunk_id"],
                "node_id": r["primary_id"],
                "role": "primary",
                "state": "ok",
            }
        )
        for b_id in r["replicated_to"]:
            replicas_meta.append(
                {
                    "chunk_id": r["chunk_id"],
                    "node_id": b_id,
                    "role": "backup",
                    "state": "ok",
                }
            )

    # Check previous latest version for version superseded event
    prev_latest = get_latest_version_for_name(filename)

    insert_file_with_chunks(
        file_id=file_id,
        name=filename,
        content_type=content_type,
        size_bytes=len(raw),
        sha256=whole_sha,
        chunk_size=CHUNK_SIZE_BYTES,
        chunk_count=len(chunks_data),
        created_at=now,
        client_id=effective_client_id,
        chunks=chunks_meta,
        replicas=replicas_meta,
        lamport_ts=file_lamport_ts,
    )

    new_latest = get_latest_version_for_name(filename)
    is_latest = bool(new_latest and new_latest["file_id"] == file_id)

    # Record version superseded if a previously existing file is replaced as latest
    if (
        prev_latest is not None
        and is_latest
        and prev_latest["file_id"] != file_id
    ):
        record_event(
            actor="gateway",
            event_type="version superseded",
            detail=f"File {filename}: version {prev_latest['file_id']} superseded by {file_id}",
            lamport=gateway_clock.value(),
        )

    record_event(
        actor="gateway",
        event_type="upload commit",
        detail=f"Upload commit for {filename} ({file_id})",
        lamport=gateway_clock.value(),
    )

    elapsed_ms = int((time.monotonic() - t0) * 1000)
    logger.info(
        "Upload done: file=%s chunks=%d elapsed_ms=%d lamport_ts=%d",
        filename,
        len(chunks_data),
        elapsed_ms,
        file_lamport_ts,
    )

    # Build chunk map for the response
    chunk_map = []
    for r in sorted(successful, key=lambda x: x["idx"]):
        nodes_list = [ReplicaNodeInfo(node_id=r["primary_id"], role="primary")]
        for b_id in r["replicated_to"]:
            nodes_list.append(ReplicaNodeInfo(node_id=b_id, role="backup"))
        chunk_map.append(
            ChunkInfo(
                idx=r["idx"],
                chunk_id=r["chunk_id"],
                size=r["size_bytes"],
                nodes=nodes_list,
            )
        )

    # Check if placement was degraded (< REPLICATION_FACTOR copies)
    is_degraded = any(
        r.get("degraded", False)
        or (len(r.get("replicated_to", [])) + 1 < REPLICATION_FACTOR)
        for r in successful
    )

    return FileRecord(
        file_id=file_id,
        name=filename,
        content_type=content_type,
        size_bytes=len(raw),
        sha256=whole_sha,
        chunk_size=CHUNK_SIZE_BYTES,
        chunk_count=len(chunks_data),
        created_at=now,
        client_id=effective_client_id,
        status="complete",
        lamport_ts=file_lamport_ts,
        gateway_lamport=gateway_clock.value(),
        is_latest=is_latest,
        chunk_map=chunk_map,
        degraded=is_degraded,
    )


def _cleanup_stored_chunks(stored: List[dict]) -> None:
    """Best-effort deletion of chunks already written to primary/backup nodes."""
    for item in stored:
        nodes_to_clean = [item["primary_id"]] + item.get("replicated_to", [])
        for nid in nodes_to_clean:
            try:
                delete_chunk(nid, item["chunk_id"])
            except NodeError as exc:
                logger.warning(
                    "Cleanup failed for chunk %s on node %d: %s",
                    item["chunk_id"],
                    nid,
                    exc,
                )


# ---------------------------------------------------------------------------
# List files
# LAB TOPIC 3: Versioning & conflict resolution — all_versions parameter
# ---------------------------------------------------------------------------

@app.get("/files")
def list_files_endpoint(
    all_versions: bool = Query(default=True),
) -> List[FileListItem]:
    """Return non-deleted files, optionally filtered to only latest version per name."""
    files_data = list_files_with_latest(all_versions=all_versions)
    return [
        FileListItem(
            file_id=row["file_id"],
            name=row["name"],
            content_type=row["content_type"],
            size_bytes=row["size_bytes"],
            chunk_count=row["chunk_count"],
            created_at=row["created_at"],
            client_id=row["client_id"],
            lamport_ts=row["lamport_ts"],
            is_latest=row["is_latest"],
        )
        for row in files_data
    ]


# ---------------------------------------------------------------------------
# File detail
# ---------------------------------------------------------------------------

@app.get("/files/{file_id}")
def get_file_detail(file_id: str) -> FileRecord:
    """Return full file record with chunk map and role per node."""
    row = get_file(file_id)
    if row is None:
        raise HTTPException(status_code=404, detail="File not found")

    chunk_rows = get_chunks_for_file(file_id)
    chunk_map = []
    for c in chunk_rows:
        replica_rows = get_replicas_for_chunk(c["chunk_id"])
        nodes = [
            ReplicaNodeInfo(node_id=r["node_id"], role=r["role"])
            for r in replica_rows
        ]
        chunk_map.append(
            ChunkInfo(
                idx=c["idx"],
                chunk_id=c["chunk_id"],
                size=c["size_bytes"],
                nodes=nodes,
            )
        )

    latest = get_latest_version_for_name(row["name"])
    is_latest = bool(latest and latest["file_id"] == file_id)

    return FileRecord(
        file_id=row["file_id"],
        name=row["name"],
        content_type=row["content_type"],
        size_bytes=row["size_bytes"],
        sha256=row["sha256"],
        chunk_size=row["chunk_size"],
        chunk_count=row["chunk_count"],
        created_at=row["created_at"],
        client_id=row["client_id"],
        status=row["status"],
        lamport_ts=row["lamport_ts"],
        is_latest=is_latest,
        chunk_map=chunk_map,
    )


# ---------------------------------------------------------------------------
# Download
# LAB TOPIC 2: parallel chunk download via ThreadPoolExecutor
# LAB TOPIC 6: read selection by lowest active requests + in-flight
# LAB TOPIC 8: fallback across replicas on failure / checksum error
# ---------------------------------------------------------------------------

def _download_one_chunk(
    chunk_id: str,
    candidate_node_ids: List[int],
    expected_sha: str,
) -> bytes:
    """Download a single chunk; tries alive replicas ordered by read selection.

    Maintains gateway-local in-flight counter around requests.
    Falls back to next replica on NodeError or checksum mismatch.
    """
    ordered_nodes = order_read_replicas(candidate_node_ids)
    if not ordered_nodes:
        raise RuntimeError(
            f"No alive replicas available for chunk {chunk_id} (candidates={candidate_node_ids})"
        )

    last_err: Optional[Exception] = None
    for node_id in ordered_nodes:
        acquire_download(node_id)
        try:
            data = get_chunk(node_id, chunk_id)
            actual_sha = hashlib.sha256(data).hexdigest()
            if actual_sha != expected_sha:
                raise ValueError(
                    f"SHA-256 mismatch on chunk {chunk_id} from node {node_id}: "
                    f"expected {expected_sha}, got {actual_sha}"
                )
            return data
        except NodeError as exc:
            logger.warning(
                "get_chunk failed node=%d chunk=%s: %s — trying next replica",
                node_id,
                chunk_id,
                exc,
            )
            mark_dead(node_id)
            last_err = exc
        except ValueError as exc:
            logger.warning(
                "Integrity error for chunk %s on node %d: %s",
                chunk_id,
                node_id,
                exc,
            )
            last_err = exc
        finally:
            release_download(node_id)

    raise RuntimeError(
        f"Could not retrieve chunk {chunk_id} from any replica: {last_err}"
    )


@app.get("/files/{file_id}/download")
def download_file(file_id: str) -> Response:
    """Reassemble a file from its chunks and return it."""
    row = get_file(file_id)
    if row is None:
        raise HTTPException(status_code=404, detail="File not found")

    chunk_rows = get_chunks_for_file(file_id)

    # Build per-chunk download tasks: (idx, chunk_id, [node_ids], sha256)
    tasks = []
    for c in chunk_rows:
        replica_rows = get_replicas_for_chunk(c["chunk_id"])
        node_ids = [r["node_id"] for r in replica_rows]
        if not node_ids:
            raise HTTPException(
                status_code=502,
                detail=f"No replicas recorded for chunk {c['chunk_id']}",
            )
        tasks.append((c["idx"], c["chunk_id"], node_ids, c["sha256"]))

    results: dict[int, bytes] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_to_idx = {
            pool.submit(_download_one_chunk, chunk_id, node_ids, sha): idx
            for idx, chunk_id, node_ids, sha in tasks
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                results[idx] = future.result()
            except RuntimeError as exc:
                raise HTTPException(status_code=502, detail=str(exc))

    ordered = [results[i] for i in range(len(tasks))]
    file_bytes = merge(ordered)

    actual_sha = sha256_of(file_bytes)
    if actual_sha != row["sha256"]:
        raise HTTPException(
            status_code=502,
            detail="Whole-file SHA-256 mismatch — data may be corrupted",
        )

    record_event(
        actor="gateway",
        event_type="download served",
        detail=f"Download served for file {row['name']} ({file_id})",
        lamport=gateway_clock.value(),
    )

    ext = row["name"].rsplit(".", 1)[-1].lower() if "." in row["name"] else ""
    content_type = ALLOWED_EXTENSIONS.get(ext, row["content_type"])

    return Response(
        content=file_bytes,
        media_type=content_type,
        headers={
            "Content-Disposition": f'attachment; filename="{row["name"]}"',
        },
    )


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------

@app.delete("/files/{file_id}")
def delete_file(file_id: str) -> dict:
    """Best-effort delete of all chunk replicas, then soft-delete the record."""
    row = get_file(file_id)
    if row is None:
        raise HTTPException(status_code=404, detail="File not found")

    replica_rows = get_all_replicas_for_file(file_id)
    for r in replica_rows:
        try:
            delete_chunk(r["node_id"], r["chunk_id"])
        except NodeError as exc:
            logger.warning(
                "Best-effort delete failed node=%d chunk=%s: %s",
                r["node_id"],
                r["chunk_id"],
                exc,
            )

    mark_file_deleted(file_id)
    record_event(
        actor="gateway",
        event_type="file deleted",
        detail=f"File {row['name']} ({file_id}) deleted",
        lamport=gateway_clock.value(),
    )
    logger.info("Deleted file %s", file_id)
    return {"deleted": file_id}


# ---------------------------------------------------------------------------
# Cluster status
# ---------------------------------------------------------------------------

@app.get("/cluster/status")
def cluster_status() -> ClusterStatus:
    """Return live node health information, leader status, and replication metrics."""
    status_map = get_all_status()
    alive_node_ids = {nid for nid, s in status_map.items() if s["alive"]}
    replica_counts = get_replica_counts_per_node()
    under_replicated = count_under_replicated_chunks(
        alive_node_ids=alive_node_ids,
        replication_factor=REPLICATION_FACTOR,
    )

    # LAB TOPIC 4: Derive leader_id from node health payloads:
    # take the id reported as leader by the alive node whose own id equals that leader_id; null if none
    leader_id: Optional[int] = None
    for nid in sorted(alive_node_ids):
        s = status_map.get(nid, {})
        if s.get("leader_id") == nid:
            leader_id = nid
            break

    nodes = [
        NodeStatus(
            node_id=nid,
            url=NODES[nid],
            alive=s["alive"],
            chunk_count=s["chunk_count"],
            bytes_used=s["bytes_used"],
            active_requests=s["active_requests"],
            primary_chunks=replica_counts.get(nid, {}).get("primary", 0),
            backup_chunks=replica_counts.get(nid, {}).get("backup", 0),
            is_leader=(leader_id is not None and nid == leader_id),
        )
        for nid, s in sorted(status_map.items())
    ]
    return ClusterStatus(
        nodes=nodes,
        replication_factor=REPLICATION_FACTOR,
        chunk_size_bytes=CHUNK_SIZE_BYTES,
        lb_algorithm=get_active_algorithm(),
        under_replicated_chunks=under_replicated,
        leader_id=leader_id,
    )


# ---------------------------------------------------------------------------
# Event log
# ---------------------------------------------------------------------------

@app.get("/events")
def get_events_endpoint(limit: int = Query(default=50, ge=1, le=200)) -> List[EventRecord]:
    """Return recent events newest first."""
    raw_events = get_events(limit=limit)
    return [
        EventRecord(
            seq=e["seq"],
            wall_time=e["wall_time"],
            lamport=e["lamport"],
            actor=e["actor"],
            type=e["type"],
            detail=e["detail"],
        )
        for e in raw_events
    ]


# ---------------------------------------------------------------------------
# Admin Proxy Endpoints (LAB TOPIC 8: Simulated failure controls)
# ---------------------------------------------------------------------------

@app.post("/admin/nodes/{node_id}/fail")
def admin_fail_node(node_id: int) -> dict:
    """Proxy failure simulation to storage node (LAB TOPIC 8)."""
    if node_id not in NODES:
        raise HTTPException(status_code=404, detail=f"Node {node_id} not found")

    url = f"{NODES[node_id].rstrip('/')}/admin/fail"
    try:
        resp = requests.post(url, timeout=3.0)
        # Mark dead on gateway immediately to promote primaries without waiting for poll
        mark_dead(node_id)
        record_event(
            actor="gateway",
            event_type="failure",
            detail=f"Admin proxy simulated failure for node {node_id}",
            lamport=gateway_clock.value(),
        )
        return resp.json() if resp.status_code == 200 else {"node_id": node_id, "failed": True}
    except Exception as exc:
        mark_dead(node_id)
        logger.warning("admin fail node %d request exception: %s", node_id, exc)
        return {"node_id": node_id, "failed": True, "error": str(exc)}


@app.post("/admin/nodes/{node_id}/recover")
def admin_recover_node(node_id: int) -> dict:
    """Proxy recover simulation to storage node (LAB TOPIC 8)."""
    if node_id not in NODES:
        raise HTTPException(status_code=404, detail=f"Node {node_id} not found")

    url = f"{NODES[node_id].rstrip('/')}/admin/recover"
    try:
        resp = requests.post(url, timeout=3.0)
        # Immediately re-probe to restore alive status without waiting for poll
        mark_alive_immediate(node_id)
        record_event(
            actor="gateway",
            event_type="recovery",
            detail=f"Admin proxy simulated recovery for node {node_id}",
            lamport=gateway_clock.value(),
        )
        return resp.json() if resp.status_code == 200 else {"node_id": node_id, "failed": False}
    except Exception as exc:
        logger.warning("admin recover node %d request exception: %s", node_id, exc)
        raise HTTPException(status_code=502, detail=f"Failed to recover node {node_id}: {exc}")


# ---------------------------------------------------------------------------
# Gateway Internal Endpoints for Leader (LAB TOPIC 4 & 8)
# ---------------------------------------------------------------------------

class ReplicaAddedRequest(BaseModel):
    chunk_id: str
    node_id: int
    role: str = "backup"


class InternalEventRequest(BaseModel):
    actor: str
    type: str
    detail: str


@app.get("/internal/replication-report")
def internal_replication_report() -> List[dict]:
    """LAB TOPIC 8: list of chunks for leader repair loop."""
    return get_replication_report()


@app.post("/internal/replica-added")
def internal_replica_added(req: ReplicaAddedRequest) -> dict:
    """LAB TOPIC 8: register a newly repaired replica."""
    upsert_replica(chunk_id=req.chunk_id, node_id=req.node_id, role=req.role)
    return {"status": "ok", "chunk_id": req.chunk_id, "node_id": req.node_id, "role": req.role}


@app.post("/internal/event")
def internal_record_event(req: InternalEventRequest) -> dict:
    """LAB TOPIC 4 & 8: append node event to the gateway event log."""
    ev = record_event(
        actor=req.actor,
        event_type=req.type,
        detail=req.detail,
        lamport=gateway_clock.value(),
    )
    return {"status": "ok", "event": ev}
