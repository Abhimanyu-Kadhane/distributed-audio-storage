# Distributed Audio File Storage System — Phase 3 (Final)

> University Distributed Systems Course Project  
> Complete Implementation (Phase 3 of 3) — Bully Leader Election · Failover & Primary Promotion · Leader-Driven Failure Detection & Re-replication/Repair · Synchronous Primary-Backup Replication · Lamport Logical Clocks & Versioning · Cluster Dashboard & Event Audit Log

---

## 1. Overview

The **Distributed Audio File Storage System** is a resilient, load-balanced, replicated distributed storage system built for audio assets (`.mp3`, `.wav`, `.ogg`, `.flac`, `.m4a`).

- **Gateway (FastAPI):** Serves as the public REST API, metadata authority (backed by SQLite in WAL mode), chunking engine (512 KB chunks), load balancer (`least_loaded` / `round_robin`), and cluster health observer.
- **Storage Nodes 1..4 (FastAPI):** Expose RPC endpoints to persist chunks atomically, forward synchronous primary-backup writes (`REPLICATION_FACTOR = 2`), participate in **Bully leader elections**, and simulate crash failures in-memory without requiring Docker container termination.
- **Cluster Leader:** The highest-ID alive storage node wins leadership via the Bully algorithm. The leader executes heartbeat broadcasting, continuous peer failure detection, and automatic chunk re-replication/repair whenever a node crashes.
- **Streamlit Frontend:** A web dashboard providing file playback, version conflict inspection, interactive cluster node controls (simulated crash / recover buttons, leader badge), real-time event logs, and guided demo scripts.

---

## 2. Architecture Diagram

```
                        Browser / Streamlit UI (port 8501)
                                      │
                                      │ HTTP REST / RPC (Lab Topic 1)
                                      ▼
 ┌────────────────────────────────────────────────────────────────────────────────┐
 │                             GATEWAY (port 8000)                                │
 │                                                                                │
 │  ┌──────────────┐   ┌───────────────┐   ┌───────────────────────────────────┐  │
 │  │ chunker.py   │   │ balancer.py   │   │ lamport.py (Logical Clocks, L.T.3)│  │
 │  │ (512KB split)│   │ (least_loaded)│   │                                   │  │
 │  └──────────────┘   └───────────────┘   └───────────────────────────────────┘  │
 │  ┌──────────────────────────────────────────────────────────────────────────┐  │
 │  │ ThreadPoolExecutor (Parallel Chunk Upload / Download — Lab Topic 2)       │  │
 │  └──────────────────────────────────────────────────────────────────────────┘  │
 │  ┌──────────────────────────────────────────────────────────────────────────┐  │
 │  │ health_monitor.py (Daemon thread: 2s poll, promotes primaries on fail)   │  │
 │  └──────────────────────────────────────────────────────────────────────────┘  │
 │  │ SQLite (WAL mode) — metadata.db (files, chunks, replicas tables)         │  │
 │  │ events.py — In-memory ring buffer (max 200 events: election, repair, etc)│  │
 │  │ Admin proxy — /admin/nodes/{id}/fail & /recover                          │  │
 └────────────────────────────────────────────────────────────────────────────────┘
               │                                       │
               │ REST PUT/GET/DELETE                   │ /internal/replication-report
               ▼ (X-Lamport-Time)                      │ /internal/replica-added
 ┌───────────────────────────┐           ┌───────────────────────────┐
 │       Storage Node 1      │           │       Storage Node 2      │
 │ :8001 /data/chunks (vol)  │           │ :8002 /data/chunks (vol)  │
 │ - Bully election follower │           │ - Bully election follower │
 │ - In-memory failure state │           │ - In-memory failure state │
 └───────────────────────────┘           └───────────────────────────┘
               ▲                                       ▲
               │ Heartbeats & /chunks/{id}/pull repair │ Heartbeats & /chunks/{id}/pull
 ┌─────────────┴─────────────┐           ┌─────────────┴─────────────┐
 │       Storage Node 3      │           │    Storage Node 4 (👑)    │
 │ :8003 /data/chunks (vol)  │           │ :8004 /data/chunks (vol)  │
 │ - Next-in-line candidate  │◄─ELECTION─│ 👑 CLUSTER LEADER         │
 │ - Steps up if Node 4 dies │─COORDINATE┤ - Bully Leader Thread     │
 └───────────────────────────┘           │ - Failure Detector (2s)   │
                                         │ - Repair Loop (5s daemon) │
                                         └───────────────────────────┘
```

---

## 3. Bully Election Algorithm (Lab Topic 4)

Storage nodes have unique integer IDs `1..4`. The node with the highest alive ID always becomes coordinator.

### Algorithm Rules
1. **Triggering an Election:**
   - A follower monitor checks every 1 second. If `(now - last_heartbeat) > LEADER_TIMEOUT_SECONDS` (6s), it initiates an election.
   - A recovered node (`POST /admin/recover`) immediately starts an election.
   - At startup, each node waits a small jitter delay (1–3s) and starts an election if no leader is yet declared.
2. **Sending Probes:**
   - The candidate sends `POST /election {from_id: me}` concurrently to all peers with `ID > mine` (`timeout = ELECTION_TIMEOUT_SECONDS = 2s`).
3. **Determining the Winner:**
   - **No higher peer answers `ok=true`:** The candidate wins. It sets `leader_id = me`, broadcasts `POST /coordinator {leader_id: me}` to all peers concurrently, and starts leader duties (heartbeats, failure detector, repair loop).
   - **Any higher peer answers `ok=true`:** The candidate waits up to `ELECTION_TIMEOUT_SECONDS * 2` (4s) for a `POST /coordinator` announcement. If none arrives, it restarts the election.
4. **Step-Down Rule:**
   - If a current leader receives a `/coordinator` or `/heartbeat` from a node with a **higher ID**, it immediately steps down, stops its leader daemon routines, and transitions to a follower.

### Message Sequence Example (Leader 4 Fails, Node 3 Takes Over, Node 4 Recovers)
```
  Node 1              Node 2              Node 3              Node 4 (Leader)
    │                   │                   │                   │
    │                   │                   │                   x (Node 4 fails)
    │                   │                   │
    │                   │                   │ (6s heartbeat timeout expires)
    │                   │                   │ 1. POST /election {from_id: 3}
    │                   │                   ├──────────────────►x (Node 4 dead, no reply)
    │                   │                   │
    │                   │                   │ 2. No higher response -> Node 3 wins!
    │                   │ 3. POST /coordinator {leader_id: 3}
    │                   │◄──────────────────┤
    │ 3. POST /coordinator {leader_id: 3}   │
    │◄──────────────────────────────────────┤
    │                   │                   │ 4. Node 3 begins sending heartbeats (2s)
    │                   │◄──HEARTBEAT (3)───┤
    │◄──HEARTBEAT (3)───┴───────────────────┤
    │                   │                   │
    │                   │                   │                   ▲ (Node 4 recovers!)
    │                   │                   │                   │ 5. POST /admin/recover
    │                   │                   │                   │    starts election: no higher peers
    │                   │                   │                   │    Node 4 wins immediately!
    │                   │                   │ 6. POST /coordinator {leader_id: 4}
    │                   │                   │◄──────────────────┤ (Node 3 steps down: 4 > 3)
    │                   │ 6. POST /coord(4) │                   │
    │                   │◄──────────────────┼───────────────────┤
    │ 6. POST /coord(4) │                   │                   │
    │◄──────────────────┴───────────────────┴───────────────────┤
    │                   │                   │                   │ 7. Node 4 resumes leader heartbeats
```

---

## 4. Failure & Repair Timeline (Lab Topic 8)

### Default Timings
- `HEARTBEAT_INTERVAL_SECONDS = 2.0s` (Leader heartbeats to peers)
- `HEALTH_INTERVAL_SECONDS = 2.0s` (Gateway health monitor polling nodes)
- `HEALTH_FAIL_THRESHOLD = 2` (Consecutive probe failures before gateway marks dead)
- `LEADER_TIMEOUT_SECONDS = 6.0s` (Follower wait before declaring leader dead)
- `ELECTION_TIMEOUT_SECONDS = 2.0s` (Timeout for remote /election calls)
- `REPAIR_INTERVAL_SECONDS = 5.0s` (Leader repair loop iteration period)
- `REPAIR_BATCH = 20` (Maximum chunks re-replicated per repair cycle)

### Timeline Breakdown

| Time Elapsed | Event & System Action |
|--------------|-----------------------|
| **$t = 0.0\text{ s}$** | Node 1 is marked failed via `POST /admin/fail`. Endpoint returns 503 for all subsequent calls. |
| **$t = 0.05\text{ s}$** | **Gateway Failover:** Gateway marks Node 1 dead immediately, calls `promote_primaries(1)`, promoting alive backup replicas (e.g. Node 2) to primary for every chunk previously held by Node 1. Reads/downloads continue without interruption. |
| **$t = 2.0\text{ s}$** | Gateway health monitor confirms Node 1 is dead. `under_replicated_chunks` metric increases. |
| **$t = 2.0 - 4.0\text{ s}$** | **Leader Failure Detector:** Leader (Node 4) probes Node 1 `/health`. After 3 failed probes (6s), leader marks Node 1 dead in its internal liveness table. |
| **$t = 5.0\text{ s}$** | **Leader Repair Loop:**<br>1. Leader requests `GET /internal/replication-report` from Gateway.<br>2. Finds chunks with `< 2` alive replicas.<br>3. Chooses source replica (Node 2) and target replica (Node 3 with fewest chunks).<br>4. Calls `POST node3/chunks/{id}/pull` with source URL & SHA-256.<br>5. Node 3 pulls chunk, validates checksum, writes atomically.<br>6. Leader notifies gateway: `POST /internal/replica-added` (role="backup"). |
| **$t = 7.0\text{ s}$** | Gateway records new replica. `under_replicated_chunks` falls back to **0**. Full $RF=2$ redundancy restored! |
| **$t = 10.0\text{ s}$** | Node 1 recovered via `POST /admin/recover`. It starts an election, detects higher leader Node 4, resumes follower state. Chunks already on Node 1 remain valid (over-replication tolerated). |

---

## 5. Complete API Reference

### Gateway Public APIs

| Method | Endpoint | Request / Parameters | Success | Description |
|--------|----------|----------------------|---------|-------------|
| `GET` | `/health` | None | `200` | Gateway liveness check |
| `POST` | `/files` | Multipart `file`, form `client_id`, `lamport` | `201` | Upload & chunk audio file; returns `FileRecord` (carrying `degraded: bool`) |
| `GET` | `/files` | Query `?all_versions=true\|false` | `200` | List uploaded files; when false, only returns latest version per name |
| `GET` | `/files/{file_id}` | Path `file_id` | `200` / `404` | File metadata with chunk map and replica roles (`primary` / `backup`) |
| `GET` | `/files/{file_id}/download` | Path `file_id` | `200` / `404` | Reassembled audio file stream (reads fall back across alive replicas) |
| `DELETE` | `/files/{file_id}` | Path `file_id` | `200` / `404` | Soft-deletes file and deletes chunk replicas |
| `GET` | `/cluster/status` | None | `200` | Cluster status, `leader_id`, per-node `is_leader`, `under_replicated_chunks` |
| `GET` | `/events` | Query `?limit=50` | `200` | Chronological ring buffer audit events with Lamport timestamps |
| `POST` | `/admin/nodes/{id}/fail` | Path `id` | `200` | Proxy simulated crash failure to target storage node |
| `POST` | `/admin/nodes/{id}/recover` | Path `id` | `200` | Proxy simulated recovery to target storage node |

### Gateway Internal APIs (Leader-Only & System)

| Method | Endpoint | Payload | Description |
|--------|----------|---------|-------------|
| `GET` | `/internal/replication-report` | None | Returns list of `{chunk_id, sha256, size, replicas:[{node_id, role}]}` for active files |
| `POST` | `/internal/replica-added` | `{"chunk_id", "node_id", "role": "backup"}` | Upserts a newly re-replicated replica into SQLite database |
| `POST` | `/internal/event` | `{"actor", "type", "detail"}` | Appends node election, failure, or repair events into the gateway audit log |

### Storage Node APIs (Storage Nodes 1..4)

| Method | Endpoint | Payload / Headers | Description |
|--------|----------|-------------------|-------------|
| `GET` | `/health` | None | Node stats, active requests, Lamport clock, and `leader_id` |
| `PUT` | `/chunks/{id}` | Headers: `X-Chunk-Sha256`, `X-Role`, `X-Backup-Url`, `X-Backup-Node-Id` | Synchronous primary-backup write; primary forwards to backup |
| `GET` | `/chunks/{id}` | None | Retrieve raw binary chunk bytes |
| `DELETE` | `/chunks/{id}` | None | Idempotent chunk deletion |
| `GET` | `/chunks` | None | Inventory of all stored chunks on disk |
| `POST` | `/chunks/{id}/pull` | `{"source_url", "sha256"}` | Instructed by leader: downloads chunk from source, verifies SHA-256, saves locally |
| `POST` | `/election` | `{"from_id": int}` | Bully election probe; replies `ok=true` and triggers own election if `from_id < my_id` |
| `POST` | `/coordinator` | `{"leader_id": int}` | Bully coordinator announcement; records leader, steps down if lower |
| `POST` | `/heartbeat` | `{"leader_id": int}` | Periodic heartbeat from leader; refreshes follower timeout watchdog |
| `GET` | `/leader` | None | Returns current known `leader_id` |
| `POST` | `/admin/fail` | None | Simulated crash: node immediately returns 503 to all calls except recover/status |
| `POST` | `/admin/recover` | None | Simulated recovery: clears failed flag and triggers Bully election |
| `GET` | `/admin/status` | None | Returns `{"node_id", "failed", "leader_id"}` |

---

## 6. Lab Topics Mapping

| Lab Topic | Files & Key Functions | Implementation Details |
|-----------|----------------------|------------------------|
| **Lab 1: Client-Server RPC via REST/HTTP** | `gateway/app/node_client.py`<br>`node/app/main.py`<br>`gateway/app/main.py` | Synchronous HTTP calls with connect/read timeouts wrapping storage procedures (`put_chunk`, `get_chunk`, `delete_chunk`, `get_health`). |
| **Lab 2: Multithreading** | `gateway/app/main.py`<br>`gateway/app/balancer.py`<br>`node/app/storage.py` | `ThreadPoolExecutor` for parallel chunk uploads/downloads. In-memory reservations and active counters protected by `threading.Lock`. |
| **Lab 3: Lamport Logical Clocks** | `gateway/app/lamport.py`<br>`node/app/lamport.py`<br>`gateway/app/db.py`<br>`frontend/app.py` | Clocks tick on send, update on receive via `X-Lamport-Time` headers. Last-writer-wins version conflict resolution using `(lamport_ts, client_id, file_id)`. Streamlit multi-client simulation. |
| **Lab 4: Bully Leader Election** | `node/app/election.py`<br>`node/app/main.py` (`/election`, `/coordinator`, `/heartbeat`) | Classic Bully election among nodes 1..4. Concurrently probes higher peers, waits for coordinator or declares victory. Step-down rule for recovered higher nodes. |
| **Lab 5: Replication** | `gateway/app/main.py` (`_upload_one_chunk`)<br>`node/app/main.py` (`put_chunk`) | Synchronous primary-backup write path with `REPLICATION_FACTOR = 2`. Primary stores locally then forwards to backup before acking gateway. |
| **Lab 6: Load Balancing** | `gateway/app/balancer.py` (`choose_nodes`, `order_read_replicas`) | `least_loaded` chunk placement scoring `chunk_count + unpolled + in_flight`. Read selection routes downloads to replica with lowest active request load. |
| **Lab 8: Failover, Failure Detection & Repair** | `gateway/app/health_monitor.py`<br>`gateway/app/db.py` (`promote_primaries`)<br>`node/app/failure_detector.py`<br>`node/app/repair.py`<br>`node/app/main.py` (`/chunks/{id}/pull`, `/admin/fail`, `/admin/recover`) | - Gateway primary promotion swaps backup to primary in 1 transaction on node failure.<br>- Leader runs failure detector (2s interval, 3 streaks = DEAD).<br>- Leader repair loop re-replicates chunks via atomic pulls and registers new replicas with gateway.<br>- In-memory crash simulation (`/admin/fail`). |

---

## 7. Interactive Demo Script (5 Scenarios)

### Scenario 1: Upload & Spread (Load Balancing & Chunking)
1. Open Streamlit at **http://localhost:8501**, click the **📁 Files** tab.
2. Upload any audio file (> 1 MB, e.g. 2.3 MB mp3).
3. Observe chunk splitting into 512 KB blocks.
4. Switch to the **🖥️ Cluster** tab. Observe the **Chunk Distribution** bar chart showing chunks distributed evenly across all alive nodes.

### Scenario 2: Synchronous Primary-Backup Replication
1. In the **📁 Files** tab, expand **🗂️ Chunk map** for the uploaded file.
2. Verify that every chunk has exactly 2 replicas on distinct nodes with exactly one `(primary)` and one `(backup)`.
3. Check the **📜 Event Log** tab to see `chunk placed` and `upload commit` events.

### Scenario 3: Lamport Clock Conflict Resolution (Last-Writer-Wins)
1. In the sidebar, select **Client A**. Enable **Override next upload timestamp** and enter `10`.
2. Upload a file named `sample.mp3`.
3. In the sidebar, select **Client B**, set timestamp override to `5`, and upload another file named `sample.mp3`.
4. In the Files tab, uncheck **Show all versions**. Notice that **Client A's file remains the LATEST** version because $10 > 5$, resolving the conflict regardless of arrival order.
5. In the sidebar, select **Client B**, set timestamp override to `11`, and upload `sample.mp3`. Notice Client B's file is now the latest version.

### Scenario 4: Node Failure & Seamless Playback / Promotion
1. Go to the **🖥️ Cluster** tab. Find which node is primary for Chunk 0 of an uploaded file (e.g. Node 1).
2. Click the **💥 Fail** button on Node 1. The status changes to `🔴 FAILED (CRASHED)`.
3. Return to the **📁 Files** tab and click **▶️ Play**. Playback streams seamlessly without error because the read path immediately fell back to the alive replica.
4. Expand **🗂️ Chunk map**: the gateway automatically promoted the backup replica to `primary`.
5. Check the **📜 Event Log** tab for the `promotion` event (`"promoted N chunks from node 1"`).

### Scenario 5: Kill the Leader, Bully Re-Election & Automatic Repair
1. In the **🖥️ Cluster** tab, identify the current leader (Node 4 with the `👑 CLUSTER LEADER` badge).
2. Click **💥 Fail** on Node 4.
3. Within 6–10 seconds, watch the **Bully Election** trigger: Node 3 detects the heartbeat timeout and gets elected as the new `👑 CLUSTER LEADER`.
4. Look at the **Under-Replicated Chunks** metric: it increases because chunks on Node 4 lost a replica.
5. Within 5–15 seconds, watch the counter drop back to **0** as the new leader's repair loop re-replicates chunks across the remaining alive nodes.
6. Click **💚 Recover** on Node 4. Node 4 starts an election upon recovery and wins back leadership (highest ID wins).

---

## 8. How to Run & Verify

### Start the Cluster
```bash
cd audio-storage
docker compose up --build -d
```

- **Streamlit Web UI:** http://localhost:8501
- **Gateway Swagger API:** http://localhost:8000/docs
- **Node APIs:** http://localhost:8001 … http://localhost:8004

### Run All Three Verification Smoke Tests
```bash
# Phase 1: Upload, download, delete, error checking
python3 scripts/smoke_test_phase1.py

# Phase 2: Replication, load balance spread, Lamport conflict resolution, events
python3 scripts/smoke_test_phase2.py

# Phase 3: Bully election, node crash, primary promotion, repair loop, re-election, degraded write
python3 scripts/smoke_test_phase3.py
```

---

## 9. Troubleshooting

- **Resetting All Volumes & State:**
  ```bash
  docker compose down -v
  docker compose up --build -d
  ```
- **Checking Service Logs:**
  ```bash
  docker compose logs -f gateway
  docker compose logs -f node4
  docker compose logs -f frontend
  ```
- **Inspecting Cluster Status from CLI:**
  ```bash
  curl -s http://localhost:8000/cluster/status | python3 -m json.tool
  ```
- **Recovering Nodes Manually via Gateway Admin Proxy:**
  ```bash
  curl -X POST http://localhost:8000/admin/nodes/1/recover
  curl -X POST http://localhost:8000/admin/nodes/4/recover
  ```

---

## 10. Known Limitations

1. **Single Gateway as SPOF:** The gateway is a single container. While storage nodes are fully distributed, replicated, and elect leaders, gateway high availability would require a gateway cluster backed by distributed consensus (e.g. Raft).
2. **No Authentication / Authorization:** All endpoints are open; in a production deployment, mTLS or JWT authentication would be required between gateway, storage nodes, and clients.
3. **Orphan Chunks on Delete:** If a file is deleted while a storage node holding some of its chunks is crashed or down, that node will retain orphan chunk files on its volume when it recovers. A garbage-collection sweep would be needed to clean unreferenced chunks.
4. **Simulated Failures:** Node crashes are simulated in-memory via `/admin/fail` to enable instant recovery and fast testing without Docker daemon intervention. A real Docker container restart clears this in-memory flag.

---

## 11. Assumptions Recorded

1. **Replication Factor:** Defaults to $RF = 2$. Chunks are replicated across 2 distinct nodes. If fewer than 2 nodes are alive during upload, degraded write is accepted ($\ge 1$ replica) and marked with `"degraded": true`. Only if 0 nodes are alive does upload return 503.
2. **Over-Replication:** If a node recovers after its chunks were re-replicated by the repair loop, chunks may have 3 alive replicas. Over-replication is intentionally tolerated and never pruned.
3. **Bully Election Jitter:** On startup, storage nodes employ a random 1–3s delay to allow peer services to reach a ready network state before triggering elections.
4. **Leader Failure Detector:** Probes peers every 2 seconds. A peer is deemed DEAD after 3 consecutive failed health probes (6 seconds), and ALIVE on the first successful probe.
5. **Repair Loop Selection:** Prefers source replica with lowest `active_requests`, and target replica with lowest `chunk_count` plus assigned repair counter to balance repair load evenly.
