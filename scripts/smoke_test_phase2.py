#!/usr/bin/env python3
"""Phase 2 smoke test — talks to http://localhost:8000.

Verification checks:
(a) after uploading a 3 MB file every chunk has exactly 2 replicas on 2 distinct nodes
    with exactly one primary;
(b) upload 6 more files, then max(chunk_count) - min(chunk_count) across nodes <= 3
    and no node has zero chunks;
(c) Lamport: upload "song.mp3" as client_id=A with lamport=10, then "song.mp3" as
    client_id=B with lamport=5 (arrives later) -> GET /files?all_versions=false shows
    A's file as latest; upload again as B with lamport=11 -> B is now latest;
(d) a node's /health returns lamport > 0 and responses carry X-Lamport-Time;
(e) GET /events returns events with non-decreasing wall order and Lamport values present;
(f) download of a replicated file is byte-identical.

Usage:
    python scripts/smoke_test_phase2.py

Exits non-zero if any check fails.
"""
import io
import os
import sys
import time

import requests

GATEWAY = os.environ.get("GATEWAY_URL", "http://localhost:8000").rstrip("/")
NODE1 = os.environ.get("NODE1_URL", "http://localhost:8001").rstrip("/")
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE_BYTES", "524288"))  # 512 KB

_GREEN = "\033[32m"
_RED = "\033[31m"
_RESET = "\033[0m"

failures = 0


def check(label: str, condition: bool, detail: str = "") -> None:
    global failures
    status = f"{_GREEN}PASS{_RESET}" if condition else f"{_RED}FAIL{_RESET}"
    suffix = f" — {detail}" if detail and not condition else ""
    print(f"  [{status}] {label}{suffix}")
    if not condition:
        failures += 1


def section(title: str) -> None:
    print(f"\n{'='*65}")
    print(f"  {title}")
    print("=" * 65)


def wait_for_gateway(timeout: int = 30) -> None:
    print(f"Waiting for gateway at {GATEWAY} …")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{GATEWAY}/health", timeout=3)
            if r.status_code == 200:
                print("  Gateway is up.\n")
                return
        except requests.RequestException:
            pass
        time.sleep(1)
    print(f"  [{_RED}FAIL{_RESET}] Gateway did not become ready in {timeout}s")
    sys.exit(1)


def run_tests() -> None:
    # Clean up any leftover files from previous runs to ensure reproducible test state
    try:
        prev_files = requests.get(f"{GATEWAY}/files", timeout=5).json()
        for f in prev_files:
            requests.delete(f"{GATEWAY}/files/{f['file_id']}", timeout=5)
        if prev_files:
            time.sleep(2.5)
    except Exception:
        pass

    # ---------------------------------------------------------------------------
    # Test (a): Upload a 3 MB file and verify chunk replication & roles
    # ---------------------------------------------------------------------------
    section("TEST (a): 3 MB upload — 2 replicas per chunk, distinct nodes, 1 primary")

    FILE_SIZE = 3 * 1024 * 1024  # 3 MB
    raw_3mb = os.urandom(FILE_SIZE)

    resp_a = requests.post(
        f"{GATEWAY}/files",
        files={"file": ("test3mb.mp3", io.BytesIO(raw_3mb), "audio/mpeg")},
        data={"client_id": "tester"},
        timeout=(5, 60),
    )
    check("Upload 3 MB file returns 201", resp_a.status_code == 201, f"got {resp_a.status_code}")

    file_id_3mb: str | None = None
    if resp_a.status_code == 201:
        body = resp_a.json()
        file_id_3mb = body["file_id"]

        detail_resp = requests.get(f"{GATEWAY}/files/{file_id_3mb}", timeout=(5, 10))
        check("GET /files/{id} returns 200", detail_resp.status_code == 200)

        chunk_map = detail_resp.json().get("chunk_map", [])
        check("Chunk map is non-empty", len(chunk_map) > 0, f"got {len(chunk_map)} chunks")

        all_two_replicas = True
        all_distinct = True
        all_one_primary = True
        all_one_backup = True

        for c in chunk_map:
            nodes = c.get("nodes", [])
            if len(nodes) != 2:
                all_two_replicas = False
            node_ids = [n["node_id"] for n in nodes]
            if len(set(node_ids)) != len(node_ids):
                all_distinct = False
            roles = [n["role"] for n in nodes]
            if roles.count("primary") != 1:
                all_one_primary = False
            if roles.count("backup") != 1:
                all_one_backup = False

        check("Every chunk has exactly 2 replicas", all_two_replicas)
        check("Every chunk has replicas on distinct nodes", all_distinct)
        check("Every chunk has exactly one primary and one backup", all_one_primary and all_one_backup)

    # ---------------------------------------------------------------------------
    # Test (b): Upload 6 more files — verify load balancing distribution
    # ---------------------------------------------------------------------------
    section("TEST (b): Load balancing — max-min diff <= 3 and no node has 0 chunks")

    uploaded_extra = 0
    for i in range(6):
        # 1.5 MB file = 3 chunks
        extra_data = os.urandom(1_500_000)
        resp_extra = requests.post(
            f"{GATEWAY}/files",
            files={"file": (f"balanced_{i}.mp3", io.BytesIO(extra_data), "audio/mpeg")},
            data={"client_id": f"worker_{i}"},
            timeout=(5, 30),
        )
        if resp_extra.status_code == 201:
            uploaded_extra += 1

    check("Uploaded 6 additional files", uploaded_extra == 6, f"uploaded={uploaded_extra}")

    # Allow background health monitor (2s poll interval) to poll all nodes
    time.sleep(2.5)

    # Inspect cluster status
    status_resp = requests.get(f"{GATEWAY}/cluster/status", timeout=(5, 10))
    check("GET /cluster/status returns 200", status_resp.status_code == 200)

    if status_resp.status_code == 200:
        c_status = status_resp.json()
        nodes = c_status.get("nodes", [])
        chunk_counts = [n["chunk_count"] for n in nodes]
        check(
            "All 4 storage nodes are tracked in cluster status",
            len(nodes) == 4,
            f"nodes={len(nodes)}",
        )
        check(
            "No node has zero chunks",
            all(cnt > 0 for cnt in chunk_counts),
            f"counts={chunk_counts}",
        )
        diff = max(chunk_counts) - min(chunk_counts) if chunk_counts else 999
        check(
            "max(chunk_count) - min(chunk_count) across nodes <= 3",
            diff <= 3,
            f"counts={chunk_counts}, diff={diff}",
        )

    # ---------------------------------------------------------------------------
    # Test (c): Lamport clocks — versioning and conflict resolution
    # ---------------------------------------------------------------------------
    section("TEST (c): Lamport clocks — last-writer-wins version resolution")

    # Clean up any existing song.mp3 from previous operations
    try:
        cur_files = requests.get(f"{GATEWAY}/files", timeout=5).json()
        for f in cur_files:
            if f["name"] == "song.mp3":
                requests.delete(f"{GATEWAY}/files/{f['file_id']}", timeout=5)
    except Exception:
        pass

    # 1. Upload "song.mp3" as client_id=A with lamport=10
    raw_song = b"audio content version A"
    resp_song_a = requests.post(
        f"{GATEWAY}/files",
        files={"file": ("song.mp3", io.BytesIO(raw_song), "audio/mpeg")},
        data={"client_id": "A", "lamport": "10"},
        timeout=(5, 30),
    )
    check("Upload song.mp3 (A, lamport=10) returns 201", resp_song_a.status_code == 201)
    file_id_a = resp_song_a.json().get("file_id") if resp_song_a.status_code == 201 else None

    # 2. Upload "song.mp3" as client_id=B with lamport=5 (arrives later, but lower timestamp)
    raw_song_b = b"audio content version B (lower timestamp)"
    resp_song_b = requests.post(
        f"{GATEWAY}/files",
        files={"file": ("song.mp3", io.BytesIO(raw_song_b), "audio/mpeg")},
        data={"client_id": "B", "lamport": "5"},
        timeout=(5, 30),
    )
    check("Upload song.mp3 (B, lamport=5) returns 201", resp_song_b.status_code == 201)

    # 3. GET /files?all_versions=false should return A's file as latest
    list_latest_1 = requests.get(f"{GATEWAY}/files?all_versions=false", timeout=(5, 10))
    check("GET /files?all_versions=false returns 200", list_latest_1.status_code == 200)

    if list_latest_1.status_code == 200:
        songs = [f for f in list_latest_1.json() if f["name"] == "song.mp3"]
        check("Single latest version for song.mp3 returned", len(songs) == 1, f"got {len(songs)}")
        if songs:
            check(
                "A's version (lamport=10) is latest over B (lamport=5)",
                songs[0]["file_id"] == file_id_a and songs[0]["client_id"] == "A",
                f"latest={songs[0]}",
            )

    # 4. Upload "song.mp3" again as B with lamport=11
    raw_song_b2 = b"audio content version B (highest timestamp)"
    resp_song_b2 = requests.post(
        f"{GATEWAY}/files",
        files={"file": ("song.mp3", io.BytesIO(raw_song_b2), "audio/mpeg")},
        data={"client_id": "B", "lamport": "11"},
        timeout=(5, 30),
    )
    check("Upload song.mp3 (B, lamport=11) returns 201", resp_song_b2.status_code == 201)
    file_id_b2 = resp_song_b2.json().get("file_id") if resp_song_b2.status_code == 201 else None

    # 5. GET /files?all_versions=false should now return B's new file
    list_latest_2 = requests.get(f"{GATEWAY}/files?all_versions=false", timeout=(5, 10))
    if list_latest_2.status_code == 200:
        songs2 = [f for f in list_latest_2.json() if f["name"] == "song.mp3"]
        check("Single latest version for song.mp3 returned", len(songs2) == 1)
        if songs2:
            check(
                "B's version (lamport=11) is now latest",
                songs2[0]["file_id"] == file_id_b2 and songs2[0]["client_id"] == "B",
                f"latest={songs2[0]}",
            )

    # ---------------------------------------------------------------------------
    # Test (d): Node /health returns lamport > 0 and responses carry X-Lamport-Time
    # ---------------------------------------------------------------------------
    section("TEST (d): Node health and X-Lamport-Time headers")

    try:
        node_health_resp = requests.get(f"{NODE1}/health", timeout=(3, 5))
        check("Node 1 /health returns 200", node_health_resp.status_code == 200)
        node_body = node_health_resp.json()
        check(
            "Node /health body returns 'lamport' > 0",
            node_body.get("lamport", 0) > 0,
            f"got {node_body.get('lamport')}",
        )
        x_lamp = node_health_resp.headers.get("X-Lamport-Time")
        check(
            "Node response carries X-Lamport-Time header > 0",
            x_lamp is not None and int(x_lamp) > 0,
            f"header={x_lamp}",
        )
    except Exception as exc:
        check("Node 1 /health check succeeds", False, str(exc))

    # ---------------------------------------------------------------------------
    # Test (e): GET /events returns events with non-decreasing wall order & Lamport
    # ---------------------------------------------------------------------------
    section("TEST (e): GET /events — Lamport values present and non-decreasing wall order")

    events_resp = requests.get(f"{GATEWAY}/events?limit=50", timeout=(5, 10))
    check("GET /events returns 200", events_resp.status_code == 200)

    if events_resp.status_code == 200:
        events = events_resp.json()
        check("Events list is non-empty", len(events) > 0, f"count={len(events)}")

        all_have_lamport = all(e.get("lamport") is not None and e["lamport"] >= 0 for e in events)
        all_have_wall = all(e.get("wall_time") is not None for e in events)
        check("All events contain non-negative Lamport values", all_have_lamport)
        check("All events contain wall_time", all_have_wall)

        # Reversing newest-first events provides chronological order
        chronological = list(reversed(events))
        wall_monotonic = True
        seq_monotonic = True
        for i in range(len(chronological) - 1):
            if chronological[i]["wall_time"] > chronological[i + 1]["wall_time"]:
                wall_monotonic = False
            if chronological[i]["seq"] > chronological[i + 1]["seq"]:
                seq_monotonic = False

        check("Events have non-decreasing wall time order", wall_monotonic)
        check("Events have non-decreasing sequence order", seq_monotonic)

    # ---------------------------------------------------------------------------
    # Test (f): Download replicated file — must be byte-identical
    # ---------------------------------------------------------------------------
    section("TEST (f): Download replicated file — byte-identical verification")

    if file_id_3mb:
        dl_resp = requests.get(f"{GATEWAY}/files/{file_id_3mb}/download", timeout=(5, 60))
        check("Download returns 200", dl_resp.status_code == 200)
        if dl_resp.status_code == 200:
            check(
                "Downloaded content matches original 3 MB raw data",
                dl_resp.content == raw_3mb,
                f"lengths: dl={len(dl_resp.content)}, orig={len(raw_3mb)}",
            )
    else:
        check("Download test skipped (upload failed)", False)

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------
    section("SUMMARY")
    if failures == 0:
        print(f"  All Phase 2 checks { _GREEN }PASSED ✅{ _RESET }\n")
    else:
        print(f"  { failures } check(s) { _RED }FAILED ❌{ _RESET }\n")


if __name__ == "__main__":
    wait_for_gateway()
    run_tests()
    sys.exit(0 if failures == 0 else 1)
