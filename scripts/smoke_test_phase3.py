#!/usr/bin/env python3
"""Phase 3 smoke test — talks to http://localhost:8000.

Verification checks (generous timeouts, polling every 1 s):
 1. Within 15 s of startup /cluster/status reports leader_id == 4.
 2. Upload a 3 MB file. Fail the node that is primary for chunk 0 -> download
    is still byte-identical; the promotion happened (chunk 0's primary is now
    an alive node); under_replicated_chunks > 0 shortly after, then returns to 0
    within 30 s (repair); each chunk of the file again has >= 2 alive replicas.
 3. Recover that node -> it is alive again in status within 10 s.
 4. Fail node 4 (the leader) -> within 15 s leader_id == 3 and events contain
    election entries. Upload a new file during this time and verify it succeeds
    and downloads correctly.
 5. Recover node 4 -> within 15 s leader_id == 4 again (Bully: the highest ID wins).
 6. Fail two nodes, upload a file -> succeeds (degraded or not), downloads correctly;
    recover both nodes and confirm under_replicated_chunks returns to 0.
 7. Restore everything (recover all nodes) at the end of the script even if a check
    fails (try/finally).

Usage:
    python scripts/smoke_test_phase3.py
"""
import io
import os
import sys
import time
from typing import Callable, Optional

import requests

GATEWAY = os.environ.get("GATEWAY_URL", "http://localhost:8000").rstrip("/")
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
    print(f"\n{'='*70}")
    print(f"  {title}")
    print("=" * 70)


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


def poll_until(
    predicate: Callable[[], bool],
    timeout: float = 30.0,
    interval: float = 1.0,
    desc: str = "",
) -> bool:
    """Poll predicate every interval until True or timeout expires."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if predicate():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


def recover_all_nodes() -> None:
    """Ensure all 4 nodes are recovered."""
    print("  Ensuring all nodes are recovered...")
    for nid in (1, 2, 3, 4):
        try:
            requests.post(f"{GATEWAY}/admin/nodes/{nid}/recover", timeout=3)
        except Exception:
            pass
    # Give a brief moment for status to settle
    time.sleep(2)


def run_tests() -> None:
    try:
        # Pre-cleanup: recover all nodes and clean files
        recover_all_nodes()
        try:
            files = requests.get(f"{GATEWAY}/files", timeout=5).json()
            for f in files:
                requests.delete(f"{GATEWAY}/files/{f['file_id']}", timeout=5)
        except Exception:
            pass

        # -----------------------------------------------------------------------
        # Check 1: Startup election -> leader_id == 4 within 15s
        # -----------------------------------------------------------------------
        section("CHECK 1: Within 15 s of startup /cluster/status reports leader_id == 4")

        def check_leader_4() -> bool:
            r = requests.get(f"{GATEWAY}/cluster/status", timeout=3)
            if r.status_code == 200:
                body = r.json()
                return body.get("leader_id") == 4
            return False

        leader_4_ok = poll_until(check_leader_4, timeout=15.0, interval=1.0, desc="leader_id == 4")
        st = requests.get(f"{GATEWAY}/cluster/status", timeout=3).json()
        check(
            "Leader is Node 4 within 15s",
            leader_4_ok,
            f"reported leader_id={st.get('leader_id')}",
        )

        # -----------------------------------------------------------------------
        # Check 2: Upload 3 MB file, fail primary of chunk 0, test download,
        #          promotion, and repair loop.
        # -----------------------------------------------------------------------
        section("CHECK 2: Fail chunk 0 primary -> download succeeds, promotion, repair to 0")

        FILE_SIZE = 3 * 1024 * 1024  # 3 MB
        raw_3mb = os.urandom(FILE_SIZE)

        upload_resp = requests.post(
            f"{GATEWAY}/files",
            files={"file": ("failover_test.mp3", io.BytesIO(raw_3mb), "audio/mpeg")},
            data={"client_id": "phase3_tester"},
            timeout=(5, 60),
        )
        check("Upload 3 MB file returns 201", upload_resp.status_code == 201)

        file_id = upload_resp.json().get("file_id") if upload_resp.status_code == 201 else None
        check("Obtained file_id from upload", file_id is not None)

        if file_id:
            detail_resp = requests.get(f"{GATEWAY}/files/{file_id}", timeout=5)
            check("GET /files/{id} returns 200", detail_resp.status_code == 200)
            chunk_map = detail_resp.json().get("chunk_map", [])
            check("File has chunk_map", len(chunk_map) > 0)

            chunk0 = chunk_map[0]
            primary_replica = next((n for n in chunk0["nodes"] if n["role"] == "primary"), None)
            backup_replica = next((n for n in chunk0["nodes"] if n["role"] == "backup"), None)

            check("Chunk 0 has primary replica", primary_replica is not None)
            check("Chunk 0 has backup replica", backup_replica is not None)

            dead_primary_id = primary_replica["node_id"] if primary_replica else 1
            backup_node_id = backup_replica["node_id"] if backup_replica else 2

            print(f"  Failing primary node {dead_primary_id} for chunk 0...")
            fail_resp = requests.post(f"{GATEWAY}/admin/nodes/{dead_primary_id}/fail", timeout=5)
            check("POST /admin/nodes/{id}/fail returns 200", fail_resp.status_code == 200)

            # Download must be byte-identical while primary is dead
            dl_resp = requests.get(f"{GATEWAY}/files/{file_id}/download", timeout=(5, 60))
            check("Download while primary is dead returns 200", dl_resp.status_code == 200)
            if dl_resp.status_code == 200:
                check("Downloaded bytes are byte-identical", dl_resp.content == raw_3mb)

            # Check promotion: chunk 0's primary must now be an alive node
            detail2 = requests.get(f"{GATEWAY}/files/{file_id}", timeout=5).json()
            chunk0_new = detail2["chunk_map"][0]
            new_primary = next((n for n in chunk0_new["nodes"] if n["role"] == "primary"), None)
            check(
                "Primary promotion happened: chunk 0 primary is now an alive node",
                new_primary is not None and new_primary["node_id"] != dead_primary_id,
                f"new primary: {new_primary}",
            )

            # under_replicated_chunks > 0 shortly after
            def check_under_replicated_positive() -> bool:
                s = requests.get(f"{GATEWAY}/cluster/status", timeout=3).json()
                return s.get("under_replicated_chunks", 0) > 0

            under_rep_positive = poll_until(
                check_under_replicated_positive,
                timeout=10.0,
                interval=1.0,
                desc="under_replicated_chunks > 0",
            )
            check("under_replicated_chunks > 0 after failure", under_rep_positive)

            # Leader repair loop should return under_replicated_chunks to 0 within 30s
            print("  Waiting for leader repair loop to re-replicate chunks to alive nodes...")

            def check_under_replicated_zero() -> bool:
                s = requests.get(f"{GATEWAY}/cluster/status", timeout=3).json()
                return s.get("under_replicated_chunks", 0) == 0

            repaired_to_zero = poll_until(
                check_under_replicated_zero,
                timeout=30.0,
                interval=1.0,
                desc="under_replicated_chunks == 0",
            )
            s_after_repair = requests.get(f"{GATEWAY}/cluster/status", timeout=3).json()
            check(
                "under_replicated_chunks returns to 0 within 30s (repair)",
                repaired_to_zero,
                f"remaining under_replicated={s_after_repair.get('under_replicated_chunks')}",
            )

            # Verify each chunk of the file again has >= 2 alive replicas
            rep_report = requests.get(f"{GATEWAY}/internal/replication-report", timeout=5).json()
            alive_nids = {n["node_id"] for n in s_after_repair["nodes"] if n["alive"]}
            file_chunks = [c for c in rep_report if c["chunk_id"].startswith(file_id)]
            all_chunks_have_2_alive = True
            for fc in file_chunks:
                alive_reps = [r for r in fc["replicas"] if r["node_id"] in alive_nids]
                if len(alive_reps) < 2:
                    all_chunks_have_2_alive = False
                    break
            check("Each chunk of file has >= 2 alive replicas after repair", all_chunks_have_2_alive)

            # -------------------------------------------------------------------
            # Check 3: Recover that node -> alive again in status within 10s
            # -------------------------------------------------------------------
            section(f"CHECK 3: Recover node {dead_primary_id} -> alive in status within 10s")

            rec_resp = requests.post(f"{GATEWAY}/admin/nodes/{dead_primary_id}/recover", timeout=5)
            check(f"POST /admin/nodes/{dead_primary_id}/recover returns 200", rec_resp.status_code == 200)

            def check_node_alive() -> bool:
                s = requests.get(f"{GATEWAY}/cluster/status", timeout=3).json()
                for n in s.get("nodes", []):
                    if n["node_id"] == dead_primary_id:
                        return bool(n.get("alive"))
                return False

            node_alive_again = poll_until(
                check_node_alive,
                timeout=10.0,
                interval=1.0,
                desc=f"node {dead_primary_id} alive",
            )
            check(f"Node {dead_primary_id} is alive again in status within 10s", node_alive_again)

        # -----------------------------------------------------------------------
        # Check 4: Fail node 4 (the leader) -> within 15s leader_id == 3 + election events
        #          Upload a new file during this time, verify download succeeds
        # -----------------------------------------------------------------------
        section("CHECK 4: Fail node 4 (leader) -> leader_id == 3 within 15s, upload succeeds")

        fail_leader_resp = requests.post(f"{GATEWAY}/admin/nodes/4/fail", timeout=5)
        check("Fail node 4 returns 200", fail_leader_resp.status_code == 200)

        def check_leader_3() -> bool:
            s = requests.get(f"{GATEWAY}/cluster/status", timeout=3).json()
            return s.get("leader_id") == 3

        leader_3_ok = poll_until(
            check_leader_3,
            timeout=15.0,
            interval=1.0,
            desc="leader_id == 3",
        )
        s_leader3 = requests.get(f"{GATEWAY}/cluster/status", timeout=3).json()
        check(
            "Leader re-elected to Node 3 within 15s",
            leader_3_ok,
            f"reported leader_id={s_leader3.get('leader_id')}",
        )

        events_resp = requests.get(f"{GATEWAY}/events?limit=50", timeout=5).json()
        has_election_events = any(e.get("type") == "election" for e in events_resp)
        check("Events log contains election entries", has_election_events)

        # Upload a new file while node 4 is failed
        raw_new = os.urandom(1_200_000)
        up_new_resp = requests.post(
            f"{GATEWAY}/files",
            files={"file": ("leader_down_test.mp3", io.BytesIO(raw_new), "audio/mpeg")},
            data={"client_id": "client_during_election"},
            timeout=(5, 30),
        )
        check("Upload during leader failure returns 201", up_new_resp.status_code == 201)
        new_file_id = up_new_resp.json().get("file_id") if up_new_resp.status_code == 201 else None

        if new_file_id:
            dl_new = requests.get(f"{GATEWAY}/files/{new_file_id}/download", timeout=(5, 30))
            check("Download of file uploaded during leader fail returns 200", dl_new.status_code == 200)
            if dl_new.status_code == 200:
                check("Downloaded bytes match original data", dl_new.content == raw_new)

        # -----------------------------------------------------------------------
        # Check 5: Recover node 4 -> within 15s leader_id == 4 again (Bully election)
        # -----------------------------------------------------------------------
        section("CHECK 5: Recover node 4 -> leader_id == 4 again within 15s (Bully: highest ID wins)")

        rec_4_resp = requests.post(f"{GATEWAY}/admin/nodes/4/recover", timeout=5)
        check("Recover node 4 returns 200", rec_4_resp.status_code == 200)

        leader_4_reclaimed = poll_until(
            check_leader_4,
            timeout=15.0,
            interval=1.0,
            desc="leader_id == 4 reclaimed",
        )
        s_leader4 = requests.get(f"{GATEWAY}/cluster/status", timeout=3).json()
        check(
            "Node 4 reclaims leadership within 15s",
            leader_4_reclaimed,
            f"reported leader_id={s_leader4.get('leader_id')}",
        )

        # -----------------------------------------------------------------------
        # Check 6: Fail two nodes, upload file (degraded or not), download, recover, repair to 0
        # -----------------------------------------------------------------------
        section("CHECK 6: Fail two nodes -> upload & download succeed; recover & repair to 0")

        requests.post(f"{GATEWAY}/admin/nodes/1/fail", timeout=5)
        requests.post(f"{GATEWAY}/admin/nodes/2/fail", timeout=5)

        raw_two_fail = os.urandom(1_000_000)
        up_two_resp = requests.post(
            f"{GATEWAY}/files",
            files={"file": ("two_fail_test.mp3", io.BytesIO(raw_two_fail), "audio/mpeg")},
            data={"client_id": "two_fail_client"},
            timeout=(5, 30),
        )
        check("Upload with 2 nodes dead returns 201", up_two_resp.status_code == 201)
        two_file_id = up_two_resp.json().get("file_id") if up_two_resp.status_code == 201 else None

        if two_file_id:
            dl_two = requests.get(f"{GATEWAY}/files/{two_file_id}/download", timeout=(5, 30))
            check("Download with 2 nodes dead returns 200", dl_two.status_code == 200)
            if dl_two.status_code == 200:
                check("Downloaded bytes match original data", dl_two.content == raw_two_fail)

        print("  Recovering nodes 1 and 2...")
        requests.post(f"{GATEWAY}/admin/nodes/1/recover", timeout=5)
        requests.post(f"{GATEWAY}/admin/nodes/2/recover", timeout=5)

        print("  Waiting for repair loop to restore under_replicated_chunks to 0...")
        repaired_after_two = poll_until(
            check_under_replicated_zero,
            timeout=30.0,
            interval=1.0,
            desc="under_replicated_chunks == 0 after 2-node recovery",
        )
        s_after_two = requests.get(f"{GATEWAY}/cluster/status", timeout=3).json()
        check(
            "under_replicated_chunks returns to 0 after recovery of both nodes",
            repaired_after_two,
            f"remaining under_replicated={s_after_two.get('under_replicated_chunks')}",
        )

    finally:
        # -----------------------------------------------------------------------
        # Check 7: Restore everything (recover all nodes) at the end (try/finally)
        # -----------------------------------------------------------------------
        section("CHECK 7: Cleanup — recover all nodes even if a check failed")
        recover_all_nodes()
        final_status = requests.get(f"{GATEWAY}/cluster/status", timeout=5).json()
        alive_nodes = [n["node_id"] for n in final_status.get("nodes", []) if n["alive"]]
        check("All 4 nodes restored and alive", len(alive_nodes) == 4, f"alive={alive_nodes}")

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------
    section("PHASE 3 TEST SUMMARY")
    if failures == 0:
        print(f"  All Phase 3 checks {_GREEN}PASSED ✅{_RESET}\n")
    else:
        print(f"  {failures} check(s) {_RED}FAILED ❌{_RESET}\n")


if __name__ == "__main__":
    wait_for_gateway()
    run_tests()
    sys.exit(0 if failures == 0 else 1)
