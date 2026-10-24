#!/usr/bin/env python3
"""Phase 1 smoke test — talks to http://localhost:8000.

Usage:
    python scripts/smoke_test_phase1.py

Exits non-zero if any check fails.
"""
import io
import math
import os
import sys
import time

import requests

GATEWAY = os.environ.get("GATEWAY_URL", "http://localhost:8000")
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
    print(f"\n{'='*60}")
    print(f"  {title}")
    print("=" * 60)


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
    # ---------------------------------------------------------------------------
    # Test 1: Upload a ~2.3 MB random-bytes mp3
    # ---------------------------------------------------------------------------
    section("TEST 1: Upload ~2.3 MB file")

    FILE_SIZE = 2_411_724  # ~2.3 MB  → ceil(2411724 / 524288) = 5 chunks
    raw_data = os.urandom(FILE_SIZE)
    expected_chunks = math.ceil(FILE_SIZE / CHUNK_SIZE)

    resp = requests.post(
        f"{GATEWAY}/files",
        files={"file": ("test.mp3", io.BytesIO(raw_data), "audio/mpeg")},
        timeout=(5, 60),
    )
    check(
        "Upload returns 201",
        resp.status_code == 201,
        f"status={resp.status_code} body={resp.text[:300]}",
    )

    file_id: str | None = None
    if resp.status_code == 201:
        body = resp.json()
        file_id = body["file_id"]
        actual_chunks = body["chunk_count"]
        check(
            f"chunk_count == ceil(size/chunk_size) → {expected_chunks}",
            actual_chunks == expected_chunks,
            f"got {actual_chunks}",
        )

        chunk_map = body.get("chunk_map", [])
        nodes_used: set[int] = set()
        for c in chunk_map:
            for n in c["nodes"]:
                nodes_used.add(n["node_id"] if isinstance(n, dict) else n)
        check(
            "Chunks land on at least 2 different nodes",
            len(nodes_used) >= 2,
            f"nodes used: {nodes_used}",
        )
    else:
        print("  Skipping downstream checks due to upload failure.")

    # ---------------------------------------------------------------------------
    # Test 2: Download and verify byte equality
    # ---------------------------------------------------------------------------
    section("TEST 2: Download — bytes must match original")

    if file_id:
        resp2 = requests.get(
            f"{GATEWAY}/files/{file_id}/download", timeout=(5, 120)
        )
        check(
            "Download returns 200",
            resp2.status_code == 200,
            f"status={resp2.status_code}",
        )
        if resp2.status_code == 200:
            check(
                "Downloaded bytes are identical to uploaded bytes",
                resp2.content == raw_data,
                f"sizes: uploaded={len(raw_data)}, downloaded={len(resp2.content)}",
            )
    else:
        print("  Skipped (no file_id from upload).")

    # ---------------------------------------------------------------------------
    # Test 3: Reject unsupported extension
    # ---------------------------------------------------------------------------
    section("TEST 3: Reject .txt upload with 400")

    resp3 = requests.post(
        f"{GATEWAY}/files",
        files={"file": ("readme.txt", io.BytesIO(b"hello world"), "text/plain")},
        timeout=(5, 10),
    )
    check(
        "Returns 400 for .txt file",
        resp3.status_code == 400,
        f"got {resp3.status_code}",
    )

    # ---------------------------------------------------------------------------
    # Test 4: Reject empty .mp3
    # ---------------------------------------------------------------------------
    section("TEST 4: Reject empty .mp3 with 400")

    resp4 = requests.post(
        f"{GATEWAY}/files",
        files={"file": ("empty.mp3", io.BytesIO(b""), "audio/mpeg")},
        timeout=(5, 10),
    )
    check(
        "Returns 400 for empty .mp3",
        resp4.status_code == 400,
        f"got {resp4.status_code}",
    )

    # ---------------------------------------------------------------------------
    # Test 5: Delete file and confirm it disappears from listing
    # ---------------------------------------------------------------------------
    section("TEST 5: Delete file — must not appear in GET /files")

    if file_id:
        del_resp = requests.delete(
            f"{GATEWAY}/files/{file_id}", timeout=(5, 10)
        )
        check(
            "DELETE returns 200",
            del_resp.status_code == 200,
            f"got {del_resp.status_code}",
        )

        list_resp = requests.get(f"{GATEWAY}/files", timeout=(5, 10))
        check(
            "GET /files returns 200 after delete",
            list_resp.status_code == 200,
        )
        if list_resp.status_code == 200:
            ids = [f["file_id"] for f in list_resp.json()]
            check(
                "Deleted file absent from GET /files listing",
                file_id not in ids,
                f"still found {file_id}",
            )

        detail_resp = requests.get(
            f"{GATEWAY}/files/{file_id}", timeout=(5, 10)
        )
        check(
            "GET /files/{id} returns 404 after delete",
            detail_resp.status_code == 404,
            f"got {detail_resp.status_code}",
        )
    else:
        print("  Skipped (no file_id).")

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------
    section("SUMMARY")
    if failures == 0:
        print("  All checks PASSED ✅\n")
    else:
        print(f"  {failures} check(s) FAILED ❌\n")


if __name__ == "__main__":
    wait_for_gateway()
    run_tests()
    sys.exit(0 if failures == 0 else 1)
