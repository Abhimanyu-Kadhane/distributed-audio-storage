"""Thin API wrappers for the Streamlit frontend.

Each function maps to one gateway endpoint (LAB TOPIC 1: RPC).
All errors propagate as requests.HTTPError or requests.RequestException
so the UI layer can catch them uniformly.
"""
import os
from typing import Optional

import requests

GATEWAY_URL: str = os.environ.get("GATEWAY_URL", "http://localhost:8000").rstrip("/")
_TIMEOUT = (5, 30)  # (connect_s, read_s)


def upload_file(
    file_bytes: bytes,
    filename: str,
    content_type: str,
    client_id: str = "default",
    lamport: Optional[int] = None,
) -> dict:
    """POST /files — upload an audio file with client_id and optional lamport clock timestamp."""
    data = {"client_id": client_id}
    if lamport is not None:
        data["lamport"] = str(lamport)

    resp = requests.post(
        f"{GATEWAY_URL}/files",
        files={"file": (filename, file_bytes, content_type)},
        data=data,
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def list_files(all_versions: bool = True) -> list:
    """GET /files — list files, optionally filtering to latest version per name."""
    resp = requests.get(
        f"{GATEWAY_URL}/files",
        params={"all_versions": str(all_versions).lower()},
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def get_file(file_id: str) -> dict:
    """GET /files/{file_id} — get file detail with chunk map."""
    resp = requests.get(f"{GATEWAY_URL}/files/{file_id}", timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def download_file(file_id: str) -> bytes:
    """GET /files/{file_id}/download — download reassembled audio bytes."""
    resp = requests.get(f"{GATEWAY_URL}/files/{file_id}/download", timeout=(5, 60))
    resp.raise_for_status()
    return resp.content


def delete_file(file_id: str) -> dict:
    """DELETE /files/{file_id} — soft-delete a file."""
    resp = requests.delete(f"{GATEWAY_URL}/files/{file_id}", timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def cluster_status() -> dict:
    """GET /cluster/status — get live node health info, replication metrics, and LB algorithm."""
    resp = requests.get(f"{GATEWAY_URL}/cluster/status", timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def get_events(limit: int = 50) -> list:
    """GET /events — get recent ring-buffer events."""
    resp = requests.get(f"{GATEWAY_URL}/events", params={"limit": limit}, timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def gateway_health() -> bool:
    """Return True if the gateway is reachable."""
    try:
        resp = requests.get(f"{GATEWAY_URL}/health", timeout=(3, 5))
        return resp.status_code == 200
    except requests.RequestException:
        return False


def fail_node(node_id: int) -> dict:
    """POST /admin/nodes/{node_id}/fail — simulated crash of a storage node."""
    resp = requests.post(f"{GATEWAY_URL}/admin/nodes/{node_id}/fail", timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def recover_node(node_id: int) -> dict:
    """POST /admin/nodes/{node_id}/recover — recover storage node from crash."""
    resp = requests.post(f"{GATEWAY_URL}/admin/nodes/{node_id}/recover", timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json()

