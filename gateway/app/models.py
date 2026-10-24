"""Pydantic response models for the gateway public API."""
from typing import List, Optional
from pydantic import BaseModel


class ReplicaNodeInfo(BaseModel):
    node_id: int
    role: str  # 'primary' | 'backup'


class ChunkInfo(BaseModel):
    idx: int
    chunk_id: str
    size: int
    nodes: List[ReplicaNodeInfo]  # [{"node_id": int, "role": str}]


class FileRecord(BaseModel):
    file_id: str
    name: str
    content_type: str
    size_bytes: int
    sha256: str
    chunk_size: int
    chunk_count: int
    created_at: str
    client_id: str
    status: str
    lamport_ts: int = 0
    gateway_lamport: Optional[int] = None
    is_latest: Optional[bool] = None
    chunk_map: Optional[List[ChunkInfo]] = None
    degraded: Optional[bool] = False


class FileListItem(BaseModel):
    file_id: str
    name: str
    content_type: str
    size_bytes: int
    chunk_count: int
    created_at: str
    client_id: str
    lamport_ts: int
    is_latest: bool


class NodeStatus(BaseModel):
    node_id: int
    url: str
    alive: bool
    chunk_count: int
    bytes_used: int
    active_requests: int
    primary_chunks: int = 0
    backup_chunks: int = 0
    is_leader: bool = False


class ClusterStatus(BaseModel):
    nodes: List[NodeStatus]
    replication_factor: int
    chunk_size_bytes: int
    lb_algorithm: str
    under_replicated_chunks: int = 0
    leader_id: Optional[int] = None


class EventRecord(BaseModel):
    seq: int
    wall_time: str
    lamport: int
    actor: str
    type: str
    detail: str
