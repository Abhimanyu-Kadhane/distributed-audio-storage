"""Gateway SQLite schema and query helpers.

Design decisions:
- One short-lived connection per operation (no shared connection across threads).
- WAL mode + busy_timeout=5000 ms avoids writer-blocks-reader issues.
- Schema carries lamport_ts, client_id, replicas table.
"""
from contextlib import contextmanager
import sqlite3
import threading
from typing import Generator, List, Optional, Set

from app.config import DB_PATH

# Ensure only one thread initialises the schema
_init_lock = threading.Lock()


@contextmanager
def _conn() -> Generator[sqlite3.Connection, None, None]:
    """Open a fresh connection, configure it, yield, then close."""
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    con.row_factory = sqlite3.Row
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def init_db() -> None:
    """Create tables if they do not already exist."""
    with _init_lock:
        with _conn() as con:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS files (
                    file_id      TEXT PRIMARY KEY,
                    name         TEXT NOT NULL,
                    content_type TEXT,
                    size_bytes   INTEGER,
                    sha256       TEXT,
                    chunk_size   INTEGER,
                    chunk_count  INTEGER,
                    lamport_ts   INTEGER DEFAULT 0,
                    client_id    TEXT    DEFAULT 'default',
                    status       TEXT    DEFAULT 'complete',
                    created_at   TEXT
                );

                CREATE TABLE IF NOT EXISTS chunks (
                    chunk_id   TEXT PRIMARY KEY,
                    file_id    TEXT,
                    idx        INTEGER,
                    size_bytes INTEGER,
                    sha256     TEXT
                );

                -- replicas table with role ('primary' | 'backup') and state ('ok' | 'degraded')
                CREATE TABLE IF NOT EXISTS replicas (
                    chunk_id TEXT,
                    node_id  INTEGER,
                    role     TEXT DEFAULT 'primary',
                    state    TEXT DEFAULT 'ok',
                    PRIMARY KEY (chunk_id, node_id)
                );
                """
            )


# ---------------------------------------------------------------------------
# File operations
# ---------------------------------------------------------------------------

def insert_file_with_chunks(
    file_id: str,
    name: str,
    content_type: str,
    size_bytes: int,
    sha256: str,
    chunk_size: int,
    chunk_count: int,
    created_at: str,
    client_id: str,
    chunks: List[dict],
    replicas: List[dict],
    lamport_ts: int = 0,
) -> None:
    """Insert files, chunks, and replicas in a single transaction."""
    with _conn() as con:
        con.execute(
            """INSERT INTO files
               (file_id, name, content_type, size_bytes, sha256,
                chunk_size, chunk_count, lamport_ts, created_at, client_id, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'complete')""",
            (
                file_id,
                name,
                content_type,
                size_bytes,
                sha256,
                chunk_size,
                chunk_count,
                lamport_ts,
                created_at,
                client_id,
            ),
        )
        con.executemany(
            "INSERT INTO chunks (chunk_id, file_id, idx, size_bytes, sha256) VALUES (?, ?, ?, ?, ?)",
            [
                (c["chunk_id"], file_id, c["idx"], c["size_bytes"], c["sha256"])
                for c in chunks
            ],
        )
        con.executemany(
            "INSERT INTO replicas (chunk_id, node_id, role, state) VALUES (?, ?, ?, ?)",
            [(r["chunk_id"], r["node_id"], r["role"], r["state"]) for r in replicas],
        )


def list_files() -> List[sqlite3.Row]:
    """Return all non-deleted files."""
    with _conn() as con:
        return con.execute(
            "SELECT * FROM files WHERE status='complete' ORDER BY created_at DESC"
        ).fetchall()


def list_files_with_latest(all_versions: bool = True) -> List[dict]:
    """Return non-deleted files annotated with is_latest (LAB TOPIC 3: conflict resolution).
    
    The latest version is the non-deleted file with greatest (lamport_ts, client_id, file_id).
    """
    rows = list_files()
    if not rows:
        return []

    # Group by name to identify latest version per logical file
    name_groups: dict[str, list[sqlite3.Row]] = {}
    for r in rows:
        name_groups.setdefault(r["name"], []).append(r)

    latest_ids: Set[str] = set()
    for name, group in name_groups.items():
        # Compare (lamport_ts, client_id, file_id) lexicographically
        latest_row = max(
            group,
            key=lambda x: (x["lamport_ts"], str(x["client_id"]), str(x["file_id"])),
        )
        latest_ids.add(latest_row["file_id"])

    result: List[dict] = []
    for r in rows:
        is_latest = r["file_id"] in latest_ids
        if not all_versions and not is_latest:
            continue
        d = dict(r)
        d["is_latest"] = is_latest
        result.append(d)

    return result


def get_latest_version_for_name(name: str) -> Optional[dict]:
    """Return the latest version file row for a given name, if any exists."""
    with _conn() as con:
        rows = con.execute(
            "SELECT * FROM files WHERE name=? AND status='complete'",
            (name,),
        ).fetchall()
        if not rows:
            return None
        latest_row = max(
            rows,
            key=lambda x: (x["lamport_ts"], str(x["client_id"]), str(x["file_id"])),
        )
        return dict(latest_row)


def get_file(file_id: str) -> Optional[sqlite3.Row]:
    """Return the file row or None."""
    with _conn() as con:
        return con.execute(
            "SELECT * FROM files WHERE file_id=? AND status='complete'",
            (file_id,),
        ).fetchone()


def get_chunks_for_file(file_id: str) -> List[sqlite3.Row]:
    """Return chunk rows ordered by index."""
    with _conn() as con:
        return con.execute(
            "SELECT * FROM chunks WHERE file_id=? ORDER BY idx",
            (file_id,),
        ).fetchall()


def get_replicas_for_chunk(chunk_id: str) -> List[sqlite3.Row]:
    """Return replica rows for a single chunk."""
    with _conn() as con:
        return con.execute(
            "SELECT * FROM replicas WHERE chunk_id=?",
            (chunk_id,),
        ).fetchall()


def get_all_replicas_for_file(file_id: str) -> List[sqlite3.Row]:
    """Return all replica rows for a file (joined via chunks table)."""
    with _conn() as con:
        return con.execute(
            """SELECT r.chunk_id, r.node_id, r.role, r.state
               FROM replicas r
               JOIN chunks c ON c.chunk_id = r.chunk_id
               WHERE c.file_id = ?""",
            (file_id,),
        ).fetchall()


def mark_file_deleted(file_id: str) -> None:
    """Soft-delete a file record."""
    with _conn() as con:
        con.execute(
            "UPDATE files SET status='deleted' WHERE file_id=?",
            (file_id,),
        )


def get_replica_counts_per_node() -> dict[int, dict[str, int]]:
    """Return count of primary and backup chunks stored per node for active files."""
    with _conn() as con:
        rows = con.execute(
            """SELECT r.node_id, r.role, COUNT(r.chunk_id) as cnt
               FROM replicas r
               JOIN chunks c ON c.chunk_id = r.chunk_id
               JOIN files f ON f.file_id = c.file_id
               WHERE f.status = 'complete'
               GROUP BY r.node_id, r.role"""
        ).fetchall()
        counts: dict[int, dict[str, int]] = {}
        for row in rows:
            nid = row["node_id"]
            role = row["role"]
            cnt = row["cnt"]
            if nid not in counts:
                counts[nid] = {"primary": 0, "backup": 0}
            counts[nid][role] = cnt
        return counts


def count_under_replicated_chunks(alive_node_ids: Set[int], replication_factor: int) -> int:
    """Count chunks of active files whose number of alive replicas < replication_factor."""
    with _conn() as con:
        rows = con.execute(
            """SELECT c.chunk_id, r.node_id
               FROM chunks c
               JOIN files f ON f.file_id = c.file_id
               LEFT JOIN replicas r ON r.chunk_id = c.chunk_id
               WHERE f.status = 'complete'"""
        ).fetchall()
        chunk_alive_map: dict[str, int] = {}
        for row in rows:
            cid = row["chunk_id"]
            nid = row["node_id"]
            if cid not in chunk_alive_map:
                chunk_alive_map[cid] = 0
            if nid is not None and nid in alive_node_ids:
                chunk_alive_map[cid] += 1
        return sum(1 for cnt in chunk_alive_map.values() if cnt < replication_factor)


# ---------------------------------------------------------------------------
# Phase 3: Failover & Repair DB Helpers
# ---------------------------------------------------------------------------

def promote_primaries(dead_node_id: int, alive_node_ids: Set[int]) -> int:
    """LAB TOPIC 8: failover promotion of backup replicas to primary.

    For every chunk where the dead node holds role 'primary' and some other
    ALIVE node holds a replica, swap roles in one transaction (alive backup
    becomes 'primary', dead node's row becomes 'backup').
    Returns the count of promoted chunks.
    """
    if not alive_node_ids:
        return 0

    with _conn() as con:
        # Chunks for complete files where dead_node_id is currently primary
        rows = con.execute(
            """SELECT c.chunk_id
               FROM replicas r
               JOIN chunks c ON c.chunk_id = r.chunk_id
               JOIN files f ON f.file_id = c.file_id
               WHERE r.node_id = ? AND r.role = 'primary' AND f.status = 'complete'""",
            (dead_node_id,),
        ).fetchall()

        promoted = 0
        for row in rows:
            cid = row["chunk_id"]
            # Find an alive backup replica for this chunk
            placeholders = ",".join("?" for _ in alive_node_ids)
            query = f"""SELECT node_id FROM replicas
                        WHERE chunk_id = ? AND node_id IN ({placeholders}) AND node_id != ?
                        ORDER BY node_id ASC LIMIT 1"""
            params = [cid] + list(alive_node_ids) + [dead_node_id]
            backup_row = con.execute(query, params).fetchone()

            if backup_row:
                new_primary_id = backup_row["node_id"]
                # Swap roles in one transaction
                con.execute(
                    "UPDATE replicas SET role = 'backup' WHERE chunk_id = ? AND node_id = ?",
                    (cid, dead_node_id),
                )
                con.execute(
                    "UPDATE replicas SET role = 'primary' WHERE chunk_id = ? AND node_id = ?",
                    (cid, new_primary_id),
                )
                promoted += 1

        return promoted


def get_replication_report() -> List[dict]:
    """LAB TOPIC 8: Replication report for the leader's repair loop.

    Returns list of chunks {chunk_id, sha256, size, replicas:[{node_id, role}]}
    for all chunks of non-deleted files.
    """
    with _conn() as con:
        rows = con.execute(
            """SELECT c.chunk_id, c.sha256, c.size_bytes, r.node_id, r.role
               FROM chunks c
               JOIN files f ON f.file_id = c.file_id
               LEFT JOIN replicas r ON r.chunk_id = c.chunk_id
               WHERE f.status = 'complete'
               ORDER BY c.chunk_id, c.idx"""
        ).fetchall()

        chunks_map: dict[str, dict] = {}
        for r in rows:
            cid = r["chunk_id"]
            if cid not in chunks_map:
                chunks_map[cid] = {
                    "chunk_id": cid,
                    "sha256": r["sha256"],
                    "size": r["size_bytes"],
                    "replicas": [],
                }
            if r["node_id"] is not None:
                chunks_map[cid]["replicas"].append({
                    "node_id": r["node_id"],
                    "role": r["role"],
                })

        return list(chunks_map.values())


def upsert_replica(chunk_id: str, node_id: int, role: str = "backup", state: str = "ok") -> None:
    """LAB TOPIC 8: Insert or update a replica record."""
    with _conn() as con:
        con.execute(
            """INSERT INTO replicas (chunk_id, node_id, role, state)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(chunk_id, node_id) DO UPDATE SET
                   role = excluded.role,
                   state = excluded.state""",
            (chunk_id, node_id, role, state),
        )

