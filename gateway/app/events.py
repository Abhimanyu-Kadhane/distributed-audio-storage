"""Thread-safe event log ring buffer for the gateway.

Keeps up to 200 recent events in memory:
{seq, wall_time, lamport, actor, type, detail}
"""
from collections import deque
from datetime import datetime, timezone
import threading
from typing import List, Optional


_lock = threading.Lock()
_events: deque = deque(maxlen=200)
_seq: int = 0


def record_event(
    actor: str,
    event_type: str,
    detail: str,
    lamport: int,
    wall_time: Optional[str] = None,
) -> dict:
    """Record an event into the ring buffer.
    
    Returns the recorded event dictionary.
    """
    global _seq
    if wall_time is None:
        wall_time = datetime.now(timezone.utc).isoformat()
    
    with _lock:
        _seq += 1
        item = {
            "seq": _seq,
            "wall_time": wall_time,
            "lamport": lamport,
            "actor": actor,
            "type": event_type,
            "detail": detail,
        }
        _events.append(item)
        return dict(item)


def get_events(limit: int = 50) -> List[dict]:
    """Return the newest events first, capped by limit."""
    with _lock:
        # _events has oldest at index 0 and newest at index -1
        # Reversing gives newest first
        items = list(reversed(_events))
        return [dict(e) for e in items[:limit]]


def clear_events() -> None:
    """Clear all events (used in tests if needed)."""
    global _seq
    with _lock:
        _events.clear()
        _seq = 0
