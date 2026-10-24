"""Runtime in-memory state for the storage node.

LAB TOPIC 8: Simulated crash failure controls
The failed state is strictly in-memory. While failed is True, every endpoint
except /admin/recover and /admin/status returns 503, and all background threads
(election, heartbeat, failure detector, repair) skip their work.
"""
import threading

_fail_lock = threading.Lock()
_failed: bool = False


def is_failed() -> bool:
    """Return True if the node is currently in a simulated crash state."""
    with _fail_lock:
        return _failed


def set_failed(val: bool) -> None:
    """Set the simulated crash state."""
    global _failed
    with _fail_lock:
        _failed = val
