"""Thread-safe Lamport logical clock.

LAB TOPIC 3: Lamport logical clocks
Provides tick, update, and value operations protected by a threading.Lock.
"""
import threading


class LamportClock:
    """Thread-safe Lamport logical clock."""

    def __init__(self, initial: int = 0) -> None:
        self._lock = threading.Lock()
        self._time: int = initial

    def tick(self) -> int:
        """Increment time by 1 for an internal or send event and return new value."""
        with self._lock:
            self._time += 1
            return self._time

    def update(self, received: int) -> int:
        """Update clock on receiving a message: time = max(time, received) + 1."""
        with self._lock:
            self._time = max(self._time, received) + 1
            return self._time

    def value(self) -> int:
        """Return the current clock value."""
        with self._lock:
            return self._time
