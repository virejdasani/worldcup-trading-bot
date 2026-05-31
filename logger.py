"""Shared logger — stores log entries and broadcasts to WebSocket clients."""
import asyncio
from datetime import datetime
from typing import Callable

_entries: list[dict] = []
_listeners: list[Callable] = []


def log(source: str, message: str):
    entry = {
        "ts": datetime.utcnow().strftime("%H:%M:%S"),
        "source": source,
        "message": message,
    }
    _entries.append(entry)
    if len(_entries) > 500:
        _entries.pop(0)
    for cb in list(_listeners):
        try:
            asyncio.get_event_loop().call_soon_threadsafe(cb, entry)
        except Exception:
            pass


def get_logs() -> list[dict]:
    return list(_entries)


def add_listener(cb: Callable):
    _listeners.append(cb)


def remove_listener(cb: Callable):
    _listeners.discard(cb) if hasattr(_listeners, "discard") else None
    if cb in _listeners:
        _listeners.remove(cb)
