import json
import os
import tempfile
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

if os.name == "nt":
    import msvcrt
else:
    import fcntl


_thread_locks: dict[str, threading.RLock] = {}
_thread_locks_guard = threading.Lock()


def _read_state(path: str) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return {}
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError("O estado local tem de ser um objeto JSON.")
    return data


@contextmanager
def _state_lock(path: str) -> Iterator[str]:
    # Keep a stable lock file: replacing or deleting it could let two processes
    # acquire locks on different inodes for the same state file.
    resolved = os.path.realpath(os.path.abspath(os.fspath(path)))
    with _thread_locks_guard:
        thread_lock = _thread_locks.setdefault(resolved, threading.RLock())
    with thread_lock:
        os.makedirs(os.path.dirname(resolved), exist_ok=True)
        with open(resolved + ".lock", "a+b") as lock_file:
            if os.name == "nt":
                if os.fstat(lock_file.fileno()).st_size == 0:
                    lock_file.write(b"0")
                    lock_file.flush()
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
            else:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield resolved
            finally:
                if os.name == "nt":
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _write_state(path: str, data: dict[str, Any]) -> None:
    if not isinstance(data, dict):
        raise TypeError("O estado local tem de ser um dicionário.")
    new_payload = json.dumps(data, ensure_ascii=False, indent=2)
    try:
        with open(path, "r", encoding="utf-8") as f:
            if f.read() == new_payload:
                return
    except FileNotFoundError:
        pass

    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=os.path.dirname(path),
            prefix=os.path.basename(path) + ".", suffix=".tmp", delete=False,
        ) as f:
            temporary_path = f.name
            f.write(new_payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None:
            try:
                os.unlink(temporary_path)
            except FileNotFoundError:
                pass


def load_local_state(path: str) -> dict[str, Any]:
    try:
        return _read_state(path)
    except Exception:
        return {}


def save_local_state(path: str, data: dict[str, Any]) -> None:
    """Atomically replace the state; use update_local_state for partial updates."""
    with _state_lock(path) as resolved:
        _write_state(resolved, data)


def update_local_state(
    path: str, updater: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    """Read, update and save under one lock shared by threads and processes.

    The updater must return the complete updated dictionary. Unlike the
    best-effort loader, this operation raises if existing JSON is corrupt, so a
    partial update cannot silently replace saved data with an empty state.
    """
    with _state_lock(path) as resolved:
        updated = updater(_read_state(resolved))
        _write_state(resolved, updated)
        return updated
