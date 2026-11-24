# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Buffer opt-in stream measurements; flush only at the existing close path.

Clones record host latency only. Span and OpenMP worker CPU remain separate.
Enabled libgomp diagnostics are flushed from
Torch's actual library here too, before process termination can discard them.
"""

import itertools
import json
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path


_enabled = os.environ.get("RUNAI_CPU_DIAGNOSTICS") == "1"
_lock = threading.Lock()
_sequence = itertools.count()
_pending = []
_totals = {}
_capacity = 65536


def _after_fork():
    global _lock, _sequence
    _lock = threading.Lock()
    _sequence = itertools.count()
    _pending.clear()
    _totals.clear()


os.register_at_fork(after_in_child=_after_fork)


def scheduler_gate():
    """Wait before model streaming, never from a streamer-method wrapper."""
    if os.environ.get("RUNAI_CPU_DIAGNOSTICS_SCHEDULER_GATE") != "1":
        return
    if not _enabled:
        raise ValueError("Scheduler gate requires enabled CPU diagnostics")
    token = os.environ.get("RUNAI_CPU_DIAGNOSTICS_SCHEDULER_TOKEN", "")
    if not 16 <= len(token) <= 128 or not all(c in "0123456789abcdef" for c in token):
        raise ValueError("Scheduler gate requires a hexadecimal ownership token")
    root = Path("/tmp/runai-cpu-scheduler")
    try:
        root.mkdir(mode=0o700)
    except FileExistsError:
        waiting = json.loads((root / "waiting.json").read_text())
        if waiting.get("token") != token or not _gate_ancestor(waiting.get("pid")):
            raise ValueError("Scheduler gate is not owned by this process tree")
    else:
        with (root / "waiting.tmp").open("x") as output:
            json.dump({"pid": os.getpid(), "token": token}, output)
        os.replace(root / "waiting.tmp", root / "waiting.json")
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        ready = root / "ready.json"
        if ready.is_file():
            proof = json.loads(ready.read_text())
            if not _gate_ancestor(proof.get("pid")) or proof.get("token") != token:
                raise ValueError("Scheduler gate ownership mismatch")
            if proof.get("version") != 1 or proof.get("links", 0) < 11:
                raise ValueError("Scheduler collector did not attach all required probes")
            return
        time.sleep(0.05)
    raise TimeoutError("Scheduler collector did not become ready before streaming")


def _gate_ancestor(owner):
    """Spawned children inherit an already-attached process-tree trace."""
    pid, seen = os.getpid(), set()
    while pid and pid not in seen:
        if pid == owner:
            return True
        seen.add(pid)
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("PPid:"):
                pid = int(line.split()[1])
                break
        else:
            raise ValueError("Missing process ancestry for scheduler gate")
    return False


def boundary(event):
    if _enabled:
        _record({
            "event": event, "pid": os.getpid(), "tid": threading.get_native_id(),
            "monotonic_ns": time.monotonic_ns(),
        })


@contextmanager
def span(kind):
    if not _enabled:
        yield {}
        return
    record = {
        "event": "span", "kind": kind, "pid": os.getpid(),
        "tid": threading.get_native_id(), "start_ns": time.monotonic_ns(),
    }
    cpu = time.clock_gettime_ns(time.CLOCK_THREAD_CPUTIME_ID)
    success = False
    try:
        yield record
        success = True
    finally:
        record["caller_cpu_ns"] = time.clock_gettime_ns(time.CLOCK_THREAD_CPUTIME_ID) - cpu
        record["end_ns"] = time.monotonic_ns()
        record["success"] = success
        _record(record)


def _emit(records):
    for record in records:
        payload = (
            "RUNAI_CPU_DIAGNOSTIC " + json.dumps(record, separators=(",", ":")) + "\n"
        ).encode()
        if os.write(2, payload) != len(payload):
            raise OSError("Incomplete RunAI CPU diagnostic write")


def _record(record):
    with _lock:
        if len(_pending) >= _capacity:
            raise BufferError("RunAI CPU diagnostic capacity exceeded; flush required")
        _pending.append(record)


def ownership(device, owned, distributed):
    if _enabled:
        _record(
            {
                "event": "ownership",
                "pid": os.getpid(),
                "tid": threading.get_native_id(),
                "monotonic_ns": time.monotonic_ns(),
                "device": str(device),
                "requested_owned": bool(owned),
                "distributed": bool(distributed),
            }
        )


def clone(buffer, kind):
    if not _enabled:
        return buffer.clone()
    operation = next(_sequence)
    size = buffer.numel() * buffer.element_size()
    device = str(buffer.device)
    tid = threading.get_native_id()
    start = time.monotonic_ns()
    success = False
    try:
        result = buffer.clone()
        success = True
        return result
    finally:
        end = time.monotonic_ns()
        with _lock:
            total = _totals.setdefault(
                kind, {"calls": 0, "bytes": 0, "wall_ns": 0, "failures": 0}
            )
            total["calls"] += 1
            total["bytes"] += size
            total["wall_ns"] += end - start
            total["failures"] += not success
        _record(
            {
                "event": "clone",
                "pid": os.getpid(),
                "tid": tid,
                "operation": operation,
                "kind": kind,
                "device": device,
                "bytes": size,
                "start_ns": start,
                "end_ns": end,
                "success": success,
            }
        )


def flush():
    if not _enabled:
        return
    with _lock:
        _emit(_pending)
        _pending.clear()
        _emit(
            [
                {
                    "event": "summary",
                    "pid": os.getpid(),
                    "tid": threading.get_native_id(),
                    "monotonic_ns": time.monotonic_ns(),
                    "totals": _totals,
                    "clone_cpu_collected": False,
                }
            ]
        )
    if os.environ.get("LIBGOMP_CPU_DIAGNOSTICS") == "1":
        import ctypes

        import torch

        library = ctypes.CDLL(
            os.path.join(os.path.dirname(torch.__file__), "lib", "libgomp.so.1")
        )
        dump = library.GOMP_cpu_diagnostics_flush
        dump.argtypes = []
        dump.restype = None
        dump()


scheduler_gate()
