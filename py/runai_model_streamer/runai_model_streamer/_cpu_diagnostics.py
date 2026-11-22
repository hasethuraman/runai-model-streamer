# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Opt-in caller CPU measurements; OpenMP worker CPU is measured separately."""

import itertools
import json
import os
import threading
import time


_enabled = os.environ.get("RUNAI_CPU_DIAGNOSTICS") == "1"
_lock = threading.Lock()
_sequence = itertools.count()
_pending = []
_totals = {}


def _after_fork():
    global _lock, _sequence
    _lock = threading.Lock()
    _sequence = itertools.count()
    _pending.clear()
    _totals.clear()


os.register_at_fork(after_in_child=_after_fork)


def _emit(records):
    for record in records:
        payload = (
            "RUNAI_CPU_DIAGNOSTIC " + json.dumps(record, separators=(",", ":")) + "\n"
        ).encode()
        if os.write(2, payload) != len(payload):
            raise OSError("Incomplete RunAI CPU diagnostic write")


def _record(record):
    with _lock:
        _pending.append(record)
        if len(_pending) >= 64:
            _emit(_pending)
            _pending.clear()


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
    cpu_start = time.clock_gettime_ns(time.CLOCK_THREAD_CPUTIME_ID)
    success = False
    try:
        result = buffer.clone()
        success = True
        return result
    finally:
        cpu = time.clock_gettime_ns(time.CLOCK_THREAD_CPUTIME_ID) - cpu_start
        end = time.monotonic_ns()
        with _lock:
            total = _totals.setdefault(
                kind, {"calls": 0, "bytes": 0, "cpu_ns": 0, "wall_ns": 0, "failures": 0}
            )
            total["calls"] += 1
            total["bytes"] += size
            total["cpu_ns"] += cpu
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
                "caller_cpu_ns": cpu,
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
                    "cpu_scope": "calling-thread-only",
                    "openmp_worker_cpu_included": False,
                }
            ]
        )
