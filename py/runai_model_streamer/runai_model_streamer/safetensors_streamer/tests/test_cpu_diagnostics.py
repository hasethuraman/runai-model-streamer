# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import importlib.util
import json
import os
import sys
import types
import unittest
from pathlib import Path
from unittest import mock


def load_diagnostics(enabled):
    path = Path(__file__).resolve().parents[2] / "_cpu_diagnostics.py"
    spec = importlib.util.spec_from_file_location("cpu_diagnostics_test", path)
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(os.environ, {"RUNAI_CPU_DIAGNOSTICS": str(int(enabled))}):
        spec.loader.exec_module(module)
    return module


class Buffer:
    device = "cpu"

    def __init__(self, values):
        self.values = values
        self.clones = 0

    def numel(self):
        return len(self.values)

    def element_size(self):
        return 4

    def clone(self):
        self.clones += 1
        return Buffer(self.values.copy())


class CpuDiagnosticsTests(unittest.TestCase):
    def test_disabled_preserves_one_clone_and_retained_ownership(self):
        module = load_diagnostics(False)
        source = Buffer([1, 2])
        with mock.patch.object(module.os, "write") as write:
            owned = module.clone(source, "distributed_owned")
            source.values[0] = 9
            module.flush()
        self.assertEqual(source.clones, 1)
        self.assertEqual(owned.values, [1, 2])
        write.assert_not_called()

    def test_enabled_preserves_clone_and_separates_caller_cpu(self):
        module = load_diagnostics(True)
        records = []

        def write(_descriptor, payload):
            records.append(json.loads(payload.decode().split(" ", 1)[1]))
            return len(payload)

        source = Buffer([1, 2])
        with mock.patch.object(module.os, "write", side_effect=write):
            module.ownership("cpu", True, False)
            owned = module.clone(source, "alignment")
            source.values[0] = 9
            module.flush()
        self.assertEqual(owned.values, [1, 2])
        self.assertEqual(source.clones, 1)
        self.assertEqual(
            [item["event"] for item in records], ["ownership", "clone", "summary"]
        )
        clone = records[1]
        self.assertEqual(clone["bytes"], 8)
        self.assertTrue(clone["success"])
        self.assertGreaterEqual(clone["caller_cpu_ns"], 0)
        self.assertGreater(clone["end_ns"], clone["start_ns"])
        self.assertLessEqual(
            clone["caller_cpu_ns"], clone["end_ns"] - clone["start_ns"]
        )
        self.assertFalse(records[2]["openmp_worker_cpu_included"])

    def test_failed_clone_propagates_and_is_not_reported_as_success(self):
        module = load_diagnostics(True)
        source = Buffer([1])
        with mock.patch.object(source, "clone", side_effect=ValueError("clone failed")):
            with self.assertRaisesRegex(ValueError, "clone failed"):
                module.clone(source, "distributed_owned")
        self.assertFalse(module._pending[0]["success"])
        self.assertEqual(module._totals["distributed_owned"]["failures"], 1)

    def test_diagnostic_delivery_failure_is_explicit(self):
        module = load_diagnostics(True)
        module.ownership("cpu", True, False)
        with mock.patch.object(module.os, "write", return_value=0):
            with self.assertRaisesRegex(OSError, "Incomplete"):
                module.flush()

    def test_fork_reset_does_not_reemit_parent_cpu(self):
        module = load_diagnostics(True)
        module.clone(Buffer([1]), "alignment")
        module._after_fork()
        self.assertEqual(module._pending, [])
        self.assertEqual(module._totals, {})
        self.assertEqual(next(module._sequence), 0)

    def test_more_than_64_operations_do_not_format_or_write_until_flush(self):
        module = load_diagnostics(True)
        source = Buffer([1])
        with (
            mock.patch.object(module.os, "write") as write,
            mock.patch.object(module.json, "dumps") as dumps,
        ):
            for _ in range(100):
                module.clone(source, "alignment")
            write.assert_not_called()
            dumps.assert_not_called()
        self.assertEqual(source.clones, 100)
        self.assertEqual(len(module._pending), 100)
        with mock.patch.object(module.os, "write", side_effect=lambda _, p: len(p)):
            module.flush()
        self.assertEqual(module._pending, [])

    def test_capacity_failure_does_not_silently_discard_records(self):
        module = load_diagnostics(True)
        module._capacity = 1
        module.ownership("cpu", True, False)
        with self.assertRaisesRegex(BufferError, "capacity exceeded"):
            module.ownership("cpu", True, False)
        self.assertEqual(len(module._pending), 1)

    def test_lifecycle_flush_dumps_the_actual_torch_libgomp(self):
        module = load_diagnostics(True)
        torch = types.SimpleNamespace(__file__="/runtime/torch/__init__.py")
        with (
            mock.patch.dict(os.environ, {"LIBGOMP_CPU_DIAGNOSTICS": "1"}),
            mock.patch.dict(sys.modules, {"torch": torch}),
            mock.patch("ctypes.CDLL") as load,
            mock.patch.object(module.os, "write", side_effect=lambda _, p: len(p)),
        ):
            module.flush()
        load.assert_called_once_with("/runtime/torch/lib/libgomp.so.1")
        load.return_value.GOMP_cpu_diagnostics_flush.assert_called_once_with()
        self.assertEqual(load.return_value.GOMP_cpu_diagnostics_flush.argtypes, [])
        self.assertIsNone(load.return_value.GOMP_cpu_diagnostics_flush.restype)


if __name__ == "__main__":
    unittest.main()
