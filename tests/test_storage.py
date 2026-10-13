import json
import multiprocessing
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from organizer.storage_local import load_local_state, save_local_state, update_local_state


def _update_in_process(path, key, start, iterations):
    start.wait(10)
    for _ in range(iterations):
        def increment(state):
            previous = state.get(key, 0)
            time.sleep(0.002)
            return {**state, key: previous + 1}

        update_local_state(path, increment)


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="organizer-storage-")
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "nested" / "state.json"

    def test_updates_preserve_independent_changes_across_threads(self):
        workers, iterations = 4, 10
        start = threading.Barrier(workers)

        def worker(number):
            start.wait(5)
            for _ in range(iterations):
                def increment(state):
                    previous = state.get(str(number), 0)
                    time.sleep(0.001)
                    return {**state, str(number): previous + 1}

                update_local_state(self.path, increment)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            list(executor.map(worker, range(workers)))

        self.assertEqual(load_local_state(self.path), {str(n): iterations for n in range(workers)})

    def test_updates_preserve_independent_changes_across_processes(self):
        context = multiprocessing.get_context("spawn")
        start = context.Event()
        processes = [
            context.Process(target=_update_in_process, args=(str(self.path), str(n), start, 10))
            for n in range(3)
        ]
        try:
            for process in processes:
                process.start()
            start.set()
            for process in processes:
                process.join(20)
                self.assertEqual(process.exitcode, 0)
        finally:
            for process in processes:
                if process.is_alive():
                    process.terminate()
                    process.join(5)

        self.assertEqual(load_local_state(self.path), {str(n): 10 for n in range(3)})

    def test_concurrent_replacements_are_complete_and_leave_no_temporaries(self):
        start = threading.Barrier(4)

        def writer(number):
            start.wait(5)
            for sequence in range(15):
                save_local_state(self.path, {"writer": number, "sequence": sequence, "text": "á" * 1000})
                with self.path.open(encoding="utf-8") as saved:
                    current = json.load(saved)
                self.assertEqual(current["text"], "á" * 1000)

        with ThreadPoolExecutor(max_workers=4) as executor:
            list(executor.map(writer, range(4)))

        self.assertEqual(list(self.path.parent.glob("*.tmp")), [])

    def test_corrupt_state_is_not_overwritten_by_partial_update(self):
        self.path.parent.mkdir()
        self.path.write_text('{"saved":', encoding="utf-8")
        original = self.path.read_bytes()

        with self.assertRaises(json.JSONDecodeError):
            update_local_state(self.path, lambda state: {**state, "new": True})

        self.assertEqual(self.path.read_bytes(), original)
        self.assertEqual(load_local_state(self.path), {})

    def test_failed_callback_and_invalid_return_leave_saved_state_intact(self):
        save_local_state(self.path, {"saved": "original"})

        def fail(state):
            state["saved"] = "changed"
            raise RuntimeError("update failed")

        with self.assertRaises(RuntimeError):
            update_local_state(self.path, fail)
        with self.assertRaises(TypeError):
            update_local_state(self.path, lambda state: None)

        self.assertEqual(load_local_state(self.path), {"saved": "original"})
        updated = update_local_state(self.path, lambda state: {**state, "new": True})
        self.assertEqual(updated, {"saved": "original", "new": True})

    def test_saving_identical_content_does_not_replace_file(self):
        save_local_state(self.path, {"value": "José"})
        before = self.path.stat()
        save_local_state(self.path, {"value": "José"})
        after = self.path.stat()

        self.assertEqual(before.st_ino, after.st_ino)
        self.assertEqual(before.st_mtime_ns, after.st_mtime_ns)


if __name__ == "__main__":
    unittest.main()
