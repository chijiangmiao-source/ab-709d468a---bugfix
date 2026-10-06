import hashlib
import os
import tempfile
import threading
import time
import unittest

from app import artifacts, config, store, worker
from app.render import render_artifact_bytes


class WorkerTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        os.environ["DATA_DIR"] = self._tmp.name
        os.environ["LEASE_TTL_SECONDS"] = "5"
        config.ensure_dirs()
        self.conn = store.connect()
        store.init_db(self.conn)

    def tearDown(self):
        self.conn.close()
        self._tmp.cleanup()
        for var in ("DATA_DIR", "LEASE_TTL_SECONDS"):
            os.environ.pop(var, None)


class ProcessTest(WorkerTestBase):
    def test_process_publishes_verified_artifact(self):
        store.submit_export(self.conn, "E-1", [{"ts": "t0", "lat": 31.2, "depth_m": 10}])
        fencing = store.acquire_lease(self.conn, worker.lease_resource("E-1"), "w-test", 5)
        result = worker.process_export(self.conn, "E-1", "w-test", fencing)
        self.assertEqual("published", result)
        row = store.get_export(self.conn, "E-1")
        self.assertEqual("PUBLISHED", row["stage"])
        data = artifacts.load_verified(row)  # digest + identity verified
        self.assertEqual(hashlib.sha256(data).hexdigest(), row["artifact_digest"])
        self.assertEqual([], artifacts.tmp_files_for("E-1"))

    def test_same_rules_distinct_exports_keep_distinct_identities(self):
        """The original defect: a second export under the same rules snapshot
        must never freeze the first export's bytes/digest/identity."""
        recs1 = [{"ts": "t0", "lat": 31.2, "depth_m": 10, "vessel_id": "V1"}]
        recs2 = [{"ts": "t1", "lat": 59.3, "depth_m": 22, "vessel_id": "V2"}]
        _, rcpt1 = store.submit_export(self.conn, "E-1", recs1)
        _, rcpt2 = store.submit_export(self.conn, "E-2", recs2)
        self.assertEqual(rcpt1["rules_digest"], rcpt2["rules_digest"])
        self.assertNotEqual(rcpt1["input_digest"], rcpt2["input_digest"])
        self.assertNotEqual(rcpt1["receipt_id"], rcpt2["receipt_id"])

        for eid in ("E-1", "E-2"):
            fencing = store.acquire_lease(self.conn, worker.lease_resource(eid), "w-test", 5)
            self.assertEqual("published", worker.process_export(self.conn, eid, "w-test", fencing))
            store.release_lease(self.conn, worker.lease_resource(eid), "w-test", fencing)

        row1 = store.get_export(self.conn, "E-1")
        row2 = store.get_export(self.conn, "E-2")
        data1 = artifacts.load_verified(row1)
        data2 = artifacts.load_verified(row2)
        self.assertNotEqual(data1, data2)
        self.assertNotEqual(row1["artifact_digest"], row2["artifact_digest"])
        import json as _json
        doc1, doc2 = _json.loads(data1), _json.loads(data2)
        self.assertEqual(doc1["export_id"], "E-1")
        self.assertEqual(doc2["export_id"], "E-2")
        self.assertEqual(doc1["receipt_id"], row1["receipt_id"])
        self.assertEqual(doc2["receipt_id"], row2["receipt_id"])
        self.assertEqual(doc1["input_digest"], row1["input_digest"])
        self.assertEqual(doc2["input_digest"], row2["input_digest"])
        self.assertEqual(doc1["rules_digest"], doc2["rules_digest"])
        self.assertEqual(doc1["records"][0]["ts"], "t0")
        self.assertEqual(doc2["records"][0]["ts"], "t1")

    def test_foreign_bytes_are_refused_even_with_matching_recorded_digest(self):
        store.submit_export(self.conn, "E-1", [{"ts": "t0", "lat": 31.2}])
        store.submit_export(self.conn, "E-2", [{"ts": "t9", "lat": 88.8}])
        f1 = store.acquire_lease(self.conn, worker.lease_resource("E-1"), "w", 5)
        worker.process_export(self.conn, "E-1", "w", f1)
        store.release_lease(self.conn, worker.lease_resource("E-1"), "w", f1)
        row1 = store.get_export(self.conn, "E-1")
        foreign = artifacts.load_verified(row1)
        # Simulate the historical mispublication: E-2 recorded with E-1's bytes.
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-2", "PROCESSING", ("RECEIVED",))
            store.mark_published(
                self.conn, "E-2", row1["artifact_digest"],
                artifacts.published_path("E-2"), "w-bug", "unit",
            )
        os.makedirs(os.path.dirname(artifacts.published_path("E-2")), exist_ok=True)
        with open(artifacts.published_path("E-2"), "wb") as fh:
            fh.write(foreign)
        row2 = store.get_export(self.conn, "E-2")
        with self.assertRaises(artifacts.IdentityMismatch):
            artifacts.load_verified(row2)

    def test_two_workers_publish_exactly_once(self):
        """Two racing worker loops: one export, one published artifact, no regression."""
        store.submit_export(self.conn, "E-1", [{"ts": "t0", "lat": 31.2, "depth_m": 10}])
        stop = threading.Event()

        def loop(name):
            conn = store.connect()
            try:
                while not stop.is_set():
                    try:
                        worker.tick(conn, name)
                    except Exception:
                        conn.rollback()
                    time.sleep(0.02)
            finally:
                conn.close()

        threads = [threading.Thread(target=loop, args=("w-%d" % i,)) for i in range(2)]
        for t in threads:
            t.start()
        deadline = time.time() + 15
        while time.time() < deadline:
            row = store.get_export(self.conn, "E-1")
            if row["stage"] == "PUBLISHED":
                break
            time.sleep(0.05)
        time.sleep(0.5)  # give the loser a chance to misbehave
        stop.set()
        for t in threads:
            t.join()

        row = store.get_export(self.conn, "E-1")
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(1, len(store.published_artifacts(self.conn, "E-1")))
        self.assertEqual(1, len(artifacts.list_published_files()))
        expected = hashlib.sha256(render_artifact_bytes(row)).hexdigest()
        self.assertEqual(expected, row["artifact_digest"])

    def test_tick_recovers_crashed_export_after_lease_expiry(self):
        """Simulate a crashed worker: staged artifact + expired lease -> tick converges."""
        os.environ["LEASE_TTL_SECONDS"] = "0.05"
        store.submit_export(self.conn, "E-1", [{"ts": "t0", "lat": 31.2, "depth_m": 10}])
        row = store.get_export(self.conn, "E-1")
        data = render_artifact_bytes(row)
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path("E-1", "dead")
        artifacts.write_tmp(tmp, data)
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
            store.record_artifact(self.conn, "E-1", "staged", tmp, digest)
            store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",))
        # dead worker's lease, already expired
        store.acquire_lease(self.conn, worker.lease_resource("E-1"), "w-dead", 0.01)
        time.sleep(0.06)

        worker.tick(self.conn, "w-alive")

        row = store.get_export(self.conn, "E-1")
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(digest, row["artifact_digest"])
        events = [e["event"] for e in store.export_events(self.conn, "E-1")]
        self.assertIn("published", events)


if __name__ == "__main__":
    unittest.main()
