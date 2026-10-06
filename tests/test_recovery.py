import hashlib
import os
import tempfile
import time
import unittest

from app import artifacts, config, recovery, store, worker
from app.render import render_artifact_bytes


class RecoveryTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        os.environ["DATA_DIR"] = self._tmp.name
        config.ensure_dirs()
        self.conn = store.connect()
        store.init_db(self.conn)
        store.submit_export(self.conn, "E-1", [{"ts": "t0", "lat": 31.2, "depth_m": 10}])

    def tearDown(self):
        self.conn.close()
        self._tmp.cleanup()
        os.environ.pop("DATA_DIR", None)

    def export(self):
        return store.get_export(self.conn, "E-1")

    def expected_digest(self):
        return hashlib.sha256(render_artifact_bytes(self.export())).hexdigest()


class ConvergeTest(RecoveryTestBase):
    def test_complete_staged_artifact_is_published_as_is(self):
        """Crash after staging: recovery converges to the same complete artifact."""
        data = render_artifact_bytes(self.export())
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path("E-1", "deadbeef")
        artifacts.write_tmp(tmp, data)
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
            store.record_artifact(self.conn, "E-1", "staged", tmp, digest)
            store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",))

        result = recovery.recover_export(self.conn, "E-1", "test")

        self.assertEqual("converged", result)
        row = self.export()
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(digest, row["artifact_digest"])
        # the very same bytes were published, not a regenerated copy
        with open(artifacts.published_path("E-1"), "rb") as fh:
            self.assertEqual(data, fh.read())
        self.assertEqual([], artifacts.tmp_files_for("E-1"))

    def test_published_file_present_but_db_not_updated(self):
        """Crash between atomic link and DB update: converge bookkeeping."""
        data = render_artifact_bytes(self.export())
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path("E-1", "cafe")
        artifacts.write_tmp(tmp, data)
        artifacts.publish(tmp, artifacts.published_path("E-1"), digest)
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
            store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",))

        result = recovery.recover_export(self.conn, "E-1", "test")

        self.assertEqual("converged", result)
        row = self.export()
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(digest, row["artifact_digest"])
        self.assertEqual(1, len(store.published_artifacts(self.conn, "E-1")))


class CleanupTest(RecoveryTestBase):
    def test_partial_write_is_cleaned_and_requeued(self):
        """Crash mid-write: partial temp file removed, export back to RECEIVED."""
        data = render_artifact_bytes(self.export())
        tmp = artifacts.tmp_path("E-1", "half")
        artifacts.write_tmp(tmp, data[: len(data) // 2])
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))

        result = recovery.recover_export(self.conn, "E-1", "test")

        self.assertEqual("requeued", result)
        row = self.export()
        self.assertEqual("RECEIVED", row["stage"])
        self.assertEqual(1, row["attempts"])
        self.assertEqual([], artifacts.tmp_files_for("E-1"))

    def test_corrupted_staged_artifact_is_aborted(self):
        data = render_artifact_bytes(self.export())
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path("E-1", "badc0de")
        artifacts.write_tmp(tmp, data + b"corruption")
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
            store.record_artifact(self.conn, "E-1", "staged", tmp, digest)
            store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",))

        result = recovery.recover_export(self.conn, "E-1", "test")

        self.assertEqual("requeued", result)
        self.assertEqual([], artifacts.tmp_files_for("E-1"))
        self.assertEqual([], store.staged_artifacts(self.conn, "E-1"))
        self.assertEqual("RECEIVED", self.export()["stage"])

    def test_published_export_is_never_touched(self):
        with store.immediate(self.conn):
            store.mark_published(self.conn, "E-1", "d" * 64, "/nowhere", "test", "unit")
        self.assertEqual("none", recovery.recover_export(self.conn, "E-1", "test"))
        self.assertEqual("PUBLISHED", self.export()["stage"])
        self.assertEqual("d" * 64, self.export()["artifact_digest"])

    def test_orphan_sweep_removes_unreferenced_old_temp_files(self):
        orphan = artifacts.tmp_path("E-9", "orphan")
        artifacts.write_tmp(orphan, b"leftover")
        old = 1_600_000_000
        os.utime(orphan, (old, old))
        removed = recovery.sweep_orphans(self.conn, "test", older_than_seconds=1)
        self.assertIn(orphan, removed)
        self.assertFalse(os.path.exists(orphan))


class ReconcilePublishedTest(unittest.TestCase):
    """Already-PUBLISHED rows whose artifact is wrong must safely converge."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        os.environ["DATA_DIR"] = self._tmp.name
        os.environ["LEASE_TTL_SECONDS"] = "5"
        config.ensure_dirs()
        self.conn = store.connect()
        store.init_db(conn=self.conn)
        store.submit_export(self.conn, "E-1", [{"ts": "t0", "lat": 31.2, "depth_m": 10}])
        store.submit_export(self.conn, "E-2", [{"ts": "t9", "lat": 59.3, "depth_m": 22}])

    def tearDown(self):
        self.conn.close()
        self._tmp.cleanup()
        for var in ("DATA_DIR", "LEASE_TTL_SECONDS"):
            os.environ.pop(var, None)

    def _publish_healthy(self, eid):
        row = store.get_export(self.conn, eid)
        data = render_artifact_bytes(row)
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path(eid, "ok")
        artifacts.write_tmp(tmp, data)
        artifacts.publish(tmp, artifacts.published_path(eid), digest)
        with store.immediate(self.conn):
            store.record_artifact(self.conn, eid, "published", artifacts.published_path(eid), digest)
            store.mark_published(self.conn, eid, digest, artifacts.published_path(eid), "w", "unit")
        return data, digest

    def _mispublish_e2_with_e1_bytes(self):
        """Reproduce the defect: E-2 marked PUBLISHED carrying E-1's artifact."""
        foreign, foreign_digest = self._publish_healthy("E-1")
        with store.immediate(self.conn):
            store.mark_published(
                self.conn, "E-2", foreign_digest,
                artifacts.published_path("E-2"), "w-bug", "unit",
            )
        with open(artifacts.published_path("E-2"), "wb") as fh:
            fh.write(foreign)
        return foreign, foreign_digest

    def test_healthy_published_export_needs_no_repair(self):
        self._publish_healthy("E-1")
        row = store.get_export(self.conn, "E-1")
        data = render_artifact_bytes(row)
        reason = artifacts.diagnose_published(row, hashlib.sha256(data).hexdigest())
        self.assertIsNone(reason)
        self.assertEqual("none", recovery.reconcile_published(self.conn, "E-1", "fixer"))

    def test_cross_export_mispublication_is_repaired_in_place(self):
        foreign, foreign_digest = self._mispublish_e2_with_e1_bytes()
        bad = store.get_export(self.conn, "E-2")
        with self.assertRaises(artifacts.IdentityMismatch):
            artifacts.load_verified(bad)  # download boundary refuses it

        result = recovery.reconcile_published(self.conn, "E-2", "fixer")
        self.assertEqual("converged", result)

        row = store.get_export(self.conn, "E-2")
        # stage never regressed
        self.assertEqual("PUBLISHED", row["stage"])
        expected = hashlib.sha256(render_artifact_bytes(row)).hexdigest()
        self.assertEqual(expected, row["artifact_digest"])
        self.assertNotEqual(foreign_digest, row["artifact_digest"])
        data = artifacts.load_verified(row)  # now downloadable and verifiable
        import json as _json
        doc = _json.loads(data)
        self.assertEqual("E-2", doc["export_id"])
        self.assertEqual(row["receipt_id"], doc["receipt_id"])
        self.assertEqual(row["input_digest"], doc["input_digest"])
        self.assertEqual(59.3, doc["records"][0]["lat"])
        self.assertEqual(1, len(store.published_artifacts(self.conn, "E-2")))
        events = [e["event"] for e in store.export_events(self.conn, "E-2")]
        self.assertIn("artifact_corrected", events)
        # wrong bytes preserved as evidence, repair leftovers cleaned
        self.assertEqual([], artifacts.repair_files_for("E-2"))
        quarantined = [n for n in os.listdir(config.quarantine_dir()) if n.startswith("E-2.json")]
        self.assertEqual(1, len(quarantined))
        with open(os.path.join(config.quarantine_dir(), quarantined[0]), "rb") as fh:
            self.assertEqual(foreign, fh.read())
        # E-1 untouched
        row1 = store.get_export(self.conn, "E-1")
        self.assertEqual(foreign_digest, row1["artifact_digest"])
        artifacts.load_verified(row1)

    def test_repair_is_idempotent_under_healthy_state(self):
        self._mispublish_e2_with_e1_bytes()
        self.assertEqual("converged", recovery.reconcile_published(self.conn, "E-2", "fixer"))
        self.assertEqual("none", recovery.reconcile_published(self.conn, "E-2", "fixer"))
        self.assertEqual("none", recovery.reconcile_published(self.conn, "E-2", "fixer"))

    def test_missing_published_file_is_rebuilt(self):
        data1, d1 = self._publish_healthy("E-1")
        os.unlink(artifacts.published_path("E-1"))
        self.assertEqual("converged", recovery.reconcile_published(self.conn, "E-1", "fixer"))
        row = store.get_export(self.conn, "E-1")
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(d1, row["artifact_digest"])
        self.assertEqual(data1, artifacts.load_verified(row))

    def test_tick_repairs_mispublished_export(self):
        self._mispublish_e2_with_e1_bytes()
        # reconcile_tick takes its own lease and performs the convergence
        self.assertTrue(worker.reconcile_tick(self.conn, "w-alive"))
        row = store.get_export(self.conn, "E-2")
        self.assertEqual("PUBLISHED", row["stage"])
        artifacts.load_verified(row)
        # second tick is a no-op
        self.assertFalse(worker.reconcile_tick(self.conn, "w-alive"))

    def test_restart_reconverges_after_relaunch(self):
        self._mispublish_e2_with_e1_bytes()
        # simulate a fresh worker process: new identity, new loop
        import threading
        stop = threading.Event()

        def loop(name):
            conn = store.connect()
            try:
                while not stop.is_set():
                    worker.tick(conn, name)
                    time.sleep(0.02)
            finally:
                conn.close()

        t = threading.Thread(target=loop, args=("w-restarted",))
        t.start()
        deadline = time.time() + 10
        while time.time() < deadline:
            row = store.get_export(self.conn, "E-2")
            if row["artifact_digest"] == hashlib.sha256(
                render_artifact_bytes(row)
            ).hexdigest():
                break
            time.sleep(0.05)
        stop.set()
        t.join()
        row = store.get_export(self.conn, "E-2")
        self.assertEqual("PUBLISHED", row["stage"])
        artifacts.load_verified(row)


class PublishPrimitiveTest(RecoveryTestBase):
    def test_publish_never_clobbers(self):
        a = artifacts.tmp_path("E-1", "a")
        b = artifacts.tmp_path("E-1", "b")
        artifacts.write_tmp(a, b"same-content")
        artifacts.write_tmp(b, b"same-content")
        dst = artifacts.published_path("E-1")
        digest = artifacts.sha256_bytes(b"same-content")
        self.assertEqual("linked", artifacts.publish(a, dst, digest))
        self.assertEqual("dedup", artifacts.publish(b, dst, digest))
        self.assertFalse(os.path.exists(a))
        self.assertFalse(os.path.exists(b))
        self.assertEqual(1, len(artifacts.list_published_files()))

    def test_publish_refuses_different_content(self):
        a = artifacts.tmp_path("E-1", "a")
        b = artifacts.tmp_path("E-1", "b")
        artifacts.write_tmp(a, b"content-a")
        artifacts.write_tmp(b, b"content-b")
        dst = artifacts.published_path("E-1")
        artifacts.publish(a, dst, artifacts.sha256_bytes(b"content-a"))
        with self.assertRaises(artifacts.PublishedMismatch):
            artifacts.publish(b, dst, artifacts.sha256_bytes(b"content-b"))

    def test_load_verified_rejects_tampered_bytes(self):
        data = render_artifact_bytes(self.export())
        digest = hashlib.sha256(data).hexdigest()
        with store.immediate(self.conn):
            store.mark_published(self.conn, "E-1", digest, artifacts.published_path("E-1"), "t", "unit")
        artifacts.write_tmp(artifacts.published_path("E-1"), data + b"tamper")
        with self.assertRaises(artifacts.DigestMismatch):
            artifacts.load_verified(self.export())


if __name__ == "__main__":
    unittest.main()
