"""Same-rules multi-export isolation and published-state self-healing.

Regression tests for the historical render-cache bug: two exports under the
same masking rules could end up PUBLISHED with one downloading the other
export's identity/digest/records. The final artifact of an export must be
determined solely by that export id's frozen records, rules snapshot and
first receipt; an already-PUBLISHED-but-foreign row must converge safely
without stage regression or exposing unverified bytes.
"""
import hashlib
import json
import os
import shutil
import tempfile
import unittest

from app import artifacts, config, recovery, store, worker
from app.render import artifact_integrity_digest, render_artifact_bytes


class IsolationTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        os.environ["DATA_DIR"] = self._tmp.name
        os.environ["LEASE_TTL_SECONDS"] = "5"
        config.ensure_dirs()
        self.conn = store.connect()
        store.init_db(self.conn)
        store.set_rules(self.conn, {"rules": [
            {"field": "depth_m", "action": "redact"},
            {"field": "vessel_id", "action": "hash", "length": 10},
        ]})

    def tearDown(self):
        self.conn.close()
        self._tmp.cleanup()
        for var in ("DATA_DIR", "LEASE_TTL_SECONDS", "REPAIR_INTERVAL_SECONDS"):
            os.environ.pop(var, None)

    def _publish(self, export_id, records):
        status, _ = store.submit_export(self.conn, export_id, records)
        self.assertEqual(201, status)
        fencing = store.acquire_lease(self.conn, worker.lease_resource(export_id), "w", 5)
        self.assertIsNotNone(fencing)
        self.assertEqual("published", worker.process_export(self.conn, export_id, "w", fencing))
        store.release_lease(self.conn, worker.lease_resource(export_id), "w", fencing)
        return store.get_export(self.conn, export_id)

    def _download_doc(self, row):
        return json.loads(artifacts.load_verified(row))


class SameRulesIsolationTest(IsolationTestBase):
    RECORDS_A = [{"ts": "t1", "lat": 1.0, "depth_m": 10, "vessel_id": "AAA"}]
    RECORDS_B = [{"ts": "t2", "lat": 2.0, "depth_m": 20, "vessel_id": "BBB"}]

    def test_same_rules_different_records_render_distinct_artifacts(self):
        a = self._publish("S1", self.RECORDS_A)
        b = self._publish("S2", self.RECORDS_B)
        self.assertEqual(a["rules_digest"], b["rules_digest"])  # identical rules
        self.assertNotEqual(a["input_digest"], b["input_digest"])
        self.assertNotEqual(a["artifact_digest"], b["artifact_digest"])

    def test_each_download_carries_its_own_identity_and_records(self):
        a = self._publish("S1", self.RECORDS_A)
        b = self._publish("S2", self.RECORDS_B)
        da, db = self._download_doc(a), self._download_doc(b)
        self.assertEqual("S1", da["export_id"])
        self.assertEqual("S2", db["export_id"])
        self.assertEqual(a["input_digest"], da["input_digest"])
        self.assertEqual(b["input_digest"], db["input_digest"])
        self.assertEqual(a["receipt_id"], da["receipt_id"])
        self.assertEqual("t1", da["records"][0]["ts"])
        self.assertEqual("t2", db["records"][0]["ts"])
        # masking still applied under the shared rules snapshot
        self.assertEqual("***", da["records"][0]["depth_m"])
        self.assertEqual("***", db["records"][0]["depth_m"])
        self.assertNotEqual("AAA", da["records"][0]["vessel_id"])
        self.assertNotEqual("BBB", db["records"][0]["vessel_id"])

    def test_no_render_cache_directory_is_created(self):
        self._publish("S1", self.RECORDS_A)
        self._publish("S2", self.RECORDS_B)
        self.assertFalse(os.path.exists(os.path.join(config.artifacts_dir(), "render-cache")))

    def test_integrity_digest_binds_identity_fields(self):
        a = self._publish("S1", self.RECORDS_A)
        doc = self._download_doc(a)
        self.assertEqual(artifact_integrity_digest(a), doc["integrity_digest"])
        # tampering with any identity field invalidates the binding
        for field in ("export_id", "input_digest", "rules_digest", "receipt_id"):
            altered = dict(a)
            altered[field] = altered[field] + "x"
            self.assertNotEqual(artifact_integrity_digest(altered), doc["integrity_digest"])


class ForeignPublishedRejectionTest(IsolationTestBase):
    RECORDS_A = [{"ts": "t1", "lat": 1.0, "depth_m": 10, "vessel_id": "AAA"}]
    RECORDS_B = [{"ts": "t2", "lat": 2.0, "depth_m": 20, "vessel_id": "BBB"}]

    def _plant_foreign(self, victim, donor):
        # byte-valid donor artifact placed at the victim path, DB digest aligned
        shutil.copyfile(donor["artifact_path"], victim["artifact_path"])
        with store.immediate(self.conn):
            self.conn.execute(
                "UPDATE exports SET artifact_digest = ? WHERE export_id = ?",
                (donor["artifact_digest"], victim["export_id"]),
            )
        return store.get_export(self.conn, victim["export_id"])

    def test_download_refuses_byte_valid_foreign_artifact(self):
        a = self._publish("S1", self.RECORDS_A)
        b = self._publish("S2", self.RECORDS_B)
        victim = self._plant_foreign(b, a)
        # a digest-only check would pass; identity verification must reject it
        with open(victim["artifact_path"], "rb") as fh:
            self.assertEqual(hashlib.sha256(fh.read()).hexdigest(), victim["artifact_digest"])
        with self.assertRaises(artifacts.DigestMismatch):
            artifacts.load_verified(victim)

    def test_repair_converges_foreign_published_without_regression(self):
        a = self._publish("S1", self.RECORDS_A)
        b = self._publish("S2", self.RECORDS_B)
        true_b_digest = b["artifact_digest"]
        victim = self._plant_foreign(b, a)

        repaired = recovery.repair_published_export(self.conn, "S2", "w")
        self.assertTrue(repaired)
        row = store.get_export(self.conn, "S2")
        # stage never left the terminal state
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(true_b_digest, row["artifact_digest"])
        # now downloadable and verifiable as S2 itself
        doc = self._download_doc(row)
        self.assertEqual("S2", doc["export_id"])
        self.assertEqual(b["input_digest"], doc["input_digest"])
        self.assertEqual("t2", doc["records"][0]["ts"])
        # the displaced foreign bytes were preserved as evidence
        self.assertTrue(os.listdir(config.quarantine_dir()))
        events = [e["event"] for e in store.export_events(self.conn, "S2")]
        self.assertIn("repaired", events)
        self.assertIn("repair_quarantined_artifact", events)

    def test_repair_is_idempotent_on_healthy_artifacts(self):
        a = self._publish("S1", self.RECORDS_A)
        self.assertFalse(recovery.repair_published_export(self.conn, "S1", "w"))
        # repair never creates a second published artifact row
        self.assertEqual(1, len(store.published_artifacts(self.conn, "S1")))

    def test_audit_repairs_only_unhealthy_and_keeps_stage(self):
        a = self._publish("S1", self.RECORDS_A)
        b = self._publish("S2", self.RECORDS_B)
        self._plant_foreign(b, a)
        repaired = recovery.audit_published(self.conn, "w")
        self.assertEqual(["S2"], repaired)
        for export_id in ("S1", "S2"):
            row = store.get_export(self.conn, export_id)
            self.assertEqual("PUBLISHED", row["stage"])
            self._download_doc(row)  # both verifiable

    def test_repair_handles_missing_published_file(self):
        a = self._publish("S1", self.RECORDS_A)
        os.unlink(a["artifact_path"])
        self.assertTrue(recovery.repair_published_export(self.conn, "S1", "w"))
        row = store.get_export(self.conn, "S1")
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(a["artifact_digest"], row["artifact_digest"])
        self._download_doc(row)


if __name__ == "__main__":
    unittest.main()
