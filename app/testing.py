"""Test-only fault helpers (usable when TEST_HOOKS=1).

These deliberately reproduce historical failure shapes so acceptance runs can
observe the system's refusal and self-repair behavior end-to-end.
"""
import os

from . import artifacts, store


class TestHookError(Exception):
    def __init__(self, status, code, message):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def inject_cross_export_artifact(conn, target_id, source_id, actor="test-hook"):
    """Make a PUBLISHED target carry another export's bytes/digest.

    Reproduces the historical mispublication: the target row stays PUBLISHED
    (the status that must never self-correct by regressing stages), its file
    is overwritten with the source export's verified bytes, and the recorded
    digest is set to the source digest. Reconciliation must then converge the
    target back to its own verifiable artifact without leaving PUBLISHED.
    """
    target = store.get_export(conn, target_id)
    source = store.get_export(conn, source_id)
    if not target:
        raise TestHookError(404, "not_found", "unknown target export_id: %s" % target_id)
    if not source:
        raise TestHookError(404, "not_found", "unknown source export_id: %s" % source_id)
    if target_id == source_id:
        raise TestHookError(422, "invalid_corrupt", "source and target must differ")
    if target["stage"] != "PUBLISHED" or source["stage"] != "PUBLISHED":
        raise TestHookError(422, "not_published",
                            "both exports must be PUBLISHED (target=%s source=%s)"
                            % (target["stage"], source["stage"]))
    foreign_bytes = artifacts.load_verified(source)
    foreign_digest = artifacts.sha256_bytes(foreign_bytes)
    dst = artifacts.published_path(target_id)

    staged = "%s.corrupt-%s" % (dst, os.getpid())
    artifacts.write_tmp(staged, foreign_bytes)
    os.replace(staged, dst)

    with store.immediate(conn):
        conn.execute(
            "UPDATE exports SET artifact_digest = ?, artifact_path = ?, updated_at = ? "
            "WHERE export_id = ? AND stage = 'PUBLISHED'",
            (foreign_digest, dst, store.utcnow(), target_id),
        )
        conn.execute(
            "UPDATE artifacts SET digest = ? WHERE export_id = ? AND kind = 'published'",
            (foreign_digest, target_id),
        )
        store.journal(
            conn, target_id, actor, "test_corruption_injected",
            "cross_export_bytes_from=%s foreign_digest=%s" % (source_id, foreign_digest),
        )
    return {"target": target_id, "source": source_id, "foreign_digest": foreign_digest}
