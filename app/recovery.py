"""Crash recovery and published-artifact self-healing.

Recovery runs under the export's lease (worker startup and every tick). For
each unfinished export, consult the journal/artifact records plus on-disk
digests:

* the staged temp artifact is complete AND certifies this export's frozen
  identity (digest matches the recorded and the deterministically recomputed
  digest, embedded integrity digest matches) -> converge: publish it;
* a published file already exists that certifies this export (crash between
  the atomic link and the DB update) -> converge the bookkeeping;
* anything else (partial write, digest mismatch, foreign identity, missing
  file, orphans) -> clean up the残缺 artifacts and requeue the export.

Self-healing runs for exports already in the terminal PUBLISHED stage whose
on-disk artifact is missing, corrupt, or certifies a DIFFERENT export (the
historical render-cache bug left such rows behind). The stage never
regresses: the bad file is quarantined as evidence, a fresh artifact is
rendered from the frozen decision and atomically published, and the digest
columns are corrected in one transaction.
"""
import hashlib
import os
import time
import uuid

from . import artifacts, store
from .render import render_artifact_bytes


def _expected(export_row):
    data = render_artifact_bytes(export_row)
    return data, hashlib.sha256(data).hexdigest()


def _certifies_this_export(data, export_row, digest):
    return (
        artifacts.sha256_bytes(data) == digest
        and artifacts.artifact_identity_ok(data, export_row)
    )


def _converge(conn, export_id, digest, actor, via):
    with store.immediate(conn):
        store.record_artifact(conn, export_id, "published", artifacts.published_path(export_id), digest)
        store.mark_published(conn, export_id, digest, artifacts.published_path(export_id), actor, via)


def publish_verified_artifact(conn, export_row, data, digest, tmp, actor):
    """Publish a freshly rendered+verified artifact for an unfinished export.

    If a file already occupies the published path it is trusted only when it
    has the expected digest and certifies this export; anything else is
    quarantined (evidence preserved) and replaced atomically. Returns
    (path, via).
    """
    export_id = export_row["export_id"]
    pub = artifacts.published_path(export_id)
    if not _certifies_this_export(data, export_row, digest):
        raise artifacts.DigestMismatch("candidate artifact failed identity verification")
    via = "linked"
    if os.path.exists(pub):
        with open(pub, "rb") as fh:
            existing = fh.read()
        if _certifies_this_export(existing, export_row, digest):
            if os.path.exists(tmp):
                os.unlink(tmp)
            via = "dedup"
        else:
            target = artifacts.quarantine(pub)
            with store.immediate(conn):
                store.journal(
                    conn, export_id, actor, "quarantined_foreign_published",
                    "path=%s quarantined=%s" % (pub, target),
                )
            via = artifacts.publish(tmp, pub, digest)
    else:
        via = artifacts.publish(tmp, pub, digest)
    return pub, via


def recover_export(conn, export_id, actor):
    """Recover one unfinished export. Caller must hold the export's lease."""
    export = store.get_export(conn, export_id)
    if not export or export["stage"] == "PUBLISHED":
        return "none"
    data, expected_digest = _expected(export)
    pub = artifacts.published_path(export_id)

    # Case 1: published file already on disk (crash between link and DB update).
    if os.path.exists(pub):
        with open(pub, "rb") as fh:
            on_disk = fh.read()
        if _certifies_this_export(on_disk, export, expected_digest):
            _converge(conn, export_id, expected_digest, actor, "recovery_published_file")
            artifacts.cleanup_tmp_for(export_id)
            return "converged"
        target = artifacts.quarantine(pub)
        with store.immediate(conn):
            store.journal(conn, export_id, actor, "recovery_quarantined_published", target)

    # Case 2: a staged temp artifact certifying this export (digest + identity).
    for row in store.staged_artifacts(conn, export_id):
        path = row["path"]
        if not os.path.exists(path):
            continue
        with open(path, "rb") as fh:
            staged = fh.read()
        if _certifies_this_export(staged, export, row["digest"]) and row["digest"] == expected_digest:
            _, via = publish_verified_artifact(conn, export, staged, row["digest"], path, actor)
            _converge(conn, export_id, row["digest"], actor, "recovery_staged_artifact:%s" % via)
            artifacts.cleanup_tmp_for(export_id)
            return "converged"

    # Case 3: incomplete/mismatched/foreign remains -> clean up and requeue.
    removed = artifacts.cleanup_tmp_for(export_id)
    with store.immediate(conn):
        for row in store.staged_artifacts(conn, export_id):
            store.abort_artifact(conn, row["id"])
        store.journal(conn, export_id, actor, "recovery_cleanup", "removed=%d" % len(removed))
        store.requeue(conn, export_id, actor, "recovery_cleanup removed=%d" % len(removed))
    return "requeued"


# ------------------------------------------------- published-state self-heal

def published_export_ids(conn, limit=500):
    rows = conn.execute(
        "SELECT export_id FROM exports WHERE stage = 'PUBLISHED' ORDER BY published_at, export_id LIMIT ?",
        (limit,),
    ).fetchall()
    return [row["export_id"] for row in rows]


def repair_published_export(conn, export_id, actor):
    """Converge one PUBLISHED export to its true, verifiable artifact.

    Returns True when a repair was made. The stage is never changed and
    unverified bytes are never downloadable at any point.
    """
    export = store.get_export(conn, export_id)
    if not export or export["stage"] != "PUBLISHED":
        return False
    pub = artifacts.published_path(export_id)
    old_digest = export["artifact_digest"]

    healthy = False
    if os.path.exists(pub):
        try:
            with open(pub, "rb") as fh:
                on_disk = fh.read()
            healthy = (
                artifacts.sha256_bytes(on_disk) == old_digest
                and artifacts.artifact_identity_ok(on_disk, export)
            )
        except OSError:
            healthy = False
    if healthy:
        return False

    reason = "missing" if not os.path.exists(pub) else "corrupt_or_foreign"
    quarantine_target = None
    try:
        if os.path.exists(pub):
            quarantine_target = artifacts.quarantine(pub)
    except FileNotFoundError:
        pass  # another repairer already moved it; identical render below dedups

    # Render the true artifact purely from the frozen decision.
    data, digest = _expected(export)
    tmp = artifacts.tmp_path(export_id, "repair-%s" % uuid.uuid4().hex[:8])
    artifacts.write_tmp(tmp, data)
    try:
        # publish() never clobbers: a concurrent identical repair is deduped.
        via = artifacts.publish(tmp, pub, digest)
    except artifacts.PublishedMismatch:
        # Another repairer raced and linked different (bad) content; retry audit.
        if os.path.exists(tmp):
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
        return False

    with store.immediate(conn):
        store.journal(
            conn, export_id, actor, "repair_quarantined_artifact",
            "reason=%s old_digest=%s quarantined=%s" % (reason, old_digest, quarantine_target),
        )
        # Preserve the original published-artifact evidence as an aborted row.
        conn.execute(
            "UPDATE artifacts SET kind = 'aborted' WHERE export_id = ? AND kind = 'published'",
            (export_id,),
        )
        store.record_artifact(conn, export_id, "published", pub, digest)
        # Terminal stage guard: only correct a PUBLISHED row; published_at is
        # left untouched so the original publication instant is preserved.
        cur = conn.execute(
            "UPDATE exports SET artifact_digest = ?, artifact_path = ?, updated_at = ? "
            "WHERE export_id = ? AND stage = 'PUBLISHED'",
            (digest, pub, store.utcnow(), export_id),
        )
        store.journal(
            conn, export_id, actor, "repaired",
            "via=%s old_digest=%s new_digest=%s" % (via, old_digest, digest),
        )
    return cur.rowcount == 1


def published_is_healthy(conn, export_id):
    """Lock-free health probe used to short-circuit the periodic audit.

    A positive answer is advisory (the authoritative re-check still happens
    under the lease inside repair_published_export); a negative answer means
    'take the lease and look carefully'.
    """
    export = store.get_export(conn, export_id)
    if not export or export["stage"] != "PUBLISHED":
        return True  # nothing the audit is responsible for
    pub = artifacts.published_path(export_id)
    if not os.path.exists(pub):
        return False
    try:
        with open(pub, "rb") as fh:
            on_disk = fh.read()
    except OSError:
        return False
    return (
        artifacts.sha256_bytes(on_disk) == export["artifact_digest"]
        and artifacts.artifact_identity_ok(on_disk, export)
    )


def audit_published(conn, actor, lease_ttl_seconds=None):
    """Repair every unhealthy PUBLISHED export. Returns repaired export ids.

    Healthy exports are skipped without touching a lease; each actual repair
    is serialized by the export's ordinary lease so two workers never rebuild
    the same published artifact at once.
    """
    from . import config

    ttl = lease_ttl_seconds if lease_ttl_seconds is not None else config.lease_ttl()
    repaired = []
    for export_id in published_export_ids(conn):
        if published_is_healthy(conn, export_id):
            continue
        fencing = store.acquire_lease(conn, "export:" + export_id, actor, ttl)
        if fencing is None:
            continue  # another worker is auditing / processing it
        try:
            try:
                if repair_published_export(conn, export_id, actor):
                    repaired.append(export_id)
            except Exception as exc:  # never let one bad export block the audit
                with store.immediate(conn):
                    store.journal(conn, export_id, actor, "repair_failed", repr(exc)[:300])
        finally:
            store.release_lease(conn, "export:" + export_id, actor, fencing)
    return repaired


def sweep_orphans(conn, actor, older_than_seconds=30.0):
    """Delete temp files not referenced by any staged artifact record."""
    removed = []
    known = set()
    for export in store.list_exports(conn, limit=1000):
        for row in store.staged_artifacts(conn, export["export_id"]):
            known.add(os.path.abspath(row["path"]))
    now = time.time()
    for path in artifacts.list_tmp_files():
        if os.path.abspath(path) in known:
            continue
        if now - os.path.getmtime(path) < older_than_seconds:
            continue  # may belong to an in-flight staging; leave it alone
        try:
            os.unlink(path)
            removed.append(path)
        except FileNotFoundError:
            pass
    if removed:
        with store.immediate(conn):
            store.journal(conn, None, actor, "recovery_orphan_sweep", "removed=%d" % len(removed))
    return removed
