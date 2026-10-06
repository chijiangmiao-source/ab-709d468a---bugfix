"""Background worker: processes exports only while holding a valid lease.

Loop: recover stuck exports (lease expired/absent) -> process one RECEIVED
export. Processing stages the artifact to a temp file, records and verifies
its digest, then atomically publishes. Fault-injection hooks (TEST_HOOKS)
simulate a crash after a partial write or after staging.
"""
import hashlib
import os
import socket
import sys
import time
import uuid

from . import artifacts, config, recovery, store
from .render import render_artifact_bytes


def identity():
    return "worker-%s-%d-%s" % (socket.gethostname(), os.getpid(), uuid.uuid4().hex[:6])


def lease_resource(export_id):
    return "export:" + export_id


def _crash(me, export_id, mode):
    print("[%s] fault injection: %s on %s -> exiting" % (me, mode, export_id), flush=True)
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(3)


def process_export(conn, export_id, me, fencing):
    with store.immediate(conn):
        if not store.cas_stage(conn, export_id, "PROCESSING", ("RECEIVED",)):
            return "skipped"
        store.journal(conn, export_id, me, "processing_started", None)

    export = store.get_export(conn, export_id)
    data = render_artifact_bytes(export)
    # The artifact's bytes are identity-bound to this frozen decision.
    artifacts.check_identity(data, export)
    digest = hashlib.sha256(data).hexdigest()
    tmp = artifacts.tmp_path(export_id, uuid.uuid4().hex[:8])

    # Fault: die in the middle of the temp write (leaves a partial artifact).
    if store.pop_fault(conn, export_id, "crash_partial_write"):
        artifacts.write_tmp(tmp, data[: max(1, len(data) // 2)])
        with store.immediate(conn):
            store.journal(conn, export_id, me, "fault_partial_write", tmp)
        _crash(me, export_id, "crash_partial_write")

    artifacts.write_tmp(tmp, data)

    with store.immediate(conn):
        store.record_artifact(conn, export_id, "staged", tmp, digest)
        store.cas_stage(conn, export_id, "STAGED", ("PROCESSING",))
        store.journal(conn, export_id, me, "staged", "digest=%s path=%s" % (digest, tmp))

    # Fault: die after the staged artifact + digest are durably recorded.
    if store.pop_fault(conn, export_id, "crash_after_staged"):
        with store.immediate(conn):
            store.journal(conn, export_id, me, "fault_exit_after_staged", tmp)
        _crash(me, export_id, "crash_after_staged")

    # Verify the staged bytes before anything becomes downloadable.
    if artifacts.sha256_file(tmp) != digest:
        artifacts.quarantine(tmp)
        with store.immediate(conn):
            store.journal(conn, export_id, me, "verify_failed", tmp)
            store.requeue(conn, export_id, me, "digest mismatch after staging")
        return "verify_failed"
    with open(tmp, "rb") as fh:
        artifacts.check_identity(fh.read(), export)

    # Fencing: only the valid lease holder may publish.
    if not store.check_lease(conn, lease_resource(export_id), me, fencing):
        with store.immediate(conn):
            store.journal(conn, export_id, me, "lease_lost", None)
        return "lease_lost"

    pub = artifacts.published_path(export_id)
    via = artifacts.publish(tmp, pub, digest)
    # Never mark PUBLISHED on bytes that do not hash right or carry another
    # export's identity -- the file at the final path is what gets served.
    with open(pub, "rb") as fh:
        served = fh.read()
    if hashlib.sha256(served).hexdigest() != digest:
        raise artifacts.DigestMismatch("published file digest differs from staged digest")
    artifacts.check_identity(served, export)
    with store.immediate(conn):
        store.record_artifact(conn, export_id, "published", pub, digest)
        store.mark_published(conn, export_id, digest, pub, me, via)
    return "published"


# Per-process memo of the last-verified published-file signature
# (inode, mtime_ns, size). Frozen decisions never change, so an unchanged
# signature means the artifact cannot have changed; a restart (or an
# os.replace during repair) drops/misses the marker and re-verifies fully.
_HEALTH_MARKERS = {}


def _signature(path):
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (st.st_ino, st.st_mtime_ns, st.st_size)


def reconcile_tick(conn, me):
    """Scan PUBLISHED rows and converge any with wrong/missing artifacts.

    Healthy steady state costs one stat per export (file content is only read
    and hashed when the signature changed or on first sight after a restart);
    rows that fail the diagnosis take a lease and get repaired.
    Returns True when a repair was performed.
    """
    did_work = False
    for row in store.published_exports(conn):
        export_id = row["export_id"]
        pub = artifacts.published_path(export_id)
        sig = _signature(pub)
        if sig is not None and _HEALTH_MARKERS.get(export_id) == sig:
            continue
        export = store.get_export(conn, export_id)
        data = render_artifact_bytes(export)
        expected_digest = hashlib.sha256(data).hexdigest()
        if artifacts.diagnose_published(export, expected_digest) is None:
            if sig is not None:
                _HEALTH_MARKERS[export_id] = sig
            else:
                _HEALTH_MARKERS.pop(export_id, None)
            continue
        fencing = store.acquire_lease(conn, lease_resource(export_id), me, config.lease_ttl())
        if fencing is None:
            continue
        try:
            result = recovery.reconcile_published(conn, export_id, me)
            did_work = did_work or result == "converged"
            if result == "converged":
                _HEALTH_MARKERS.pop(export_id, None)
        finally:
            store.release_lease(conn, lease_resource(export_id), me, fencing)
    return did_work


def tick(conn, me):
    did_work = False
    # Terminal rows first: a mispublished artifact must converge to its own,
    # verifiable bytes (without ever leaving PUBLISHED).
    did_work = reconcile_tick(conn, me) or did_work
    # Recover exports whose owner vanished (lease expired or absent).
    for row in store.stuck_exports(conn):
        export_id = row["export_id"]
        fencing = store.acquire_lease(conn, lease_resource(export_id), me, config.lease_ttl())
        if fencing is None:
            continue
        try:
            recovery.recover_export(conn, export_id, me)
            did_work = True
        finally:
            store.release_lease(conn, lease_resource(export_id), me, fencing)
    # Process one pending export.
    row = store.next_received(conn)
    if row:
        export_id = row["export_id"]
        fencing = store.acquire_lease(conn, lease_resource(export_id), me, config.lease_ttl())
        if fencing is not None:
            try:
                process_export(conn, export_id, me, fencing)
                did_work = True
            finally:
                store.release_lease(conn, lease_resource(export_id), me, fencing)
    return did_work


def run_forever(me=None):
    me = me or identity()
    config.ensure_dirs()
    conn = store.connect()
    store.init_db(conn)
    recovery.sweep_orphans(conn, me)
    # After a restart, converge any previously mispublished artifacts before
    # serving steady state.
    try:
        reconcile_tick(conn, me)
    except Exception as exc:  # noqa: BLE001 - main loop will retry
        print("[%s] startup reconcile error: %r" % (me, exc), file=sys.stderr, flush=True)
        conn.rollback()
    print("[%s] worker started (poll=%.2fs lease_ttl=%.1fs)" % (me, config.poll_interval(), config.lease_ttl()), flush=True)
    while True:
        try:
            tick(conn, me)
        except Exception as exc:  # keep the loop alive; next tick retries
            print("[%s] tick error: %r" % (me, exc), file=sys.stderr, flush=True)
            try:
                conn.rollback()
            except Exception:
                pass
        time.sleep(config.poll_interval())


def main():
    run_forever()


if __name__ == "__main__":
    main()
