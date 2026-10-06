"""Artifact filesystem operations: temp write, digest verify, atomic publish.

Publish uses hard-link + unlink on the same filesystem: atomic, and it never
clobbers an already-published artifact (second publisher gets FileExistsError
and must verify the existing digest instead).

Every artifact is *identity-bound*: its bytes embed the frozen decision
(export_id, first receipt, received_at, input digest, rules digest). A digest
match alone is not enough to serve a download -- the embedded identity must
match the row, so one export's bytes can never be served under another id.
"""
import hashlib
import json
import os
import time

from . import config

# Fields that bind the artifact bytes to exactly one frozen decision.
IDENTITY_FIELDS = ("export_id", "receipt_id", "received_at", "input_digest", "rules_digest")


class PublishedMismatch(Exception):
    """A different artifact already occupies the published path."""


class ArtifactMissing(Exception):
    pass


class DigestMismatch(Exception):
    pass


class IdentityMismatch(Exception):
    """Artifact bytes embed a different frozen decision than the export row."""


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def tmp_path(export_id, token):
    return os.path.join(config.tmp_dir(), "%s.%s.part" % (export_id, token))


def published_path(export_id):
    return os.path.join(config.published_dir(), "%s.json" % export_id)


def write_tmp(path, data):
    with open(path, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())


def _fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def publish(tmp, dst, expected_digest):
    """Atomically publish tmp at dst without clobbering. Returns 'linked' or 'dedup'."""
    if os.path.exists(dst):
        if sha256_file(dst) == expected_digest:
            if os.path.exists(tmp):
                os.unlink(tmp)
            return "dedup"
        raise PublishedMismatch("published path holds different content: %s" % dst)
    try:
        os.link(tmp, dst)
    except FileExistsError:
        if sha256_file(dst) == expected_digest:
            os.unlink(tmp)
            return "dedup"
        raise PublishedMismatch("published path holds different content: %s" % dst)
    os.unlink(tmp)
    _fsync_dir(os.path.dirname(dst))
    return "linked"


def stage_published(tmp, dst, expected_digest):
    """Hard-link verified tmp next to dst as ``dst.repair-<mtime>`` (never overwrites).

    Used by reconciliation to stage the correct bytes for an export whose
    published file is wrong or missing, while the stage stays PUBLISHED.
    Returns the staged path.
    """
    if sha256_file(tmp) != expected_digest:
        raise DigestMismatch("staged repair artifact failed its own digest check")
    for _ in range(8):
        candidate = "%s.repair-%d-%d" % (dst, int(time.time() * 1000), os.getpid())
        try:
            os.link(tmp, candidate)
        except FileExistsError:
            time.sleep(0.01)
            continue
        _fsync_dir(os.path.dirname(candidate))
        return candidate
    raise PublishedMismatch("could not stage a unique repair path: %s" % dst)


def _unique_quarantine(name):
    os.makedirs(config.quarantine_dir(), exist_ok=True)
    base = "%s.%d" % (name, int(time.time() * 1000))
    target = os.path.join(config.quarantine_dir(), base + ".wrong")
    n = 1
    while os.path.exists(target):
        target = os.path.join(config.quarantine_dir(), "%s.wrong.%d" % (base, n))
        n += 1
    return target


def replace_published(staged, dst, expected_digest):
    """Atomically move a staged repair file onto dst, preserving the old file.

    os.replace is atomic on one filesystem; the previous (wrong) file is first
    hard-linked into the quarantine dir so the original evidence is preserved.
    Returns the quarantine path of the displaced file, or None if none existed.
    """
    if sha256_file(staged) != expected_digest:
        raise DigestMismatch("repair artifact failed digest check before replace")
    quarantined = None
    if os.path.exists(dst):
        if sha256_file(dst) == expected_digest:
            # already the right bytes (e.g. only the DB pointer was stale)
            os.unlink(staged)
            return None
        quarantined = _unique_quarantine(os.path.basename(dst))
        os.link(dst, quarantined)
    os.replace(staged, dst)
    _fsync_dir(os.path.dirname(dst))
    if quarantined:
        _fsync_dir(os.path.dirname(quarantined))
    return quarantined


def quarantine(path):
    os.makedirs(config.quarantine_dir(), exist_ok=True)
    target = os.path.join(
        config.quarantine_dir(),
        "%s.%d" % (os.path.basename(path), int(time.time() * 1000)),
    )
    os.replace(path, target)
    return target


def tmp_files_for(export_id):
    prefix = export_id + "."
    out = []
    directory = config.tmp_dir()
    if os.path.isdir(directory):
        for name in os.listdir(directory):
            if name.startswith(prefix) and name.endswith(".part"):
                out.append(os.path.join(directory, name))
    return out


def cleanup_tmp_for(export_id):
    removed = []
    for path in tmp_files_for(export_id):
        try:
            os.unlink(path)
            removed.append(path)
        except FileNotFoundError:
            pass
    return removed


def list_tmp_files():
    directory = config.tmp_dir()
    if not os.path.isdir(directory):
        return []
    return [os.path.join(directory, name) for name in os.listdir(directory) if name.endswith(".part")]


def list_published_files():
    directory = config.published_dir()
    if not os.path.isdir(directory):
        return []
    return [os.path.join(directory, name) for name in os.listdir(directory) if name.endswith(".json")]


def repair_files_for(export_id):
    """Staged repair files left beside the published path for an export."""
    dst = published_path(export_id)
    directory = os.path.dirname(dst)
    prefix = os.path.basename(dst) + ".repair-"
    if not os.path.isdir(directory):
        return []
    return [os.path.join(directory, name) for name in os.listdir(directory) if name.startswith(prefix)]


def cleanup_repair_files(older_than_seconds=30.0, export_id=None):
    """Remove stale staged-repair files (older than the in-flight safety window)."""
    now = time.time()
    targets = repair_files_for(export_id) if export_id else list_repair_files()
    removed = []
    for path in targets:
        try:
            if now - os.path.getmtime(path) < older_than_seconds:
                continue
            os.unlink(path)
            removed.append(path)
        except FileNotFoundError:
            pass
    return removed


def list_repair_files():
    directory = config.published_dir()
    if not os.path.isdir(directory):
        return []
    return [os.path.join(directory, name)
            for name in os.listdir(directory) if ".repair-" in name]


def verify_artifact_bytes(data, export_row):
    """Digest + identity binding check for candidate artifact bytes.

    Raises DigestMismatch on a wrong digest and IdentityMismatch when the
    bytes embed a different frozen decision (export id, receipt, digests).
    """
    if sha256_bytes(data) != export_row.get("artifact_digest"):
        raise DigestMismatch("artifact digest mismatch")
    check_identity(data, export_row)


def check_identity(data, export_row):
    """Assert the bytes embed exactly the row's frozen decision identity."""
    try:
        doc = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityMismatch("artifact is not a JSON document: %s" % exc)
    for field in IDENTITY_FIELDS:
        if doc.get(field) != export_row.get(field):
            raise IdentityMismatch(
                "artifact %s=%r does not match frozen row %r"
                % (field, doc.get(field), export_row.get(field))
            )


def load_verified(export_row):
    """Read a published artifact only when it is fully safe to serve.

    The download path must never expose unverified or cross-export content:
    the file must exist, its digest must match the row, and the identity baked
    into the bytes must match the frozen decision.
    """
    path = export_row.get("artifact_path")
    if not path or not os.path.exists(path):
        raise ArtifactMissing("artifact file is missing")
    with open(path, "rb") as fh:
        data = fh.read()
    verify_artifact_bytes(data, export_row)
    return data


def diagnose_published(export_row, expected_digest):
    """Cheap pre-flight diagnosis of a PUBLISHED row for reconciliation.

    Returns None when the on-disk artifact is correct (digest + identity),
    otherwise a short reason string. Does not raise on filesystem trouble.
    """
    path = export_row.get("artifact_path") or published_path(export_row["export_id"])
    if not os.path.exists(path):
        return "missing"
    try:
        with open(path, "rb") as fh:
            data = fh.read()
    except OSError as exc:
        return "unreadable:%r" % exc
    actual_digest = sha256_bytes(data)
    if actual_digest != expected_digest:
        return "wrong_digest"
    # The recorded digest may itself be wrong (e.g. a cross-export publish);
    # the expected (recomputed) digest agreeing is not sufficient on its own.
    if actual_digest != export_row.get("artifact_digest"):
        return "recorded_digest_stale"
    try:
        doc = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return "not_json"
    for field in IDENTITY_FIELDS:
        if doc.get(field) != export_row.get(field):
            return "identity:%s" % field
    return None
