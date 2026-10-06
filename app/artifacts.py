"""Artifact filesystem operations: temp write, digest verify, atomic publish.

Publish uses hard-link + unlink on the same filesystem: atomic, and it never
clobbers an already-published artifact (second publisher gets FileExistsError
and must verify the existing digest instead).

There is deliberately NO cross-export render cache: an artifact's bytes are
specific to one frozen decision (export id, input digest, rules snapshot,
first receipt), so caching them under a rules-only key would let one export
download another export's identity and records.
"""
import hashlib
import json
import os
import time

from . import config
from .render import artifact_integrity_digest


class PublishedMismatch(Exception):
    """A different artifact already occupies the published path."""


class ArtifactMissing(Exception):
    pass


class DigestMismatch(Exception):
    pass


class PublishedIdentityMismatch(DigestMismatch):
    """An artifact file does not belong to the export it was found under."""


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


def embedded_identity_digest(data):
    """Extract the integrity digest embedded in a rendered artifact, or None."""
    try:
        doc = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    value = doc.get("integrity_digest") if isinstance(doc, dict) else None
    return value if isinstance(value, str) else None


def artifact_identity_ok(data, export_row):
    """The bytes must certify they were rendered for THIS frozen decision.

    This is what stops a valid-but-foreign artifact (e.g. another export's
    records under the same rules) from being served from this export's path.
    """
    embedded = embedded_identity_digest(data)
    if embedded is None:
        return False
    return embedded == artifact_integrity_digest(export_row)


def load_verified(export_row):
    """Read a published artifact only when it certifies the frozen decision.

    The download path must never expose unverified or foreign content: both
    the on-disk content digest (vs. the frozen artifact_digest) and the
    embedded identity (vs. export id / input / rules / first receipt) must
    match.
    """
    path = export_row.get("artifact_path")
    if not path or not os.path.exists(path):
        raise ArtifactMissing("artifact file is missing")
    with open(path, "rb") as fh:
        data = fh.read()
    if sha256_bytes(data) != export_row.get("artifact_digest"):
        raise DigestMismatch("artifact digest mismatch")
    if not artifact_identity_ok(data, export_row):
        raise PublishedIdentityMismatch("artifact does not certify this export decision")
    return data
