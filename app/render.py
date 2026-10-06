"""Deterministic artifact rendering from the frozen decision.

The artifact bytes depend only on the frozen decision (normalized records,
rules snapshot, first receipt), never on wall-clock time, so recovery can
recompute the expected digest and converge to the very same artifact.

Every artifact embeds an ``integrity_digest`` bound to this export's frozen
identity (export id + input digest + rules snapshot digest + first receipt).
Downloads and recovery verify it, so a byte-valid artifact belonging to
another export (e.g. rendered under the same masking rules) can never be
served or converged under this export's id.
"""
import hashlib
import json

from .canonical import canonical
from .masking import apply_rules

GENERATOR = "track-export/1.1"

# Fields whose concatenation uniquely identifies one frozen decision.
_IDENTITY_FIELDS = ("export_id", "input_digest", "rules_digest", "receipt_id")


def artifact_integrity_digest(export_row):
    """Digest binding the artifact to one frozen decision (its true identity)."""
    material = "\n".join(str(export_row[name]) for name in _IDENTITY_FIELDS)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def render_artifact_bytes(export_row):
    records = json.loads(export_row["records"])
    rules_doc = json.loads(export_row["rules_snapshot"])
    masked = apply_rules(records, rules_doc)
    integrity = artifact_integrity_digest(export_row)
    doc = {
        "export_id": export_row["export_id"],
        "receipt_id": export_row["receipt_id"],
        "received_at": export_row["received_at"],
        "input_digest": export_row["input_digest"],
        "rules_digest": export_row["rules_digest"],
        "rules_version": export_row["rules_version"],
        "integrity_digest": integrity,
        "generator": GENERATOR,
        "record_count": len(masked),
        "records": masked,
    }
    return (canonical(doc) + "\n").encode("utf-8")
