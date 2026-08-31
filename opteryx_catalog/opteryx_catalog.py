from __future__ import annotations

import hashlib
import logging
import re
import secrets

# The "no expectation" sentinel for save_dataset_metadata, defined with the
# commit paths that pass it (catalog/dataset.py).
from .catalog.dataset import _NO_SNAPSHOT_EXPECTATION
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from google.cloud import firestore
from google.cloud import storage

from .alerts import report as _alert
from .audit import emit_audit
from .catalog.dataset import SimpleDataset
from .catalog.dataset import _as_int
from .catalog.metadata import DatasetMetadata
from .catalog.metadata import Snapshot
from .catalog.metadata import snapshot_is_tombstoned
from .catalog.metastore import Metastore
from .catalog.orphan_quarantine import MAINTENANCE_SUBCOLLECTION
from .catalog.view import View as CatalogView
from .exceptions import CollectionAlreadyExists
from .exceptions import ConstraintNotFound
from .exceptions import CollectionLocked
from .exceptions import CollectionNotEmpty
from .exceptions import CollectionNotFound
from .exceptions import DatasetAlreadyExists
from .exceptions import DatasetLocked
from .exceptions import DatasetNotFound
from .exceptions import EgressRestricted
from .exceptions import MaterializedViewError
from .exceptions import NameInUse
from .exceptions import SnapshotAncestryTooDeep
from .exceptions import SnapshotMissingError
from .exceptions import TagAlreadyExists
from .exceptions import TagLimitExceeded
from .exceptions import TagNotFound
from .exceptions import TaskAlreadyExists
from .exceptions import TaskNotFound
from .exceptions import TriggerNotFound
from .exceptions import ViewAlreadyExists
from .exceptions import ViewNotFound
from .exceptions import WorkspaceDeletionProtected
from .exceptions import WorkspaceNotFound
from .exceptions import WorkspaceStorageReclaimFailed
from .exceptions import PlatformIdentityOwnerRefused
from .iops.base import FileIO
from .resource_types import ResourceType
from .webhooks import send_webhook
from .webhooks.events import dataset_created_payload
from .webhooks.events import dataset_deleted_payload
from .webhooks.events import dataset_renamed_payload
from .webhooks.events import view_created_payload
from .webhooks.events import view_deleted_payload
from .webhooks.events import workspace_deleted_payload
from .webhooks.events import workspace_locked_payload
from .webhooks.events import workspace_unlocked_payload

logger = logging.getLogger(__name__)

# Subcollection under a dataset document holding that dataset's triggers - one
# document per trigger, keyed by trigger name. A subcollection rather than a
# doc field for the same reason as `maintenance`: `load_dataset` reads the
# dataset document on every call and must not pay for opt-in state. The commit
# path reads this subcollection to decide what to fire.
TRIGGERS_SUBCOLLECTION = "triggers"

# Identities the platform runs itself as. They may not be pinned as a trigger's
# or a materialized view's `runs-as`: nothing bills them, so work pinned to one
# is standing compute charged to nobody, and `federator`'s credential is
# deliberately distributed to every service that commits a dataset, which makes
# it the widest authority in the system to hand unattended work.
#
# Deliberately a short, closed list, mirroring `opteryx_access.checks`, which
# holds the same names for the same reason and is where the SQL surface's gate
# reads them. Duplicated rather than imported because this library depends on
# neither the engine nor the access package - it is installed in services that
# have no permissions capability at all - and a check that silently vanished
# when an optional dependency was absent would be worse than a second copy of
# two strings. If a third name is ever added, both lists move together.
PLATFORM_IDENTITIES = frozenset({"federator", "xb500"})

# The reserved workspace of curated open data, and the identity that maintains
# it. Together they are the one place the rule above cannot apply - see
# `_assert_can_own`.
PUBLIC_WORKSPACE = "public"
PUBLIC_MAINTENANCE_IDENTITY = "xb500"


def _assert_can_own(principal: str | None, what: str, *, workspace: str | None = None) -> None:
    """Refuse a platform identity as an unattended-run owner.

    Silent on None: a missing owner is a different failure with its own
    handling at fire time (`MaterializedViewOwnerMissing`, `TaskOwnerMissing`),
    and conflating the two would report "cannot be billed" for a damaged record.

    ONE EXEMPTION: `xb500` may own a TRIGGER in `public`. The rule assumes a
    billable owner is always available and that reaching for a platform identity
    is a way of dodging the bill. In `public`, neither half holds.

    Reached only by passing `workspace`, which the trigger paths do and the
    materialized-view paths deliberately do not: a view in `public` is not
    something the platform currently maintains, and an exemption nothing needs
    is one nobody is checking. A caller that omits the argument gets the
    unexempted rule, so a path added later arrives strict by default.

    There is no billable owner to demand. `public` is reserved:
    `opteryx_access.checks.implicit_grants` caps every non-platform identity at
    `reader` over `public.*` and answers from that pass without consulting
    issued policies, and `validate_pattern` refuses to write a policy over the
    workspace at all - so WRITE there is reachable only by the identities in
    `PLATFORM_IDENTITIES`. Requiring a billable owner for a trigger whose task
    writes `public` does not yield a safely-owned trigger; it yields one that
    fails on every fire.

    And the work is not unbilled. Maintaining public data is on the house, and
    that is already how it is metered: jobs.opteryx resolves an owner with no
    billing membership to `HOUSE_BILLING_ACCOUNT`
    (`interface._billing_account_for_principal`), a fallback documented there as
    keeping a platform-armed trigger off a customer's bill. "Charged to nobody"
    describes an identity the biller has no answer for, and it has one.

    `federator` stays refused, here as everywhere. Only the COSTING half of the
    original rationale is answered above; the other half is untouched, and is
    about that identity specifically - its credential is shipped to every
    service that commits a dataset, making it the widest authority in the system
    to hang standing work on. `xb500` is held by the xb500 service and the Pi
    that runs the ingest jobs, which is why it can carry this and federator
    cannot.
    """
    if not principal:
        return
    normalized = principal.strip().lower()
    if normalized not in PLATFORM_IDENTITIES:
        return
    if workspace == PUBLIC_WORKSPACE and normalized == PUBLIC_MAINTENANCE_IDENTITY:
        return
    raise PlatformIdentityOwnerRefused(
        f"{principal} cannot be the owner of {what}. It is a platform identity "
        "rather than an account, so work it performs is billed to nobody - and "
        "unattended runs execute as the owner. Use a user or service account, "
        "both of which carry a billing account."
    )


# Subcollection under a dataset document holding the relationships DECLARED ON
# that dataset - one document per relationship, keyed by the constraint name,
# which is the handle `ALTER TABLE ... DROP CONSTRAINT` removes it by.
#
# Keyed under the dataset for the same reason the relationship graph's design
# wanted a `(workspace, collection, dataset)` cluster key: the dominant read by
# a wide margin is "what relates to THIS dataset", behind every `$metadata`
# build. As a subcollection that is not a cluster key to maintain, it is the
# parent - one keyed read, and a dropped dataset takes its relationships with
# it exactly as it takes its triggers.
#
# Nothing here is enforced, ever. A declared relationship records that two
# columns hold corresponding values; a write that breaks it succeeds. This is
# metadata for tooling and discovery, and the engine never reads it back to
# plan or rewrite a query.
RELATIONSHIPS_SUBCOLLECTION = "relationships"

# Subcollection under a dataset document holding the COLUMN PAIRS inference has
# been told not to propose again - one document per rejected pair, keyed by the
# pair digest below.
#
# Keyed on the PAIR, never on the constraint name. An inference run generates
# its own names, so a rejection recorded against a name would be re-proposed
# under the next run's name and the owner would reject the same claim forever.
# The pair is what was actually judged.
#
# Deliberately NOT a status on the relationship document itself. A rejected
# proposal is not a relationship - it is a statement that no relationship is
# there - and leaving it in `relationships` would put it in front of every
# consumer that reads that subcollection, each of which would then need to know
# to exclude it. The status enum stays `active | unverified | broken`.
#
# No expiry. A rejection says the values happen to overlap and the columns still
# do not correspond, which is a fact about what the data MEANS; nothing a later
# run observes changes it, because a later run observes the same kind of
# evidence that was already rejected. Only a person can undo it, by dropping the
# suppression - and `rejected-at-ms` is recorded so a future policy could revisit
# that decision with the date in hand.
RELATIONSHIP_SUPPRESSIONS_SUBCOLLECTION = "relationship-suppressions"

# Subcollection under a dataset document holding that dataset's snapshot tags -
# one document per tag, keyed by the NORMALIZED tag name. The document id doing
# the naming is the point: uniqueness is then Firestore's (a create-if-absent
# write, with no read-then-write race to lose) rather than ours, and
# immutability is structural - tag documents are created and deleted, never
# updated.
#
# A subcollection rather than a field on the dataset document, because
# `save_dataset_metadata` writes that document whole with `set()`: a field
# `DatasetMetadata` does not carry is DESTROYED by the next commit, not left
# stale. Tags here are never in that blast radius, and a tag write never
# contends with a commit.
TAGS_SUBCOLLECTION = "tags"

# Tasks sit beside `datasets` and `views` under a collection, not inside a
# dataset: a task is a named catalog object addressed as
# `<workspace>.<collection>.<task>` by EXECUTE, exactly as a view is addressed
# by a SELECT. Which dataset's commits FIRE a task is a separate fact, and it
# lives where it always has - a trigger document on that dataset.
TASKS_SUBCOLLECTION = "tasks"

# Most tags one dataset may hold. A tag pins its snapshot from expiry until it
# is dropped - nothing ages one out - so this is the only bound on how much
# history a single dataset can hold alive, and every byte of it is charged.
MAX_TAGS_PER_DATASET = 100

# Tag names nobody may take, because they name the head or the version behind
# it. A tag is IMMUTABLE, so `current` as a real tag would stop meaning "the
# head" from the moment it was created and would keep the word working while
# quietly returning a frozen snapshot - the one failure mode a time-travel read
# must not have. Compared against the NORMALIZED (lowercase) name.
#
# `current` and `previous` are answered by the read path and surfaced as virtual
# tags (see `virtual_tags`). `last` and `head` are not answered by anything: they
# are reserved because they are what a person reaches for when they mean the
# pointer, and a real tag holding one of those words would answer a
# pointer-shaped question with a snapshot frozen at the moment it was created.
# Reserving a word costs nothing; taking it back after someone has scripted
# against it costs a migration.
RESERVED_TAG_NAMES = frozenset({"current", "previous", "last", "head"})

# Names the read path USED to answer and no longer does. `latest` was renamed to
# `current`; the rename is a hard cutover, so `VERSION AS OF latest` must fail.
#
# Retired is not the same as free. If `latest` were merely dropped from the
# reserved set, the first person to create a tag by that name would make the old
# spelling resolve again - to one frozen snapshot, forever. That is a silent
# wrong answer where the cutover promised a loud failure, so the word stays
# unavailable and the error says why.
RETIRED_TAG_NAMES = frozenset({"latest"})

# Reserved names the read path resolves to a real snapshot, and which are shown
# alongside real tags in a snapshot listing. Both are RESERVED, so no stored tag
# can ever collide with one.
VIRTUAL_TAG_NAMES = ("current", "previous")

# A tag name is a SQL identifier: a letter, then letters, digits or
# underscores. No dots (they are the catalog's own separator) and no hyphens.
_TAG_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")

# Prefix on every constraint name inference generates. Not decoration: it is
# what tells a reader looking at a DROP CONSTRAINT statement that the name was
# machine-made, and it keeps generated names in their own space so one can
# never collide with a name a person chose.
INFERRED_CONSTRAINT_PREFIX = "inferred_"


def relationship_pair_digest(
    column: str,
    references_collection: str,
    references_dataset: str,
    references_column: str,
) -> str:
    """A stable 16-hex identity for one directed column pair.

    THE PAIR, NOT THE NAME, is what inference and rejection both key on. A
    constraint name is a handle a person or a generator chose; the pair is the
    claim being made. Keying on the name would let the next run re-propose a
    rejected pair under a new name, and would let a second run duplicate a
    proposal the first already wrote.

    The near dataset is NOT part of the digest - the digest is only ever used
    inside that dataset's own subcollections, so including it would add a
    constant. The workspace is absent for the same reason, and because a
    relationship cannot cross one.

    Case is normalised because column and dataset names are not case-sensitive
    here, and the two sides of a comparison come from different places - the
    parser's tokens on one, a stored document on the other.
    """
    payload = "\x00".join(
        part.casefold()
        for part in (column, references_collection, references_dataset, references_column)
    )
    return hashlib.blake2b(payload.encode("utf-8"), digest_size=8).hexdigest()


def generated_constraint_name(
    column: str,
    references_collection: str,
    references_dataset: str,
    references_column: str,
) -> str:
    """The constraint name inference gives one proposed pair.

    STABLE ACROSS RUNS, because the document id IS the constraint name and
    uniqueness is Firestore's: a second run over unchanged data must land on
    the same id and update that document, not write a second one beside it.
    So the name is a pure function of the pair and carries no timestamp, no
    score and no run id.

    Readable head, unique tail. The near column name is carried in clear
    because the name appears in the `DROP CONSTRAINT` an owner may type, and
    `inferred_a3f19c2b04e7d518` tells them nothing about what they are
    dropping; the digest is what actually makes it unique, since the column
    name alone collides as soon as one column is proposed against two targets.

    Sanitised to the identifier grammar the rest of this catalog uses - a
    column name may contain characters a constraint name may not.
    """
    stem = re.sub(r"[^A-Za-z0-9_]", "_", column or "")[:48].strip("_")
    digest = relationship_pair_digest(
        column, references_collection, references_dataset, references_column
    )
    if not stem:
        return f"{INFERRED_CONSTRAINT_PREFIX}{digest}"
    return f"{INFERRED_CONSTRAINT_PREFIX}{stem}_{digest}"

# Longest accepted tag name, in characters.
MAX_TAG_NAME_LENGTH = 128

# The only trigger kind in v1: re-run a materialized view's defining SQL when
# the dataset carrying the trigger takes a user-created commit.
MV_REFRESH_TRIGGER_KIND = "materialized_view_refresh"
# Trigger kind for a task run. Defined here beside the refresh kind, and
# imported by `trigger_firing`, so the creation gate and the firing path cannot
# disagree about which triggers carry an identity of their own.
TASK_TRIGGER_KIND = "task"

# Value of a dataset document's `dataset-type` field marking it as the backing
# table of a materialized view. Plain datasets have no `dataset-type` field.
MATERIALIZED_VIEW_TYPE = "materialized_view"

# Workspace `$properties` flag: when on, this workspace's datasets may not be
# copied into a *different* workspace by an automated, repeating copy
# (materialized view refresh, CTAS). See `OpteryxCatalog.enforce_egress_policy`
# for what that does and does not stop.
EGRESS_PROTECTION_PROPERTY = "egress_protection"

# Workspace `$properties` flag: when on, the workspace itself cannot be deleted.
DELETION_PROTECTION_PROPERTY = "deletion_protection"


@dataclass(frozen=True)
class EgressRefusal:
    """One source workspace refusing to let a copy of its data leave.

    Returned by `OpteryxCatalog.egress_verdict`, which reports EVERY refusal
    rather than stopping at the first, so a caller can tell someone all the
    workspaces they have to clear instead of one per attempt.

    The wording lives here, in one place, because both shapes of the gate use
    it: `enforce_egress_policy` raises with `str(refusal)`, and a caller
    composing its own message reads `workspace` and `remediation` off it.
    """

    workspace: str
    destination: str
    operation: str

    @property
    def remediation(self) -> str:
        """The statement that clears this refusal - run against the SOURCE."""
        return f"ALTER WORKSPACE {self.workspace} SET {EGRESS_PROTECTION_PROPERTY} TO OFF."

    def __str__(self) -> str:
        return (
            f"Cannot {self.operation}: it would copy data out of workspace "
            f"'{self.workspace}' into '{self.destination}', and "
            f"'{self.workspace}' restricts egress. Clear it with {self.remediation}"
        )


def _guard_is_on(properties: dict, name: str) -> bool:
    """Read a workspace guard flag, which is ON unless explicitly turned off.

    Both guards (`deletion_protection`, `egress_protection`) are tri-state and
    default to protecting: unset means ON, and so does a workspace with no
    `$properties` document at all. A workspace is born protected and stays that
    way until someone deliberately writes the flag off - the states that mean
    "nobody has decided yet" resolve to the safe answer, not the permissive one.

    Only an explicit falsey value turns a guard off. That is what the engine
    writes for `... TO OFF` / `TO FALSE` (`_parse_boolean_workspace_property`
    stores a real bool), and it means an unrecognised value left by hand -
    the string "OFF", say - keeps the guard on rather than silently clearing
    it. Fail-closed is the right way for a default-on flag to be wrong.

    `None` reads as unset, matching `set_workspace_properties`, where writing
    None is how a property is removed.
    """
    value = properties.get(name)
    if value is None:
        return True
    return bool(value)


def _core_type_to_stored(column_type: Any) -> tuple:
    """Map an Opteryx ``ColumnType`` to ``(type_name, element_type, precision, scale)``.

    ``type_name`` is the EXACT type string — ``str(ColumnType)`` — which is what
    ``parse_column_type`` reads back unchanged: ``IPV4``, ``UINT32``, ``INT8``,
    ``FLOAT32``, ``TIMESTAMP[ms]``, ``DECIMAL(10, 2)``, ``ARRAY<VARCHAR>``.

    It used to store the dispatch CATEGORY name, which is lossy in the direction
    that silently WIDENS and cannot be detected downstream: IPv4's category is
    INTEGER (deliberately — that is what makes ordering, grouping and joins run
    on the raw uint32), and so is every unsigned and narrow width's, so IPV4,
    UINT32, UINT64 and INT8 all persisted as ``INTEGER`` and came back as a
    signed INT64. A stored address then rendered as a number, and an unsigned
    column silently became signed.

    ``precision``/``scale``/``element_type`` stay populated alongside it. They are
    redundant with the parameterized name now, but they are separate stored
    columns other readers consume, and the engine's reader still falls back to
    them for the bare ``DECIMAL``/``ARRAY`` spellings written before this.
    """
    if column_type is None:
        return ("VARCHAR", None, None, None)

    type_name = str(column_type)
    category = column_type.category.name
    if category == "DECIMAL":
        logical = column_type.logical
        precision = getattr(logical, "precision", None) if logical is not None else None
        scale = getattr(logical, "scale", None) if logical is not None else None
        return (type_name, None, precision, scale)
    if category == "ARRAY":
        element = column_type.element
        element_name = element.category.name if element is not None else None
        return (type_name, element_name, None, None)
    return (type_name, None, None, None)


def _stored_columns_of(schema: Any):
    """`(name, quartet)` per column when `schema` is already in stored spelling.

    None when it is not, so the caller falls through to the duck-typed shapes.
    A caller holding `{"name": "source_ip", "type": "IPV4"}` has said everything
    this method needs; the only work left is filling in the parameters that the
    name already carries, so `DECIMAL(10, 2)` still stores a precision and a
    scale and `ARRAY<VARCHAR>` still stores an element type.

    An explicit `precision`, `scale` or `element-type` on the column wins over
    the parsed one. A caller that spelled them out means them, and evolution
    hands back stored dicts that carry exactly those keys.
    """
    columns = schema.get("columns") if isinstance(schema, dict) else schema
    if not isinstance(columns, (list, tuple)):
        return None
    if not all(isinstance(column, dict) and "name" in column for column in columns):
        return None
    if not columns:
        return []

    out = []
    for column in columns:
        declared = column.get("type")
        type_name, element_type, precision, scale = _parse_stored_type(declared)
        out.append(
            (
                column["name"],
                (
                    type_name,
                    column.get("element-type", element_type),
                    column.get("precision", precision),
                    column.get("scale", scale),
                ),
            )
        )
    return out


def _parse_stored_type(declared: Any) -> tuple:
    """Split a stored type name into the quartet the column document holds.

    The name is authoritative and is stored unchanged - `parse_column_type` on
    the engine side reads it back whole. What is pulled out of it is only the
    redundant copies other readers consume.
    """
    if declared is None:
        return ("VARCHAR", None, None, None)
    name = str(declared).strip()
    upper = name.upper()
    if upper.startswith("DECIMAL(") and name.endswith(")"):
        parameters = name[name.index("(") + 1 : -1].split(",")
        if len(parameters) == 2:
            try:
                return (name, None, int(parameters[0].strip()), int(parameters[1].strip()))
            except ValueError:
                # A name we cannot read the parameters out of is still a name.
                # Storing it whole and leaving the copies empty is better than
                # refusing a dataset over the redundant half of the record.
                return (name, None, None, None)
        return (name, None, None, None)
    if upper.startswith("ARRAY<") and name.endswith(">"):
        return (name, name[name.index("<") + 1 : -1].strip() or None, None, None)
    return (name, None, None, None)


def _expand_column_type(column: dict) -> dict:
    """Resolve a ``column_type`` key into the stored ``type``/``element-type``/
    ``precision``/``scale`` quartet.

    Schema evolution (``alter_dataset_schema``) is handed columns by the query
    engine, which holds Opteryx ``ColumnType`` objects, not the catalog's stored
    spelling. Converting here rather than at the caller keeps
    ``_core_type_to_stored`` the single place that mapping happens - a caller
    that spelled a type itself would drift from what ``CREATE TABLE`` writes for
    the identical column.

    A column that already carries an explicit ``type`` is passed through
    untouched, so a caller with a stored dict in hand does not have to
    round-trip it through a ``ColumnType``.
    """
    if "column_type" not in column:
        return column
    resolved = {k: v for k, v in column.items() if k != "column_type"}
    type_name, element_type, precision, scale = _core_type_to_stored(column["column_type"])
    resolved["type"] = type_name
    resolved["element-type"] = element_type
    resolved["precision"] = precision
    resolved["scale"] = scale
    return resolved


# draken physical type name (DrakenType.name, from Morsel.schema) -> the name to
# STORE. Every entry must be a spelling opteryx's parse_column_type reads back,
# which is why this is not simply `DrakenType.name`: DATE32, TIMESTAMP64, TIME32,
# TIME64 and VECTOR_FP16 do not parse, and a stored name that does not parse
# falls through the reader to its VARCHAR default — a timestamp column silently
# becoming a string, which is worse than the widening this change fixes.
#
# The EXACT widths map to themselves. That is the whole point: they used to
# collapse to INTEGER/FLOAT, so a UINT32 column read back signed and 64-bit.
#
# The rest keep the category spelling they always had, because a Morsel's schema
# exposes only the physical tag — no DECIMAL precision/scale, no VECTOR
# dimension, no TIMESTAMP unit — so there is nothing more exact to write. The
# reader's parameter-aware branches handle those bare names, as they always did.
_DRAKEN_STORED_NAME = {
    "INT8": "INT8",
    "INT16": "INT16",
    "INT32": "INT32",
    "INT64": "INT64",
    "UINT8": "UINT8",
    "UINT16": "UINT16",
    "UINT32": "UINT32",
    "UINT64": "UINT64",
    "FLOAT32": "FLOAT32",
    "FLOAT64": "FLOAT64",
    "DECIMAL": "DECIMAL",
    "DECIMAL128": "DECIMAL",
    "DATE32": "DATE",
    "TIMESTAMP64": "TIMESTAMP",
    "TIME32": "TIME",
    "TIME64": "TIME",
    "INTERVAL": "INTERVAL",
    # BOOL, not BOOLEAN — BOOL is the canonical spelling opteryx-core's
    # `_NAME_TO_PHYSICAL` resolves directly; BOOLEAN only still reads back
    # because `_SQL_NAME_ALIASES` keeps the older spelling alive for schemas
    # persisted before the rename.
    "BOOL": "BOOL",
    "VARCHAR": "VARCHAR",
    "NVARCHAR": "NVARCHAR",
    "VARBINARY": "VARBINARY",
    "VARIANT": "VARIANT",
    "ARRAY": "ARRAY",
    "NULL": "NULL",
    "VECTOR_FP16": "VECTOR",
}


def _morsel_type_to_stored(dtype: Any) -> tuple:
    """Map a draken ``DrakenType`` (from ``Morsel.schema``) to
    ``(type_name, element_type, precision, scale)``.

    This is the write/create path (a Morsel's schema is being persisted), so
    draken's physical type tag is used directly — no opteryx-core or pyarrow
    involved. ``type_name`` uses the same category names as
    :func:`_core_type_to_stored`.

    ``Morsel.schema`` only exposes the physical type tag, not parameterization
    (DECIMAL precision/scale, ARRAY element type), so those fall back to the
    same defaults used on read (see ``dataset.py``'s ``_stored_type_display``).
    """
    name = getattr(dtype, "name", str(dtype))
    stored = _DRAKEN_STORED_NAME.get(name, "VARCHAR")
    if stored == "DECIMAL":
        return ("DECIMAL", None, 38, 9)
    if stored == "ARRAY":
        return ("ARRAY", "VARIANT", None, None)
    return (stored, None, None, None)


_SNAPSHOT_SUMMARY_KEYS = (
    "added-data-files",
    "added-files-size",
    "added-records",
    "deleted-data-files",
    "deleted-files-size",
    "deleted-records",
    "total-data-files",
    "total-files-size",
    "total-records",
)


def _virtual_tag_row(name: str, snapshot_id: int, comment: str) -> dict:
    """A computed tag in the same shape `list_tags` returns for a stored one.

    Carries the keys a consumer reads off a real tag, so a listing can hold both
    kinds without special-casing every field access, plus `virtual: True` for the
    one thing that must never be confused: these are not stored, cannot be
    dropped, and pin nothing against expiry.

    `created-by` and `created-at-ms` are explicitly None rather than absent -
    nobody created these, and the honest answer to "who made this tag?" is
    nobody, not a missing key that reads as a lost value.
    """
    return {
        "name": name,
        "snapshot-id": int(snapshot_id),
        "virtual": True,
        "created-by": None,
        "created-at-ms": None,
        "comment": comment,
    }


def _snapshot_to_document(snapshot: Snapshot) -> dict:
    """The canonical Firestore document for one snapshot.

    BOTH writers of a snapshot document — `save_snapshot` and
    `save_dataset_metadata`'s upsert loop — MUST serialize through here.
    They write the SAME document id with `.set()`, which REPLACES the
    document rather than merging it, so any field one writer omits is
    silently ERASED by the other.

    They previously carried different field sets, and `save_dataset_metadata`
    runs last in every write path (`SimpleDataset.append`/`overwrite`/
    `truncate`/`refresh_manifest` all call `save_snapshot` then
    `save_dataset_metadata`), so the two fields only `save_snapshot` wrote —
    `operation-type` and `parent-snapshot-id` — were written and then wiped
    on every commit. Every snapshot in the catalog therefore read back with
    `operation_type=None` and no parent link: an append, a compaction and a
    statistics refresh were indistinguishable after the fact, and the
    snapshot ancestry chain was never persisted at all. The asymmetry cut
    both ways — `user-created` existed only in `save_dataset_metadata`, so
    any path where `save_snapshot` ran last would erase that instead.
    `DatasetCompactor` calls only `save_dataset_metadata`, so its snapshots
    never recorded an operation type even once.

    `_snapshot_from_dict` is the reader; every key it looks for must be
    produced here. Keep the three in step.
    """
    summary = dict(snapshot.summary or {})
    for key in _SNAPSHOT_SUMMARY_KEYS:
        summary.setdefault(key, 0)

    return {
        "snapshot-id": snapshot.snapshot_id,
        "timestamp-ms": snapshot.timestamp_ms,
        "manifest": snapshot.manifest_list,
        "commit-message": getattr(snapshot, "commit_message", "") or "",
        "schema-id": getattr(snapshot, "schema_id", None),
        "summary": summary,
        "author": getattr(snapshot, "author", None),
        "sequence-number": getattr(snapshot, "sequence_number", None),
        "user-created": getattr(snapshot, "user_created", None),
        "operation-type": getattr(snapshot, "operation_type", None),
        "parent-snapshot-id": getattr(snapshot, "parent_snapshot_id", None),
    }


_PARQUET_ENGINE_HELP = (
    "opteryx-catalog needs `rugo` to read and write Parquet, and it could not be "
    "imported. Install it directly:\n"
    "    pip install 'opteryx-catalog[parquet]'   (or: pip install rugo)\n"
    "or install opteryx-core, whose wheel bundles a matching rugo:\n"
    "    pip install opteryx-core\n"
    "If rugo IS installed, check that the separately published `draken` "
    "distribution is not installed alongside it: rugo vendors its own draken and "
    "pip overwrites those files, which breaks the import at the C ABI level."
)


def _require_parquet_engine() -> None:
    """Fail fast, with install guidance, when the Parquet engine is unusable.

    rugo is an optional dependency and every import of it in this package is
    function-local, so without this check a missing (or ABI-broken) rugo only
    surfaces deep inside a write or a manifest read, as a bare ImportError from
    a module the caller has never heard of.
    """
    try:
        import rugo.parquet  # noqa: F401
    except Exception as err:  # ImportError, or a draken ABI ValueError
        raise ImportError(f"{_PARQUET_ENGINE_HELP}\n\nImport failed with: {err!r}") from err


class OpteryxCatalog(Metastore):
    """Firestore-backed Metastore implementation.

    Terminology: catalog -> workspace -> collection -> dataset|view

    Stores dataset documents under the configured workspace in Firestore.
    Snapshots are stored in a `snapshots` subcollection under each
    dataset's document. Parquet manifests are written to GCS under the
    dataset location's `metadata/manifest-<snapshot_id>.parquet` path.

    The workspace must already exist: constructing a handle for an unknown
    workspace raises `WorkspaceNotFound` rather than provisioning one, so a
    mistyped name in a query can't create an empty workspace. Provisioning is
    explicit - pass `create_if_missing=True`.
    """

    def __init__(
        self,
        workspace: str,
        firestore_project: str | None = None,
        firestore_database: str | None = None,
        gcs_bucket: str | None = None,
        io: FileIO | None = None,
        include_deleted: bool = False,
        create_if_missing: bool = False,
    ):
        # Before any Firestore work: a catalog handle whose Parquet engine is
        # missing can read metadata but fails on the first manifest touch, so
        # say so here rather than several calls later.
        _require_parquet_engine()
        # `workspace` is the configured catalog/workspace name
        self.workspace = workspace
        # Backwards-compatible alias: keep `catalog_name` for older code paths
        self.catalog_name = workspace
        self.firestore_client = firestore.Client(
            project=firestore_project, database=firestore_database
        )
        self._catalog_ref = self.firestore_client.collection(workspace)
        # Gate construction on the workspace existing. The $properties doc
        # records metadata for the workspace such as 'timestamp-ms',
        # 'author', 'billing-account-id', 'owner', and the lock fields below.
        #
        # Constructing a handle is a READ, not a provisioning step. Writing
        # $properties here for an unknown name is what makes the workspace
        # exist in Firestore (a collection is implied by its documents), so a
        # mistyped workspace name in a query used to silently conjure an empty
        # workspace. Provisioning is now opt-in via `create_if_missing`.
        #
        # `include_deleted` is now inert - kept only so existing callers
        # across other repos (billing.opteryx in particular) don't need a
        # coordinated signature change. It used to also gate construction on
        # 'deleted-at-ms', back when workspaces were soft-deleted with a
        # restore grace period; DROP WORKSPACE replaced that model with an
        # immediate, total drop (deletes this very doc, see drop_workspace())
        # and 'deleted-at-ms' is no longer written by anything. Leaving that
        # gate in blocked drop_workspace() itself from ever reaching a
        # workspace stuck with a legacy 'deleted-at-ms' from before this
        # change - the exact bug this comment replaces.
        #
        # The existence-check read is deliberately NOT under the same broad
        # `except Exception: pass` as the write below - a Firestore read
        # failure here is tolerated (conservative: don't fail catalog init on
        # transient Firestore errors, and don't claim a workspace is missing
        # when we simply couldn't look), but WorkspaceNotFound is a real
        # business-logic decision and must always propagate.
        props_doc = None
        try:
            props_ref = self._catalog_ref.document("$properties")
            props_doc = props_ref.get()
        except Exception as exc:  # noqa: BLE001 - Firestore client boundary
            # Deliberate, and explained above: a transient read failure must not
            # be reported as "workspace does not exist". `props_doc` staying None
            # routes past both of those decisions rather than guessing either.
            logger.debug("Workspace properties for %s unreadable (%s)", workspace, exc)
            props_doc = None

        if props_doc is not None:
            if not props_doc.exists:
                if not create_if_missing:
                    raise WorkspaceNotFound(f"Workspace does not exist: {workspace}")
                try:
                    now_ms = int(time.time() * 1000)
                    props_ref.set(
                        {
                            "timestamp-ms": now_ms,
                            "billing-account-id": None,
                            "owner": None,
                            "deleted-at-ms": None,
                            "deleted-by": None,
                            "locked-by": None,
                            "locked-at-ms": None,
                        }
                    )
                except Exception as exc:  # noqa: BLE001 - Firestore client boundary
                    # Be conservative: don't fail catalog initialization on Firestore errors
                    logger.debug("Could not seed $properties for %s (%s)", workspace, exc)
        self.gcs_bucket = gcs_bucket
        self._storage_client = storage.Client() if gcs_bucket else None
        # Caches for immutable, version-addressed artifacts. Snapshots, schemas
        # and data files are write-once (a new write mints a new id), so caching
        # them by id is always correct: only the dataset doc's current-*-id
        # pointers are mutable, and those are re-read on every get_relation().
        self._snapshot_cache = {}  # (collection, name, snapshot_id) -> Snapshot
        self._schema_cache = {}  # (collection, name, schema_id) -> schema dict
        # Default to a GCS-backed FileIO when a GCS bucket is configured and
        # no explicit `io` was provided.
        if io is not None:
            self.io = io
        else:
            if gcs_bucket:
                from .iops.gcs import GcsFileIO

                self.io = GcsFileIO()
            else:
                self.io = FileIO()

    def _collection_ref(self, collection: str):
        """Alias for `_namespace_ref` using the preferred term `collection`.

        Do NOT change call signatures; this helper provides a clearer name
        for new code paths while remaining backwards-compatible.
        """
        return self._catalog_ref.document(collection)

    def _datasets_collection(self, collection: str):
        # Primary subcollection for datasets.
        return self._collection_ref(collection).collection("datasets")

    def _dataset_doc_ref(self, collection: str, dataset_name: str):
        return self._datasets_collection(collection).document(dataset_name)

    def _snapshots_collection(self, collection: str, dataset_name: str):
        return self._dataset_doc_ref(collection, dataset_name).collection("snapshots")

    def _views_collection(self, collection: str):
        return self._collection_ref(collection).collection("views")

    def _view_doc_ref(self, collection: str, view_name: str):
        return self._views_collection(collection).document(view_name)

    def name_holder(self, collection: str, name: str) -> str | None:
        """Which kind of object holds `collection.name`, or None if it is free.

        `workspace.collection.<object>` is one namespace. Datasets, views and
        tasks are stored in three separate subcollections, so nothing about the
        storage prevents the same name existing in all three - this is what
        prevents it. A materialized view is a dataset carrying a type marker, so
        the dataset check covers it.
        """
        if self._dataset_doc_ref(collection, name).get().exists:
            return "dataset"
        if self._view_doc_ref(collection, name).get().exists:
            return "view"
        if self._task_doc_ref(collection, name).get().exists:
            return "task"
        return None

    def assert_name_free(self, collection: str, name: str, kind: str) -> None:
        """Refuse `collection.name` if another KIND of object already holds it.

        The same-kind case is left to each creator, which knows whether it is a
        replace: this answers only "is this name someone else's".
        """
        holder = self.name_holder(collection, name)
        if holder is not None and holder != kind:
            raise NameInUse(
                f"{collection}.{name} already exists as a {holder}. A table, a view "
                "and a task share one namespace, so a name identifies exactly one of "
                "them - drop the existing object or choose another name."
            )

    def _task_parts(self, identifier: str | tuple) -> tuple[str, str]:
        """(collection, task) for a task name, qualified or not.

        Accepts `<collection>.<task>` and `<workspace>.<collection>.<task>`
        alike, because both spellings genuinely occur: a trigger records its
        target FULLY qualified, so the firing path hands one in with the
        workspace still attached, while a caller registering a task writes the
        short form. Both must resolve to the same document.

        Hand-rolling `identifier.split(".")` here instead is what made every
        trigger-fired run raise `TaskNotFound: opteryx.ops.compaction_log_ingest`
        in production - the name was right, the parser was not.
        """
        if isinstance(identifier, (tuple, list)):
            return identifier[0], identifier[1]
        return self._local_parts(identifier)

    def _tasks_collection(self, collection: str):
        return self._collection_ref(collection).collection(TASKS_SUBCOLLECTION)

    def _task_doc_ref(self, collection: str, task_name: str):
        return self._tasks_collection(collection).document(task_name)

    @staticmethod
    def _delete_subcollection(coll_ref) -> None:
        """Delete every document in a subcollection.

        Firestore does not cascade: deleting a document leaves its subcollections
        addressable but unreachable, so each one must be emptied explicitly.
        """
        for doc in coll_ref.stream():
            coll_ref.document(doc.id).delete()

    def create_dataset(
        self,
        identifier: str,
        schema: Any,
        properties: dict | None = None,
        author: str | None = None,
    ) -> SimpleDataset:
        if author is None:
            raise ValueError("author must be provided when creating a dataset")
        collection, dataset_name = identifier.split(".")
        doc_ref = self._dataset_doc_ref(collection, dataset_name)
        # Check primary `datasets` location
        if doc_ref.get().exists:
            raise DatasetAlreadyExists(f"Dataset already exists: {identifier}")
        # ... and the rest of the namespace, which is shared with views and tasks.
        self.assert_name_free(collection, dataset_name, "dataset")

        # Build default dataset metadata
        location = f"gs://{self.gcs_bucket}/{self.workspace}/{collection}/{dataset_name}"
        metadata = DatasetMetadata(
            dataset_identifier=identifier,
            schema=schema,
            location=location,
            properties=properties or {},
        )

        # Allocate stable, never-reused field-ids for the initial schema's columns
        # up front, so they can be persisted alongside the dataset doc in the same
        # write as the rest of its metadata.
        field_ids = None
        if schema is not None:
            column_count = len(self._schema_to_columns(schema))
            field_ids = list(range(1, column_count + 1))
            metadata.next_field_id = column_count + 1

        # Persist document with timestamp and author
        now_ms = int(time.time() * 1000)
        metadata.timestamp_ms = now_ms
        metadata.author = author
        doc_ref.set(
            {
                "name": dataset_name,
                "collection": collection,
                "workspace": self.workspace,
                "location": location,
                "properties": metadata.properties,
                "format-version": metadata.format_version,
                "timestamp-ms": now_ms,
                "author": author,
                "maintenance-policy": metadata.maintenance_policy,
                "annotations": metadata.annotations,
                "refresh-frequency-mins": None,
                "next-field-id": metadata.next_field_id,
                "locked-by": None,
                "locked-at-ms": None,
            }
        )

        # Persist initial schema into `schemas` subcollection if provided
        if schema is not None:
            schema_id = self._write_schema(
                collection, dataset_name, schema, author=author, field_ids=field_ids
            )
            metadata.current_schema_id = schema_id
            # Read back the schema doc to capture timestamp-ms, author, sequence-number
            try:
                sdoc = doc_ref.collection("schemas").document(schema_id).get()
                sdata = sdoc.to_dict() or {}
                metadata.schemas = [
                    {
                        "schema_id": schema_id,
                        "columns": sdata.get(
                            "columns", self._schema_to_columns(schema, field_ids=field_ids)
                        ),
                        "timestamp-ms": sdata.get("timestamp-ms"),
                        "author": sdata.get("author"),
                        "sequence-number": sdata.get("sequence-number"),
                    }
                ]
            except Exception as exc:  # noqa: BLE001 - read-back is an optimisation
                # The schema document is already written; this only reads it back to
                # mirror its server-side fields into the in-memory metadata. Rebuilding
                # the column list locally loses the timestamp and sequence number, not
                # the schema.
                logger.debug(
                    "Schema read-back for %s.%s failed (%s)", collection, dataset_name, exc
                )
                metadata.schemas = [
                    {
                        "schema_id": schema_id,
                        "columns": self._schema_to_columns(schema, field_ids=field_ids),
                    }
                ]
            # update dataset doc to reference current schema
            doc_ref.update({"current-schema-id": metadata.current_schema_id})

        # Send webhook notification
        send_webhook(
            action="create",
            workspace=self.workspace,
            collection=collection,
            resource_type=ResourceType.DATASET,
            resource_name=dataset_name,
            payload=dataset_created_payload(
                schema=schema,
                location=location,
                properties=properties,
            ),
        )

        emit_audit(
            "create_dataset",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            location=location,
        )

        # Return SimpleDataset (attach this catalog so append() can persist)
        return SimpleDataset(identifier=identifier, _metadata=metadata, io=self.io, catalog=self)

    def load_dataset(self, identifier: str, load_history: bool = False) -> SimpleDataset:
        """Load a dataset from Firestore.

        Args:
            identifier: Dataset identifier in format 'collection.dataset_name'
                (dataset_name can contain dots, e.g. 'public.github.events')
            load_history: If True, load all snapshots from Firestore (expensive for
                large histories). If False (default), only load the current snapshot,
                which is sufficient for most write operations.

        Returns:
            SimpleDataset instance with metadata loaded from Firestore.

        Raises:
            DatasetNotFound: If the dataset does not exist in Firestore.
        """
        collection, dataset_name = identifier.split(".", 1)
        doc = self._dataset_doc_ref(collection, dataset_name).get()
        if not doc.exists:
            raise DatasetNotFound(f"Dataset not found: {identifier}")
        return self._build_dataset(identifier, collection, dataset_name, doc, load_history)

    def get_relation(self, identifier):
        """Catalog resolution step: resolve a relation without knowing whether
        it is a dataset or a view, in a single ``get_all([dataset, view])``
        round trip. Returns ``(kind, obj)`` with kind in
        ``{"dataset", "view", None}``.

        The mutable dataset doc is read here on every call (it is the version
        pointer), so there is no staleness. The immutable snapshot/schema docs
        reached while building a dataset are served from id-keyed caches.
        """
        if isinstance(identifier, (tuple, list)):
            ident = ".".join(str(p) for p in identifier)
        else:
            ident = identifier
        # Dataset names may contain dots (split on the first); views are
        # addressed name-last. The two coincide for the common two-part name.
        ds_collection, ds_name = ident.split(".", 1) if "." in ident else (ident, ident)
        parts = ident.split(".")
        vw_collection = ".".join(parts[:-1]) or ds_collection
        vw_name = parts[-1]

        ds_ref = self._dataset_doc_ref(ds_collection, ds_name)
        vw_ref = self._view_doc_ref(vw_collection, vw_name)

        docs_by_path = {}
        try:
            for d in self.firestore_client.get_all([ds_ref, vw_ref]):
                ref = getattr(d, "reference", None)
                if ref is not None:
                    docs_by_path[ref.path] = d
        except Exception as exc:  # noqa: BLE001 - Firestore client boundary
            # `get_all` is a batched read of a dataset doc and a view doc, used to
            # answer "which kind of thing is this?" in one round trip. An empty
            # map falls through to the per-document lookups below.
            logger.debug("Batched existence lookup failed (%s)", exc)
            docs_by_path = {}

        ds_doc = docs_by_path.get(ds_ref.path)
        vw_doc = docs_by_path.get(vw_ref.path)

        if ds_doc is not None and ds_doc.exists:
            return "dataset", self._build_dataset(ident, ds_collection, ds_name, ds_doc)
        if vw_doc is not None and vw_doc.exists:
            return "view", self._build_view(vw_collection, vw_name, vw_doc)
        return None, None

    def _snapshot_from_dict(self, sd: dict) -> Snapshot:
        return Snapshot(
            snapshot_id=sd.get("snapshot-id"),
            timestamp_ms=sd.get("timestamp-ms"),
            author=sd.get("author"),
            sequence_number=sd.get("sequence-number"),
            user_created=sd.get("user-created"),
            manifest_list=sd.get("manifest"),
            schema_id=sd.get("schema-id"),
            summary=sd.get("summary", {}),
            operation_type=sd.get("operation-type"),
            parent_snapshot_id=sd.get("parent-snapshot-id"),
            commit_message=sd.get("commit-message"),
        )

    def _load_tags(self, collection: str, dataset_name: str) -> tuple[dict[str, int], bool]:
        """Read the `tags` subcollection into a {name: snapshot_id} map.

        Returns `(tags, loaded)`. `loaded` is False when the subcollection
        could not be streamed - the caller must NOT read that as "no tags".
        A tag is a retention pin, so an unreadable tag set has the same shape
        of consequence as an unreadable manifest: the protected set comes back
        short and the pinned data looks reclaimable. See
        `DatasetMetadata.tags_loaded`.

        A tag document with no `snapshot-id` is skipped rather than admitted
        with a null target: it pins nothing and would resolve to nothing.
        """
        tags: dict[str, int] = {}
        try:
            # Read by (collection, dataset_name) rather than through
            # `list_tags`, which re-parses an identifier we have already split.
            for tag_doc in self._tags_collection(collection, dataset_name).stream():
                snapshot_id = (tag_doc.to_dict() or {}).get("snapshot-id")
                if snapshot_id is None:
                    logger.error(
                        "Tag %s of %s.%s names no snapshot; ignoring",
                        tag_doc.id,
                        collection,
                        dataset_name,
                    )
                    continue
                tags[str(tag_doc.id).lower()] = int(snapshot_id)
        except Exception as exc:  # noqa: BLE001 - Firestore client boundary
            # Broad on purpose, and reported rather than raised: reads must
            # still work. What must not happen is a caller treating the empty
            # map as fact, which the False guards against.
            logger.error("Could not read tags for %s.%s: %s", collection, dataset_name, exc)
            return {}, False
        return tags, True

    def _schema_entry_from_doc(self, sdoc) -> dict:
        sd = sdoc.to_dict() or {}
        return {
            "schema_id": sdoc.id,
            "columns": sd.get("columns", []),
            "timestamp-ms": sd.get("timestamp-ms"),
            "author": sd.get("author"),
            "sequence-number": sd.get("sequence-number"),
        }

    def _build_dataset(
        self, identifier, collection, dataset_name, doc, load_history: bool = False
    ) -> SimpleDataset:
        """Build a SimpleDataset from an already-fetched dataset doc.

        Non-history path resolves the current snapshot + schema, preferring the
        id-keyed caches (immutable artifacts) and batching any misses into one
        get_all().
        """
        data = doc.to_dict() or {}
        metadata = DatasetMetadata(
            dataset_identifier=identifier,
            location=data.get("location")
            or f"gs://{self.gcs_bucket}/{self.workspace}/{collection}/{dataset_name}",
            schema=data.get("schema"),
            properties=data.get("properties") or {},
        )
        metadata.timestamp_ms = data.get("timestamp-ms")
        metadata.author = data.get("author")
        metadata.description = data.get("description")
        metadata.describer = data.get("describer")
        metadata.annotations = data.get("annotations") or []
        metadata.refresh_frequency_mins = data.get("refresh-frequency-mins")
        metadata.next_field_id = data.get("next-field-id", 1)
        # Load the configured sort order. Without this the value round-tripped
        # by save_dataset_metadata is silently dropped on read, so DatasetCompactor
        # always sees an empty sort_orders and falls back to the (non-locality-
        # preserving) brute strategy — i.e. order-aware compaction never runs.
        metadata.sort_orders = data.get("sort-orders") or []
        # Load the configured maintenance policy. Same failure mode as
        # sort-orders above: save_dataset_metadata writes 'maintenance-policy'
        # but nothing read it back, so every dataset presented the class
        # default of {'retained-snapshot-age-days': None} - which SnapshotExpiry
        # reads as "keep only the current snapshot". Retention was therefore
        # ignored everywhere, and every expiration run condemned the entire
        # history regardless of what was configured.
        stored_maintenance_policy = data.get("maintenance-policy")
        if stored_maintenance_policy:
            metadata.maintenance_policy = stored_maintenance_policy
        # Absent on plain datasets; "materialized_view" on an MV's backing
        # table. Read back so callers can distinguish the two from a dataset
        # they already hold, without a second catalog round trip - and, with
        # the registration fields below, so a commit round-trips them instead
        # of destroying them (see DatasetMetadata).
        metadata.dataset_type = data.get("dataset-type")
        metadata.statement_id = data.get("statement-id")
        metadata.source_tables = data.get("source-tables") or []
        metadata.runs_as = data.get("runs-as")
        metadata.suspended_at_ms = data.get("suspended-at-ms")
        metadata.suspended_by = data.get("suspended-by")
        metadata.last_refreshed_at_ms = data.get("last-refreshed-at-ms")
        metadata.last_refresh_status = data.get("last-refresh-status")
        metadata.last_refresh_execution_id = data.get("last-refresh-execution-id")

        schemas_coll = self._dataset_doc_ref(collection, dataset_name).collection("schemas")

        if load_history:
            snaps = []
            for snap_doc in self._snapshots_collection(collection, dataset_name).stream():
                snap_data = snap_doc.to_dict() or {}
                # Tombstoned snapshots stay out of every normal read. They are
                # records for the restore path, not history: if they appeared
                # here, expiration would re-condemn them each run, the orphan-
                # detection threshold would count them (a 15-minutely dataset
                # accrues ~2,880 tombstones per 30-day window - enough on its
                # own to trip MAX_SNAPSHOTS_FOR_ORPHAN_DETECTION and silently
                # disable orphan cleanup), and their manifests would count as
                # referenced, pinning the very files expiration just released.
                if snapshot_is_tombstoned(snap_data):
                    continue
                snaps.append(self._snapshot_from_dict(snap_data))
            # The HEAD comes from the dataset document's pointer (stored as
            # `current-snapshot-id` - see
            # DatasetMetadata.current_snapshot_id), NOT from the tail of this
            # list. The stream arrives in Firestore document-id order, which is
            # LEXICOGRAPHIC on the id string and only accidentally chronological;
            # and after a `rollback` the head is deliberately not the newest
            # snapshot at all. Taking `snaps[-1]` would silently roll a rolled-
            # back dataset forward again the first time anything loaded its
            # history - expiration does, on every run.
            stored_head = data.get("current-snapshot-id")
            if stored_head is not None and any(s.snapshot_id == stored_head for s in snaps):
                metadata.current_snapshot_id = stored_head
            elif snaps:
                # No pointer recorded (a dataset older than the pointer), or one
                # naming a snapshot that has expired out of this list. Fall back
                # to the newest by sequence number, which IS chronological.
                metadata.current_snapshot_id = max(
                    snaps, key=lambda s: (s.sequence_number or 0, s.snapshot_id or 0)
                ).snapshot_id
            metadata.snapshots = snaps
            metadata.schemas = [self._schema_entry_from_doc(s) for s in schemas_coll.stream()]
            metadata.current_schema_id = data.get("current-schema-id")
            metadata.tags, metadata.tags_loaded = self._load_tags(collection, dataset_name)
            return SimpleDataset(
                identifier=identifier, _metadata=metadata, io=self.io, catalog=self
            )

        # The non-history path deliberately does NOT fetch tags: it exists to be
        # cheap, and every write path uses it. `tags_loaded` stays at its False
        # default, which is what makes a caller that needs the pins go and read
        # them rather than conclude from an empty map that nothing is pinned.

        # `current-snapshot-id` is the stored key for the head pointer, and it
        # matches the field name. See DatasetMetadata.current_snapshot_id.
        current_snap_id = data.get("current-snapshot-id")
        current_schema_id = data.get("current-schema-id")

        # Prefer the immutable id-keyed caches; batch any misses into one read.
        snap_obj = (
            self._snapshot_cache.get((collection, dataset_name, current_snap_id))
            if current_snap_id
            else None
        )
        schema_entry = (
            self._schema_cache.get((collection, dataset_name, current_schema_id))
            if current_schema_id
            else None
        )

        snap_ref = schema_ref = None
        refs = []
        if current_snap_id and snap_obj is None:
            snap_ref = self._snapshots_collection(collection, dataset_name).document(
                str(current_snap_id)
            )
            refs.append(snap_ref)
        if current_schema_id and schema_entry is None:
            schema_ref = schemas_coll.document(str(current_schema_id))
            refs.append(schema_ref)

        docs_by_path = {}
        if refs:
            try:
                for d in self.firestore_client.get_all(refs):
                    ref = getattr(d, "reference", None)
                    if ref is not None:
                        docs_by_path[ref.path] = d
            except Exception as exc:  # noqa: BLE001 - Firestore client boundary, see above
                logger.debug("Batched snapshot/schema lookup failed (%s)", exc)
                docs_by_path = {}

        snaps = []
        if snap_obj is None and snap_ref is not None:
            sdoc = docs_by_path.get(snap_ref.path)
            if sdoc is not None and sdoc.exists:
                snap_obj = self._snapshot_from_dict(sdoc.to_dict() or {})
                self._snapshot_cache[(collection, dataset_name, current_snap_id)] = snap_obj
        if snap_obj is not None:
            snaps.append(snap_obj)
            metadata.current_snapshot_id = current_snap_id
        elif current_snap_id:
            # The dataset names a current snapshot whose document we could not
            # resolve. The metastore-side analogue of a manifest 404: the
            # dataset loads as empty rather than failing, so every reader sees a
            # table with no data and no error, and garbage collection sees
            # nothing to protect.
            #
            # Reported, not raised: making this fatal would change what
            # `load_dataset` returns to callers who currently get an empty
            # dataset, which needs its own change.
            _alert(
                SnapshotMissingError(
                    f"Dataset {identifier} names current-snapshot-id {current_snap_id} "
                    "but that snapshot document could not be resolved; the dataset "
                    "will load as empty."
                ),
                fingerprint=("snapshot-missing", identifier),
                context={"dataset": identifier, "current_snapshot_id": current_snap_id},
            )
        metadata.snapshots = snaps

        if schema_entry is None and schema_ref is not None:
            scdoc = docs_by_path.get(schema_ref.path)
            if scdoc is not None and scdoc.exists:
                schema_entry = self._schema_entry_from_doc(scdoc)
                self._schema_cache[(collection, dataset_name, current_schema_id)] = schema_entry
        if schema_entry is not None:
            metadata.schemas = [schema_entry]
            metadata.current_schema_id = current_schema_id

        return SimpleDataset(identifier=identifier, _metadata=metadata, io=self.io, catalog=self)

    def drop_dataset(self, identifier: str, author: str | None = None) -> None:
        """Drop a dataset. Its storage is reclaimed later, by reconciliation.

        Dropping removes the dataset from the catalog immediately; the files
        under its location are left in place and become unreferenced storage.
        Nothing records where they were, deliberately. A dropped dataset's
        location is derived from its name, so any persisted note of it is a
        claim about a prefix that `create_dataset` can silently re-bind to a
        new dataset - which is exactly how a stale note once pointed at live
        data. Reclamation instead re-derives the answer from ground truth:
        storage that no live dataset claims is orphaned, found by comparing
        what is physically in the bucket against the locations the catalog
        currently holds.

        Raises `DatasetLocked` if the dataset's `locked-by` field is set -
        the two-person deniability lock takes precedence over the drop.

        The workspace's `deletion_protection` does NOT apply here: it protects the
        workspace from being deleted, not the assets inside it. Per-asset
        protection is `locked-by`.

        An author is required, as it is to create one. The check is up front,
        before the does-it-exist return: a caller that forgot must fail the same
        way whether or not the dataset happened to be there, or the omission
        only surfaces on the runs that actually drop something. Empty string is
        rejected alongside None - callers resolve the author with
        `session_user or DEFAULT`, and "" survives that to attribute the drop to
        nobody just as effectively.
        """
        if not author:
            raise ValueError("author must be provided when dropping a dataset")
        collection, dataset_name = identifier.split(".")
        doc_ref = self._dataset_doc_ref(collection, dataset_name)
        doc = doc_ref.get()
        if not doc.exists:
            # Nothing to drop, so nothing to reclaim and nothing to announce.
            return

        data = doc.to_dict() or {}
        if data.get("locked-by") is not None:
            raise DatasetLocked(f"Dataset is locked: {identifier}")

        # Refuse to drop a dataset that materialized views read. Their triggers
        # live in THIS dataset's subcollection, so the drop would take them with
        # it and leave each dependent view refreshing on whatever sources
        # remain - silently producing partial data, or silently never
        # refreshing again if this was the last one. Consistent with
        # `rename_dataset`, which refuses for the same reason.
        dependents = self._materialized_views_reading(identifier)
        if dependents:
            raise MaterializedViewError(
                f"Cannot drop {identifier}: it is a source of materialized view(s) "
                f"{', '.join(sorted(dependents))}. Drop those first."
            )

        location = data.get("location")

        # A dropped dataset takes its own triggers with it. If it is itself a
        # materialized view, its refresh triggers live on *other* datasets and
        # must be chased down too - Firestore cannot cascade across documents
        # any more than it can into subcollections.
        if data.get("dataset-type") == MATERIALIZED_VIEW_TYPE:
            trigger_name = self._mv_trigger_name(f"{self.workspace}.{identifier}")
            for source in data.get("source-tables") or []:
                self.drop_trigger(source, trigger_name, author=author, missing_ok=True)

        # A dropped dataset takes its OWN relationships with it - they are its
        # metadata and they go with the subcollection below. What does not go,
        # because Firestore cascades nothing, is a relationship declared on some
        # OTHER dataset that points AT this one. Those are now references to
        # something that does not exist, and they are marked broken rather than
        # deleted: the row is the only remaining record that this dataset was
        # ever depended on, and whoever asserted it is entitled to find out
        # from their own catalog rather than from a query that returns nothing.
        #
        # One collection group query, the same one the projection makes. The
        # design's reason for punting this - that it would mean scanning every
        # dataset in the workspace - has been superseded by that query existing.
        # Non-fatal, and deliberately so. The drop has been decided by this
        # point; failing it because a METADATA sweep could not run would leave
        # the caller unable to drop a dataset at all whenever the collection
        # group index is unavailable. What the sweep misses is an inbound row
        # pointing at something that no longer exists - which is precisely the
        # shape `integrity.py` already sweeps for, and a far smaller problem
        # than an undroppable dataset. Same reasoning as `_reclaim_paths` in
        # `rename_dataset`, and it is logged rather than swallowed.
        try:
            self._break_inbound_relationships(
                collection,
                dataset_name,
                reason="dataset-dropped",
                detail=f"dataset {collection}.{dataset_name} was dropped",
                author=author,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "could not break inbound relationships for %s.%s: %s: %s - they now "
                "reference a dropped dataset and will be found by fsck",
                collection,
                dataset_name,
                type(exc).__name__,
                exc,
            )

        self._delete_subcollection(self._snapshots_collection(collection, dataset_name))
        self._delete_subcollection(self._tags_collection(collection, dataset_name))
        self._delete_subcollection(doc_ref.collection("schemas"))
        self._delete_subcollection(doc_ref.collection(MAINTENANCE_SUBCOLLECTION))
        self._delete_subcollection(doc_ref.collection("statement"))
        self._delete_subcollection(doc_ref.collection(TRIGGERS_SUBCOLLECTION))
        self._delete_subcollection(doc_ref.collection(RELATIONSHIPS_SUBCOLLECTION))
        self._delete_subcollection(
            doc_ref.collection(RELATIONSHIP_SUPPRESSIONS_SUBCOLLECTION)
        )
        doc_ref.delete()

        send_webhook(
            action="delete",
            workspace=self.workspace,
            collection=collection,
            resource_type=ResourceType.DATASET,
            resource_name=dataset_name,
            payload=dataset_deleted_payload(location=location, dropped_by=author),
        )

        emit_audit(
            "drop_dataset",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            location=location,
        )

    def _blob_name(self, path: str) -> str:
        """Strip the `gs://<bucket>/` prefix off a path, leaving the blob name."""
        prefix = f"gs://{self.gcs_bucket}/"
        if path.startswith(prefix):
            return path[len(prefix) :]
        return path

    def _copy_object(self, source_path: str, target_path: str) -> None:
        """Copy one object, preferring a server-side copy.

        `copy_blob` is server-side: the bytes never travel through this process,
        which is what makes moving a large dataset merely slow rather than
        impossible. The FileIO read/write path is the fallback for a catalog
        with no GCS bucket configured (local/test FileIO).
        """
        if self._storage_client is not None and self.gcs_bucket:
            bucket = self._storage_client.bucket(self.gcs_bucket)
            bucket.copy_blob(
                bucket.blob(self._blob_name(source_path)),
                bucket,
                self._blob_name(target_path),
            )
            return

        with self.io.new_input(source_path).open() as handle:
            data = handle.read()
        out = self.io.new_output(target_path).create()
        out.write(data)
        out.close()

    def _read_bytes(self, path: str) -> bytes:
        with self.io.new_input(path).open() as handle:
            return handle.read()

    def _write_bytes(self, path: str, data: bytes) -> None:
        out = self.io.new_output(path).create()
        out.write(data)
        out.close()

    def rename_dataset(
        self, identifier: str, new_identifier: str, author: str | None = None
    ) -> None:
        """Rename a dataset, moving its files, manifests and catalog entry.

        A dataset's `location` is assigned once at creation and never re-derived
        from its name, so a catalog-only rename would leave the files under the
        old prefix - and creating a new dataset under the vacated name would
        then derive that same prefix, putting two datasets on one location where
        dropping either reclaims the other's files. This moves everything so
        name and storage stay in step and no prefix is ever shared.

        All snapshots move, not just the current one: every historical manifest
        is rewritten at the new location with remapped file paths, so
        time-travel keeps working across a rename.

        Cost: this copies every data file the dataset references. It is
        server-side (see `_copy_object`) but still O(all bytes) and O(all
        objects) - a rename of a large dataset is a long-running job, not the
        instant metadata edit the SQL makes it look like.

        Ordering, since Firestore and GCS share no transaction:
        copy files -> rewrite manifests -> write new catalog entry -> delete old
        entry -> delete the files the copy superseded. A failure before the new
        entry is written leaves the dataset untouched and some orphan copies at
        the new location (a re-run overwrites them). A failure after it leaves
        the old entry present alongside the new one, both readable, until
        re-run. No ordering here loses data; the superseded files are deleted
        by exact path, never by clearing the old prefix, so a re-run that
        already moved them simply finds nothing left to delete.

        Files the dataset references from *outside* its own location are left
        exactly where they are and referenced unchanged - they were never this
        dataset's to move.

        Args:
            identifier: Current identifier, 'collection.dataset'
            new_identifier: New identifier, 'collection.dataset'. May name a
                different collection; the workspace is always this catalog's.
            author: The identity making the change - None when unauthenticated,
                never substituted (see audit.emit_audit).

        Raises:
            DatasetNotFound: If the source does not exist.
            DatasetAlreadyExists: If the target already exists.
            DatasetLocked: If the source's `locked-by` field is set.
            ValueError: If source and target are the same.
        """
        if identifier == new_identifier:
            raise ValueError(f"rename source and target are the same: {identifier}")

        collection, dataset_name = identifier.split(".")
        new_collection, new_dataset_name = new_identifier.split(".")

        doc_ref = self._dataset_doc_ref(collection, dataset_name)
        doc = doc_ref.get()
        if not doc.exists:
            raise DatasetNotFound(f"Dataset not found: {identifier}")

        data = doc.to_dict() or {}
        # Same precedence as drop_dataset - the two-person deniability lock
        # outranks any operation that would move the dataset out from under it.
        if data.get("locked-by") is not None:
            raise DatasetLocked(f"Dataset is locked: {identifier}")

        # v1: a materialized view, or any dataset wearing triggers, cannot be
        # renamed. Trigger documents and MV source lists reference names, and
        # chasing that reference graph through a rename is not worth it yet -
        # drop the MV (or its triggers) first, recreate under the new name.
        if data.get("dataset-type") == MATERIALIZED_VIEW_TYPE:
            raise MaterializedViewError(f"Cannot rename a materialized view: {identifier}")
        if any(True for _ in self._triggers_collection(collection, dataset_name).stream()):
            raise MaterializedViewError(
                f"Cannot rename a dataset with triggers attached: {identifier}"
            )

        new_doc_ref = self._dataset_doc_ref(new_collection, new_dataset_name)
        if new_doc_ref.get().exists:
            raise DatasetAlreadyExists(f"Dataset already exists: {new_identifier}")

        old_location = data.get("location")
        new_location = (
            f"gs://{self.gcs_bucket}/{self.workspace}/{new_collection}/{new_dataset_name}"
        )

        def _remap(path: str) -> str:
            """Move a path from under old_location to under new_location.

            Anything not under old_location is returned unchanged - see the
            note about externally-referenced files above.
            """
            if old_location and path.startswith(old_location + "/"):
                return new_location + path[len(old_location) :]
            return path

        from .catalog.manifest import read_manifest_rows

        snapshot_docs = list(self._snapshots_collection(collection, dataset_name).stream())

        # 1. Copy data files, and 2. rewrite manifests. One pass per snapshot;
        # a file referenced by several snapshots is copied once (copied_paths).
        copied_paths = set()
        # Paths left behind under `old_location` once the copies land. Collected
        # as they are copied, rather than by listing the old prefix afterwards:
        # these exact paths are known to be superseded, whereas a listing would
        # also sweep up anything written under that prefix concurrently.
        vacated_paths = set()
        rewritten_manifests = {}
        for snapshot_doc in snapshot_docs:
            snapshot_data = snapshot_doc.to_dict() or {}
            manifest_path = snapshot_data.get("manifest")
            if not manifest_path:
                continue

            manifest_bytes = self._read_bytes(manifest_path)
            rows = read_manifest_rows(manifest_bytes)
            if old_location and manifest_path.startswith(old_location + "/"):
                vacated_paths.add(manifest_path)

            for row in rows:
                source_path = row.get("file_path")
                if not source_path:
                    continue
                target_path = _remap(source_path)
                if target_path == source_path or target_path in copied_paths:
                    continue
                self._copy_object(source_path, target_path)
                copied_paths.add(target_path)
                vacated_paths.add(source_path)
                row["file_path"] = target_path

            snapshot_id = snapshot_data.get("snapshot-id")
            new_manifest_path = self.write_parquet_manifest(snapshot_id, rows, new_location)
            rewritten_manifests[snapshot_doc.id] = (snapshot_data, new_manifest_path)

        # 3. Write the new catalog entry: dataset doc, schemas, snapshots. This
        # is the point the rename becomes visible under the new name.
        new_doc_ref.set(
            {
                **data,
                "name": new_dataset_name,
                "collection": new_collection,
                "location": new_location,
                "timestamp-ms": int(time.time() * 1000),
            }
        )

        for schema_doc in doc_ref.collection("schemas").stream():
            new_doc_ref.collection("schemas").document(schema_doc.id).set(
                schema_doc.to_dict() or {}
            )

        # Tags travel with the dataset. They name snapshot ids, and the ids are
        # preserved by the copy below, so the documents move verbatim - but they
        # MUST move: a rename that left them behind would unpin every tagged
        # snapshot silently, and the next expiration run would reclaim exactly
        # the data the tags exist to hold.
        new_tags = self._tags_collection(new_collection, new_dataset_name)
        for tag_doc in self._tags_collection(collection, dataset_name).stream():
            new_tags.document(tag_doc.id).set(tag_doc.to_dict() or {})

        # Relationships travel with the dataset for the same reason tags do:
        # they are its own metadata, keyed under it, and a rename that left them
        # behind would delete every relationship declared on it with nothing
        # saying so.
        #
        # A relationship declared on some OTHER dataset that points AT this one
        # is FOLLOWED, below. It used to be left stale on purpose, on the
        # grounds that chasing inbound references would mean scanning every
        # dataset in the workspace and that a stale reference is what
        # `status = 'broken'` is for. THAT REASONING IS SUPERSEDED:
        # `find_relationships_to` answers "what points at this dataset" in one
        # collection group query - the same read `information_schema` already
        # makes - so the punt now costs more than the fix.
        new_relationships = self._relationships_collection(new_collection, new_dataset_name)
        for relationship_doc in self._relationships_collection(collection, dataset_name).stream():
            payload = relationship_doc.to_dict() or {}
            # The near address is denormalised onto every row, so a rename that
            # copied the documents verbatim would leave each one claiming to
            # belong to the dataset that no longer exists - and the reverse
            # lookup and the workspace-wide query both read exactly these
            # fields rather than the path.
            payload["collection"] = new_collection
            payload["dataset"] = new_dataset_name
            new_relationships.document(relationship_doc.id).set(payload)

        # Rejections travel too, and for a sharper reason than tags do: a
        # rename that dropped them would have inference re-propose every pair
        # this dataset's owner has already turned down. The digest keys on the
        # near COLUMN and the far end, neither of which a rename changes, so the
        # documents move under their own ids.
        new_suppressions = self._relationship_suppressions_collection(
            new_collection, new_dataset_name
        )
        for suppression_doc in self._relationship_suppressions_collection(
            collection, dataset_name
        ).stream():
            payload = suppression_doc.to_dict() or {}
            payload["collection"] = new_collection
            payload["dataset"] = new_dataset_name
            new_suppressions.document(suppression_doc.id).set(payload)

        # Inbound references follow the rename rather than breaking. A renamed
        # dataset is the SAME dataset - its snapshots, tags and relationships
        # all travelled above - so a reference to it still names the data it
        # meant. Breaking these would report a data problem where there is
        # none, and an owner who "fixed" it by dropping the constraint would
        # lose a true relationship to a bookkeeping event. A drop is the
        # opposite case and is handled as one.
        # Non-fatal, for a sharper reason than in `drop_dataset`: by this point
        # the rename has already copied every byte to the new location, and
        # raising here would abort it halfway - files at the new prefix, the
        # catalog entry still at the old one. An inbound row left pointing at
        # the old name is a stale reference, which is the state this whole
        # sweep is an improvement on, and `fsck` finds it.
        try:
            self._repoint_inbound_relationships(
                collection,
                dataset_name,
                new_collection,
                new_dataset_name,
                author=author,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "could not repoint inbound relationships from %s.%s to %s.%s: %s: %s - "
                "they still name the old dataset and will be found by fsck",
                collection,
                dataset_name,
                new_collection,
                new_dataset_name,
                type(exc).__name__,
                exc,
            )

        new_snapshots = self._snapshots_collection(new_collection, new_dataset_name)
        for snapshot_doc in snapshot_docs:
            snapshot_data, new_manifest_path = rewritten_manifests.get(
                snapshot_doc.id, (snapshot_doc.to_dict() or {}, None)
            )
            payload = dict(snapshot_data)
            if new_manifest_path is not None:
                payload["manifest"] = new_manifest_path
            new_snapshots.document(snapshot_doc.id).set(payload)

        # 4. Remove the old catalog entry. The orphan quarantine is dropped
        # rather than carried over: its entries name paths under the old
        # location, which no longer exist, so the renamed dataset starts with a
        # clean record and its files each need two fresh sightings.
        self._delete_subcollection(self._snapshots_collection(collection, dataset_name))
        self._delete_subcollection(self._tags_collection(collection, dataset_name))
        self._delete_subcollection(doc_ref.collection("schemas"))
        self._delete_subcollection(doc_ref.collection(MAINTENANCE_SUBCOLLECTION))
        self._delete_subcollection(doc_ref.collection(RELATIONSHIPS_SUBCOLLECTION))
        self._delete_subcollection(
            doc_ref.collection(RELATIONSHIP_SUPPRESSIONS_SUBCOLLECTION)
        )
        doc_ref.delete()

        # 5. Delete what the copy vacated. Done inline, and by exact path:
        # every one of these was copied to `new_location` moments ago and is
        # now a duplicate, so there is nothing to defer and no grace period to
        # wait out. Deleting the old *prefix* instead would be the unsafe
        # version - it can be re-bound to a new dataset of the same name - and
        # is precisely what this no longer does. Failures are left alone rather
        # than raised: the rename itself has fully succeeded by this point, and
        # anything not deleted here is unreferenced storage that reconciliation
        # finds later.
        self._reclaim_paths(vacated_paths)

        send_webhook(
            action="rename",
            workspace=self.workspace,
            collection=new_collection,
            resource_type=ResourceType.DATASET,
            resource_name=new_dataset_name,
            payload=dataset_renamed_payload(
                old_identifier=identifier,
                new_identifier=new_identifier,
                old_location=old_location,
                new_location=new_location,
                renamed_by=author,
            ),
        )

        emit_audit(
            "rename_dataset",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=new_collection,
            resource=new_dataset_name,
            author=author,
            old_identifier=identifier,
            new_identifier=new_identifier,
            old_location=old_location,
            new_location=new_location,
            files_copied=len(copied_paths),
            snapshots_moved=len(snapshot_docs),
        )

    def _reclaim_paths(self, paths) -> dict:
        """Delete an explicit set of storage paths, reporting rather than raising.

        Takes the paths themselves, never a prefix to expand. Callers know
        exactly which objects they have superseded; re-deriving that set from a
        listing would also catch anything written under the same prefix in the
        meantime, which is the failure mode this codebase is deliberately out
        of the business of.
        """
        deleted = 0
        failed = 0
        for path in sorted(paths):
            try:
                self.io.delete(path)
                deleted += 1
            except (AttributeError, ValueError, OSError) as exc:
                logger.error("Could not delete %s: %s", path, exc)
                failed += 1
        return {"files_deleted": deleted, "files_failed": failed}

    # --- Workspace lifecycle (drop / lock) ------------------------------
    #
    # These methods execute the state change and record who asked - they do
    # NOT enforce identity rules (e.g. "a different owner must unlock").
    # That authorization decision lives in the calling engine
    # (opteryx.managers.permissions.can_perform_workspace_action, via
    # opteryx-access's ACTION_ROLES), which originates both the decision and
    # the call with nothing in between - see DROP WORKSPACE's binder step.

    def drop_workspace(self, author: str) -> None:
        """Permanently drop this workspace.

        An EXTERNALLY-BOUND workspace is unlinked instead - see
        `_unlink_bound_workspace`, which this delegates to. Its datasets are
        someone else's, so nothing below applies to it: no dataset is dropped,
        no storage is reclaimed, and the bound catalog is never called. The
        rest of this describes a workspace that domiciles its own data.

        Drops every materialized view, then
        every remaining dataset, then every view, then every now-empty
        collection document itself, in every collection, their storage
        reclaimed; policy.opteryx's access grants for this workspace
        (`$policies/access` - a different service's data, sharing this same
        Firestore database, with no webhook consumer of its own to clean it
        up); then the workspace's own `$properties` document. No grace
        period, no restore - turning off `deletion_protection`
        (checked here, via `_assert_not_deletion_protected`) is the
        deliberate signal of intent; there is nothing left to undo, and
        nothing left in Firestore or on disk, after this returns.

        Materialized views go first, workspace-wide, across a full pass over
        every collection before any plain dataset in ANY collection is
        touched - not per-collection interleaved with datasets. `drop_dataset`
        refuses to drop a dataset that a materialized view still reads from
        (see its own docstring), and that source can be in a different
        collection than the MV, so a per-collection ordering would still
        raise `MaterializedViewError` partway through whenever a source and
        its MV don't happen to share a collection. `drop_materialized_view`
        clears the MV's own refresh triggers before dropping its backing
        dataset, which is what `drop_dataset`'s check on the *source*
        actually reads - so by the time any plain dataset is reached, no
        materialized view anywhere in this workspace can still be blocking
        it. A materialized view whose SOURCE lives in a *different*
        workspace entirely (only possible if egress_protection was off when
        it was created) is out of scope here and still refuses correctly -
        that is a real, external dependency this operation cannot and must
        not silently override, not a bug in the ordering.

        Reuses `drop_dataset`/`drop_view`/`drop_materialized_view` per item,
        none of which touch storage, and then reclaims that storage itself,
        inline. Every dataset's location is read BEFORE the drops begin, while
        the dataset documents still exist to be read - the one moment those
        locations are known to be correct. Nothing is persisted and nothing is
        deferred, so there is no window in which a recorded location could come
        to mean something else. Leaving this to the reconciliation pass instead
        would strand a dropped workspace's files in the bucket until that pass
        next ran, breaking this method's promise that nothing is left on disk
        after it returns.

        A dataset held by its own per-asset `locked-by` lock raises
        `DatasetLocked` out of `drop_dataset` and stops this partway
        through, the same as it would dropping that one dataset directly -
        clear the lock and re-run, rather than this silently skipping it.

        A reclaim failure on any one location (e.g. a storage listing error)
        raises `WorkspaceStorageReclaimFailed` rather than letting the drop
        finish anyway - `$properties` is deliberately NOT deleted in that
        case, because deleting it removes the only way to construct a
        normal handle on this workspace again to retry the leftover
        location(s). Every dataset/view/collection has still been dropped
        by that point; only the storage confirmation and the final
        `$properties` delete are held back. Re-run `drop_workspace` once
        the underlying storage issue is resolved - it re-reads what is
        physically there rather than replaying a record.

        Raises `WorkspaceDeletionProtected` if the workspace is
        deletion-protected. This is the only thing that flag ever guarded -
        it protects the workspace itself, not the assets inside it.
        """
        if author is None:
            raise ValueError("author must be provided when dropping a workspace")

        self._assert_not_deletion_protected()

        # An externally-bound workspace is UNLINKED, not emptied: its tables
        # live in someone else's catalog and this one has no business deleting
        # them. Decided here, from the binding on this workspace's own
        # `$properties`, rather than by the caller - a caller that got this
        # wrong in the deleting direction would destroy data this catalog does
        # not own, so the choice must not be theirs to make.
        if (self.get_workspace_properties() or {}).get("catalog"):
            self._unlink_bound_workspace(author=author)
            return

        collections = list(self.list_collections())

        # Read every dataset's location up front, straight off the dataset
        # documents, while they still exist. Streaming the subcollection rather
        # than going through `list_datasets()` catches materialized views'
        # backing datasets too, whatever their `dataset-type`. This is the only
        # point at which these locations are known-correct, and they are used
        # before this call returns - never written down.
        doomed_locations = []
        for collection in collections:
            for dataset_doc in self._datasets_collection(collection).stream():
                location = (dataset_doc.to_dict() or {}).get("location")
                if location:
                    doomed_locations.append(location)

        for collection in collections:
            for mv in list(self.list_materialized_views(collection)):
                self.drop_materialized_view(f"{collection}.{mv}", author=author)

        for collection in collections:
            for dataset in list(self.list_datasets(collection)):
                self.drop_dataset(f"{collection}.{dataset}", author=author)

        for collection in collections:
            for view in list(self.list_views(collection)):
                self.drop_view(f"{collection}.{view}", author=author)

        for collection in collections:
            # The collection document itself, not just its datasets/views -
            # left behind otherwise, since Firestore does not cascade a
            # delete into (or out of) a parent. CollectionNotFound is
            # tolerated: a collection created only implicitly, by a dataset
            # inside it rather than through create_collection() explicitly,
            # stops existing on its own once that last child is gone -
            # Firestore has no document that doesn't exist through either
            # its own data or a live descendant. drop_dataset/drop_view have
            # the same tolerance built in for the same reason; this is that
            # rule applied one level up.
            try:
                self.drop_collection(collection, author=author)
            except CollectionNotFound:
                pass

        from .catalog.deep_clean import DatasetDeepClean

        # Listing each doomed location is safe in a way that listing a dropped
        # *dataset's* prefix would not be: every dataset in this workspace has
        # just been dropped, so nothing can still be writing here, and the
        # workspace itself ceases to exist a few lines below.
        lister = DatasetDeepClean(self, author=author, agent="drop-workspace")
        failed = []
        for location in doomed_locations:
            try:
                files = lister.get_all_physical_files(location)
            except Exception as exc:  # noqa: BLE001 - storage listing boundary
                logger.error("Could not list files under %s: %s", location, exc)
                failed.append({"location": location, "reason": "list-failed", "error": str(exc)})
                continue

            result = self._reclaim_paths(files)
            if result["files_failed"]:
                failed.append(
                    {
                        "location": location,
                        "reason": "partial-delete",
                        "files_failed": result["files_failed"],
                    }
                )

        if failed:
            # Must not proceed to delete $properties: once it's gone there is
            # no way to construct a normal handle on this workspace again to
            # retry whatever storage this left behind - see
            # WorkspaceStorageReclaimFailed's docstring.
            raise WorkspaceStorageReclaimFailed(
                f"Dropped every dataset/view in {self.workspace!r}, but "
                f"{len(failed)} of {len(doomed_locations)} "
                f"storage location(s) could not be confirmed reclaimed: "
                f"{failed}. The workspace's $properties document was NOT "
                f"deleted - re-run the drop once the underlying storage "
                f"issue is resolved."
            )

        # $policies/access is policy.opteryx's own data - the access grants
        # for this workspace - not opteryx_catalog's. It lives here anyway,
        # in the same `catalogs` Firestore database the two services
        # deliberately share, and policy.opteryx has no webhook consumer
        # for workspace deletion to clean it up itself (checked: there is
        # none). Left behind, this is not just an orphan - if this
        # workspace name is ever reused, its old grants would silently
        # reactivate for whatever gets created under the same name next,
        # handing out access nobody asked for to data that has nothing to
        # do with why those grants existed. Cleared here directly rather
        # than left to a cross-service webhook that does not exist and
        # would add its own delivery-reliability question if it did.
        self._delete_subcollection(self._catalog_ref.document("$policies").collection("access"))

        self._catalog_ref.document("$properties").delete()

        send_webhook(
            action="delete",
            workspace=self.workspace,
            collection=None,
            resource_type=ResourceType.WORKSPACE,
            resource_name=self.workspace,
            payload=workspace_deleted_payload(dropped_by=author),
        )

        emit_audit(
            "drop_workspace",
            resource_type=ResourceType.WORKSPACE,
            workspace=self.workspace,
            resource=self.workspace,
            author=author,
        )

    def _unlink_bound_workspace(self, author: str) -> None:
        """Drop an externally-bound workspace by unlinking it.

        The tables of a bound workspace live in the catalog it is bound to.
        What is in Firestore is a PROJECTION of that catalog's listing - stub
        documents carrying a name, a collection, and at most a schema and some
        statistics (see stub_projection._stub_document). So unlinking deletes
        the projection, the workspace's access grants and its `$properties` -
        and the external catalog is not called at all. Nothing in the bound
        catalog is dropped, renamed or otherwise touched; a workspace can be
        unlinked and re-bound with its tables entirely unaware.

        Deliberately NOT built out of `drop_dataset`, the way `drop_workspace`
        is. That path reads each dataset's storage location and deletes the
        files behind it, which is right for datasets this catalog domiciles and
        meaningless for a stub, which has no location and no files. Running it
        here would ask the storage layer to reclaim addresses that were never
        ours, and there is no reclaim step at all in what follows because there
        is nothing to reclaim.

        `deletion_protection` has already been checked by the caller: unlinking
        is not a lesser act needing a lesser gate. It destroys the binding and
        the grants, and re-establishing them means re-provisioning a credential
        this catalog cannot recover on its own.
        """
        for collection_doc in list(self._catalog_ref.list_documents()):
            if collection_doc.id.startswith("$"):
                continue
            self._delete_subcollection(collection_doc.collection("datasets"))
            collection_doc.delete()

        # policy.opteryx's grants for this workspace, cleared for exactly the
        # reason drop_workspace clears them: left behind, they silently
        # reactivate if this workspace name is ever reused.
        self._delete_subcollection(self._catalog_ref.document("$policies").collection("access"))

        self._catalog_ref.document("$properties").delete()

        send_webhook(
            action="delete",
            workspace=self.workspace,
            collection=None,
            resource_type=ResourceType.WORKSPACE,
            resource_name=self.workspace,
            payload=workspace_deleted_payload(dropped_by=author),
        )

        emit_audit(
            "unlink_workspace",
            resource_type=ResourceType.WORKSPACE,
            workspace=self.workspace,
            resource=self.workspace,
            author=author,
        )

    def lock_workspace(self, author: str) -> None:
        """Set the two-person-deniability lock on this workspace."""
        if author is None:
            raise ValueError("author must be provided when locking a workspace")

        now_ms = int(time.time() * 1000)
        self._catalog_ref.document("$properties").update(
            {"locked-by": author, "locked-at-ms": now_ms}
        )

        send_webhook(
            action="lock",
            workspace=self.workspace,
            collection=None,
            resource_type=ResourceType.WORKSPACE,
            resource_name=self.workspace,
            payload=workspace_locked_payload(locked_by=author),
        )

        emit_audit(
            "lock_workspace",
            resource_type=ResourceType.WORKSPACE,
            workspace=self.workspace,
            resource=self.workspace,
            author=author,
        )

    def unlock_workspace(self, author: str) -> None:
        """Clear the lock set by `lock_workspace`."""
        if author is None:
            raise ValueError("author must be provided when unlocking a workspace")

        self._catalog_ref.document("$properties").update({"locked-by": None, "locked-at-ms": None})

        send_webhook(
            action="unlock",
            workspace=self.workspace,
            collection=None,
            resource_type=ResourceType.WORKSPACE,
            resource_name=self.workspace,
            payload=workspace_unlocked_payload(unlocked_by=author),
        )

        emit_audit(
            "unlock_workspace",
            resource_type=ResourceType.WORKSPACE,
            workspace=self.workspace,
            resource=self.workspace,
            author=author,
        )

    def _assert_not_deletion_protected(self) -> None:
        """Refuse deletion of this workspace while it is deletion-protected.

        **On by default** (see `_guard_is_on`): a workspace is protected from
        birth, and deleting one is a deliberate two-step - turn the flag off,
        then delete. Losing a whole workspace is not something an operator
        should be able to do in a single statement they half-meant, and the
        cost of the default being wrong is one extra statement.

        Scope is the workspace itself and nothing else: `deletion_protection`
        does not guard the datasets, collections or views inside it. Per-asset
        protection is the `locked-by` two-person lock, which is a separate
        mechanism with separate semantics.

        Read fresh from Firestore every time rather than cached on the handle: a
        cached flag would let a long-lived catalog object delete the workspace
        after protection was switched on elsewhere, which is exactly the case
        the setting exists to prevent.

        A Firestore read error propagates rather than being swallowed into
        "not protected".
        """
        if _guard_is_on(self.get_workspace_properties(), DELETION_PROTECTION_PROPERTY):
            raise WorkspaceDeletionProtected(
                f"Cannot delete workspace '{self.workspace}': it is deletion-protected "
                "(workspaces are protected unless the flag is explicitly turned off). "
                f"Clear it with ALTER WORKSPACE {self.workspace} "
                f"SET {DELETION_PROTECTION_PROPERTY} TO OFF."
            )

    def get_workspace_properties(self) -> dict:
        """Return the workspace's `$properties` document as a plain dict.

        This is the whole document, lifecycle fields included, so callers can
        read `owner`, `billing-account-id`, `deleted-at-ms`, `locked-by` and any
        settable property in one go. Returns `{}` when the document does not
        exist - a workspace whose `$properties` never got written (the
        constructor's write is best-effort) reads as "no properties", not as an
        error, because every field it holds is optional.

        Unlike the constructor's read this does NOT gate on `deleted-at-ms`;
        reading a deleted workspace's properties is exactly how a caller finds
        out that it is deleted.
        """
        doc = self._catalog_ref.document("$properties").get()
        if not doc.exists:
            return {}
        return doc.to_dict() or {}

    # ------------------------------------------------------------------
    # Egress lock
    # ------------------------------------------------------------------

    def _foreign_properties_ref(self, workspace: str):
        """The `$properties` document of any workspace in this database.

        Workspaces are sibling root collections of one Firestore database
        (`self._catalog_ref` is `client.collection(self.workspace)`), so a
        handle bound to one workspace can read another's properties without
        constructing a second catalog - which would re-run the constructor's
        existence and soft-delete gates and raise for exactly the workspaces
        an egress check most wants an answer about.
        """
        if workspace == self.workspace:
            return self._catalog_ref.document("$properties")
        return self.firestore_client.collection(workspace).document("$properties")

    def is_egress_restricted(self, workspace: str | None = None) -> bool:
        """Whether `workspace` (default: this one) restricts egress.

        **On by default** (see `_guard_is_on`): unset reads as restricted, and
        so does a workspace with no `$properties` document at all. Sharing a
        workspace's data out is opt-in, not opt-out - the whole point is that
        `reader` should not silently carry the right to mirror the data
        somewhere else, and a default of "unrestricted until someone notices"
        would leave every workspace nobody has thought about wide open.

        A name matching no workspace reads as restricted too. That is
        fail-closed on a typo - and the only cost is a clear refusal on a copy
        that had no valid source to read anyway.

        Read fresh every time, for the same reason `_assert_not_deletion_protected`
        does: a cached answer would let a long-lived catalog handle keep copying
        after the lock went on.
        """
        workspace = workspace or self.workspace
        doc = self._foreign_properties_ref(workspace).get()
        if not doc.exists:
            return True
        return _guard_is_on(doc.to_dict() or {}, EGRESS_PROTECTION_PROPERTY)

    def egress_verdict(
        self,
        source_workspaces: Iterable[str],
        destination_workspace: str,
        operation: str,
    ) -> list[EgressRefusal]:
        """Every source workspace that refuses this copy, in first-seen order.

        The decision itself - `enforce_egress_policy` is this plus a raise, so
        there is one implementation of what egress protection means and not two
        that can drift. Callers that need to *compose* a refusal with something
        else (an engine reporting several reasons a statement cannot run, a UI
        listing what would have to change) read this; callers that just need the
        statement stopped use `enforce_egress_policy`.

        Empty means allowed. That makes an accidental empty return a silent
        permit, so a caller that could not reach this at all - a version skew, an
        unreachable store - must produce a refusal of its own rather than an
        empty list.

        Unlike the raising form this reads every crossing workspace's flag
        rather than stopping at the first refusal, which is the point: someone
        clearing egress on a three-workspace join should be told all three at
        once instead of discovering them one failed statement at a time. The
        extra reads happen only on the path that is already refusing.

        Args:
            source_workspaces: workspace of each table the copy reads.
            destination_workspace: workspace the copy writes into.
            operation: what is being attempted, for the message - e.g.
                "create materialized view mart.daily".
        """
        refusals: list[EgressRefusal] = []
        checked: set[str] = set()
        for source_workspace in source_workspaces:
            if source_workspace == destination_workspace or source_workspace in checked:
                continue
            checked.add(source_workspace)
            if self.is_egress_restricted(source_workspace):
                refusals.append(
                    EgressRefusal(
                        workspace=source_workspace,
                        destination=destination_workspace,
                        operation=operation,
                    )
                )
        return refusals

    def enforce_egress_policy(
        self,
        source_workspaces: Iterable[str],
        destination_workspace: str,
        operation: str,
    ) -> None:
        """Refuse a copy that would land a source workspace's data elsewhere.

        The shared gate behind every automated copy path: materialized-view
        creation and refresh here, CTAS in the engine. Callers that already
        know each source's workspace (the engine parses fully-qualified names)
        pass them directly; the MV paths in this class go through
        `enforce_materialized_view_egress`, which resolves them first.

        `enforce_` rather than this class's usual `_assert_` because these two
        are public and called across a duck-typed catalog boundary (the firing
        path, the engine): `unittest.mock` refuses any attribute starting with
        `assert`, so an `assert_`-named method is unusable on a stubbed catalog.

        The **source** workspace's flag decides, not the destination's: the
        property protects data leaving. A copy that stays inside the source
        workspace is not egress and is always allowed, whatever the flag says -
        which is what keeps the default-on posture liveable, since the ordinary
        same-workspace view or CTAS never touches this at all.

        Because the flag defaults to ON, a cross-workspace copy is refused
        until the *source* workspace's owner opts out. Sharing out is a
        decision someone makes, not a state a workspace drifts into.

        What this is NOT: containment. Anyone with read on the source can
        SELECT it and paste the rows anywhere they like, and this does nothing
        about that - it is leaky by construction, the same way a VPC Service
        Controls perimeter is an egress boundary rather than a permission. What
        it stops is the systematic, automated, recurring copy: the standing MV
        or CTAS that keeps a full mirror of someone else's data fresh forever
        off the back of a single `reader` grant. That is where the real leakage
        volume is, and it is the part a read grant is never understood to
        include.

        Args:
            source_workspaces: workspace of each table the copy reads.
            destination_workspace: workspace the copy writes into.
            operation: what is being attempted, for the error message - e.g.
                "create materialized view mart.daily".

        Raises:
            EgressRestricted: if any source workspace differs from the
                destination and has `egress_protection` set.
        """
        refusals = self.egress_verdict(source_workspaces, destination_workspace, operation)
        if refusals:
            raise EgressRestricted(str(refusals[0]))

    # Fields on `$properties` that only their own dedicated methods may write.
    # `locked-by` gates real control flow (drop_dataset/drop_collection raise
    # Locked when it's set), so letting a generic property setter touch it
    # would let a caller clear a lock while bypassing lock_workspace /
    # unlock_workspace and the audit records and webhooks those emit.
    # `deleted-at-ms`/`deleted-by` are legacy fields from the soft-delete
    # model DROP WORKSPACE replaced - nothing reads them anymore, but they
    # stay reserved rather than becoming a plain writable property.
    _RESERVED_WORKSPACE_PROPERTIES = frozenset(
        {
            "timestamp-ms",
            "deleted-at-ms",
            "deleted-by",
            "locked-by",
            "locked-at-ms",
        }
    )

    def set_workspace_properties(self, properties: dict, author: str | None = None) -> None:
        """Merge `properties` into the workspace's `$properties` document.

        A merge, not a replace: keys absent from `properties` are left as they
        are, so a caller setting one property cannot blank the rest by omission.
        To remove a property, set it to None explicitly.

        Args:
            properties: Property names to values. Must be non-empty.
            author: The identity making the change - None when unauthenticated,
                never substituted (see audit.emit_audit).

        Raises:
            ValueError: If `properties` is empty or names a reserved lifecycle
                field (see `_RESERVED_WORKSPACE_PROPERTIES`).
        """
        if not properties:
            raise ValueError("properties must be a non-empty mapping")

        reserved = sorted(set(properties) & self._RESERVED_WORKSPACE_PROPERTIES)
        if reserved:
            raise ValueError(
                f"Cannot set reserved workspace lifecycle field(s) {reserved} through "
                "set_workspace_properties; use the dedicated drop/restore/lock/unlock methods."
            )

        updates = dict(properties)
        updates["timestamp-ms"] = int(time.time() * 1000)

        # set(merge=True) rather than update() so a workspace whose $properties
        # doc was never written (the constructor's write is best-effort and
        # swallows Firestore errors) gets one here instead of raising NotFound.
        self._catalog_ref.document("$properties").set(updates, merge=True)

        emit_audit(
            "set_workspace_properties",
            resource_type=ResourceType.WORKSPACE,
            workspace=self.workspace,
            resource=self.workspace,
            author=author,
            properties=sorted(properties),
        )

    def list_datasets(self, collection: str) -> Iterable[str]:
        coll = self._datasets_collection(collection)
        return [doc.id for doc in coll.stream()]

    def list_collections(self) -> Iterable[str]:
        """List top-level collections (documents) in this workspace."""
        try:
            return [col.id for col in self._catalog_ref.list_documents() if col.id[0] != "$"]
        except (ValueError, KeyError, AttributeError):
            return []

    def create_collection(
        self,
        collection: str,
        properties: dict | None = None,
        exists_ok: bool = False,
        author: str | None = None,
    ) -> None:
        """Create a collection document under the catalog.

        If `exists_ok` is False and the collection already exists, a KeyError is raised.

        A collection name may not contain a dot. Qualified names are split
        left-anchored with maxsplit - `ws.coll.a.b` is dataset `a.b` in
        collection `coll` - so a dotted collection name would make every
        qualified name ambiguous and silently misroute materialized-view
        sources and triggers. See `_split_qualified`.
        """
        if "." in collection:
            raise ValueError(
                f"collection name may not contain a dot: {collection!r} - qualified "
                "names are parsed as 'workspace.collection.dataset' and a dotted "
                "collection would make that split ambiguous"
            )
        doc_ref = self._collection_ref(collection)
        if doc_ref.get().exists:
            if exists_ok:
                return
            raise CollectionAlreadyExists(f"Collection already exists: {collection}")

        now_ms = int(time.time() * 1000)
        if author is None:
            raise ValueError("author must be provided when creating a collection")
        doc_ref.set(
            {
                "name": collection,
                "properties": properties or {},
                "timestamp-ms": now_ms,
                "author": author,
                "annotations": [],
                "locked-by": None,
                "locked-at-ms": None,
            }
        )

        emit_audit(
            "create_collection",
            resource_type=ResourceType.COLLECTION,
            workspace=self.workspace,
            resource=collection,
            author=author,
        )

    def create_collection_if_not_exists(
        self, collection: str, properties: dict | None = None, author: str | None = None
    ) -> None:
        """Convenience wrapper that creates the collection only if missing."""
        self.create_collection(collection, properties=properties, exists_ok=True, author=author)

    def collection_exists(self, collection: str) -> bool:
        """Return True if the collection exists."""
        try:
            return self._collection_ref(collection).get().exists
        except Exception as exc:  # noqa: BLE001 - Firestore client boundary
            # On any error, be conservative and return False. Callers use this to
            # decide whether to CREATE, and create_collection re-checks under its
            # own guard, so a false negative costs a retry rather than a clobber.
            logger.debug("collection_exists(%s) failed (%s)", collection, exc)
            # On any error, be conservative and return False
            return False

    def drop_collection(self, collection: str, author: str | None = None) -> None:
        """Drop a collection.

        A collection owns no storage of its own - only its datasets and views
        do - so this reclaims nothing; deleting the catalog document is the
        whole operation. Raises CollectionNotEmpty if any datasets or views
        remain, since deleting a non-empty collection would otherwise silently
        orphan them - their storage still reachable by reconciliation, but the
        datasets themselves no longer reachable through `list_collections()`.
        Raises `CollectionLocked` if the collection's `locked-by` field is
        set - the two-person deniability lock takes precedence over the drop.
        The workspace's `deletion_protection` does not apply; it protects the
        workspace itself, not the assets inside it.

        An author is required, as it is to create one.
        """
        if not author:
            raise ValueError("author must be provided when dropping a collection")
        doc_ref = self._collection_ref(collection)
        doc = doc_ref.get()
        if not doc.exists:
            raise CollectionNotFound(f"Collection not found: {collection}")

        data = doc.to_dict() or {}
        if data.get("locked-by") is not None:
            raise CollectionLocked(f"Collection is locked: {collection}")

        if any(True for _ in self._datasets_collection(collection).stream()) or any(
            True for _ in self._views_collection(collection).stream()
        ):
            raise CollectionNotEmpty(f"Collection is not empty: {collection}")

        doc_ref.delete()

        emit_audit(
            "drop_collection",
            resource_type=ResourceType.COLLECTION,
            workspace=self.workspace,
            resource=collection,
            author=author,
        )

    def dataset_exists(
        self, identifier_or_collection: str, dataset_name: str | None = None
    ) -> bool:
        """Return True if the dataset exists.

        Supports two call forms:
        - dataset_exists("collection.dataset")
        - dataset_exists("collection", "dataset")
        """
        # Normalize inputs
        if dataset_name is None:
            # Expect a single collection like 'collection.dataset'
            if "." not in identifier_or_collection:
                raise ValueError(
                    "collection must be 'collection.dataset' or pass dataset_name separately"
                )
            collection, dataset_name = identifier_or_collection.rsplit(".", 1)
        else:
            collection = identifier_or_collection

        try:
            doc_ref = self._dataset_doc_ref(collection, dataset_name)
            return doc_ref.get().exists
        except Exception as exc:  # noqa: BLE001 - Firestore client boundary, see above
            # On any error, be conservative and return False
            logger.debug("dataset_exists(%s) failed (%s)", collection, exc)
            # On any error, be conservative and return False
            return False

    # Dataset API methods have been renamed to the preferred `dataset` terminology.

    # --- View support -------------------------------------------------
    def create_view(
        self,
        identifier: str | tuple,
        sql: str,
        schema: Any | None = None,
        author: str | None = None,
        description: str | None = None,
        properties: dict | None = None,
        update_if_exists: bool = False,
    ) -> CatalogView:
        """Create a view document and a statement version in the `statement` subcollection.

        `identifier` may be a string like 'namespace.view' or a tuple ('namespace','view').
        """
        # Normalize identifier
        if isinstance(identifier, (tuple, list)):
            collection, view_name = identifier[0], identifier[1]
        else:
            collection, view_name = identifier.split(".")

        doc_ref = self._view_doc_ref(collection, view_name)
        self.assert_name_free(collection, view_name, "view")
        if doc_ref.get().exists:
            if not update_if_exists:
                raise ViewAlreadyExists(f"View already exists: {collection}.{view_name}")
            # Update existing view - get current sequence number
            existing_doc = doc_ref.get().to_dict()
            current_statement_id = existing_doc.get("statement-id")
            if current_statement_id:
                stmt_ref = doc_ref.collection("statement").document(current_statement_id)
                stmt_doc = stmt_ref.get()
                if stmt_doc.exists:
                    sequence_number = stmt_doc.to_dict().get("sequence-number", 0) + 1
                else:
                    sequence_number = 1
            else:
                sequence_number = 1
        else:
            sequence_number = 1

        now_ms = int(time.time() * 1000)
        if author is None:
            raise ValueError("author must be provided when creating a view")

        # Write statement version
        # The statement id is the millisecond it was written, so two
        # redefinitions inside one millisecond would collide on the document id
        # and the earlier statement would be overwritten - losing a version the
        # sequence number still claims is there. Step forward to the first free
        # id instead.
        stmt_coll = doc_ref.collection("statement")
        statement_id = str(now_ms)
        while stmt_coll.document(statement_id).get().exists:
            now_ms += 1
            statement_id = str(now_ms)
        stmt_coll.document(statement_id).set(
            {
                "sql": sql,
                "timestamp-ms": now_ms,
                "author": author,
                "sequence-number": sequence_number,
            }
        )

        # Persist root view doc referencing the statement id
        doc_ref.set(
            {
                "name": view_name,
                "collection": collection,
                "workspace": self.workspace,
                "timestamp-ms": now_ms,
                "author": author,
                "description": description,
                "describer": author,
                "last-execution-ms": None,
                "last-execution-data-size": None,
                "last-execution-records": None,
                "statement-id": statement_id,
                "properties": properties or {},
            }
        )

        # Send webhook notification
        send_webhook(
            action="create" if not update_if_exists else "update",
            workspace=self.workspace,
            collection=collection,
            resource_type=ResourceType.VIEW,
            resource_name=view_name,
            payload=view_created_payload(
                definition=sql,
                properties=properties,
            ),
        )

        emit_audit(
            "update_view" if update_if_exists else "create_view",
            resource_type=ResourceType.VIEW,
            workspace=self.workspace,
            collection=collection,
            resource=view_name,
            author=author,
            statement_id=statement_id,
        )

        # Return a simple CatalogView wrapper
        v = CatalogView(name=view_name, definition=sql, properties=properties or {})
        # provide convenient attributes used by docs/examples
        v.sql = sql
        v.metadata = type("M", (), {})()
        v.metadata.schema = schema
        # Attach catalog and identifier for describe() method
        v._catalog = self
        v._identifier = f"{collection}.{view_name}"
        return v

    def load_view(self, identifier: str | tuple) -> CatalogView:
        """Load a view by identifier. Returns a `CatalogView` with `.definition` and `.sql`.

        Raises `ViewNotFound` if the view doc is missing.
        """
        if isinstance(identifier, (tuple, list)):
            collection, view_name = identifier[0], identifier[1]
        else:
            collection, view_name = identifier.split(".")

        doc = self._view_doc_ref(collection, view_name).get()
        if not doc.exists:
            raise ViewNotFound(f"View not found: {collection}.{view_name}")
        return self._build_view(collection, view_name, doc)

    def _build_view(self, collection, view_name, doc) -> CatalogView:
        """Build a CatalogView from an already-fetched view doc."""
        data = doc.to_dict() or {}
        stmt_id = data.get("statement-id")
        schema = data.get("schema")

        sdoc = (
            self._view_doc_ref(collection, view_name)
            .collection("statement")
            .document(str(stmt_id))
            .get()
        )
        sql = (sdoc.to_dict() or {}).get("sql")

        v = CatalogView(name=view_name, definition=sql or "", properties=data.get("properties", {}))
        v.sql = sql or ""
        v.metadata = type("M", (), {})()
        v.metadata.schema = schema
        # Populate metadata fields from the stored view document so callers
        # expecting attributes like `timestamp_ms` won't fail.
        v.metadata.author = data.get("author")
        v.metadata.description = data.get("description")
        v.metadata.timestamp_ms = data.get("timestamp-ms")
        # Execution/operational fields (may be None)
        v.metadata.last_execution_ms = data.get("last-execution-ms")
        v.metadata.last_execution_data_size = data.get("last-execution-data-size")
        v.metadata.last_execution_records = data.get("last-execution-records")
        # Optional describer (used to flag LLM-generated descriptions)
        v.metadata.describer = data.get("describer")
        # Attach catalog and identifier for describe() method
        v._catalog = self
        v._identifier = f"{collection}.{view_name}"
        return v

    def drop_view(self, identifier: str | tuple, author: str | None = None) -> None:
        """Drop a view.

        A view owns no storage, so dropping it leaves nothing to reclaim -
        unlike `drop_dataset`.

        The workspace's `deletion_protection` does not apply; it protects the
        workspace itself, not the assets inside it.

        An author is required, as it is to create one - see `drop_dataset` for
        why the check precedes the does-it-exist return.
        """
        if not author:
            raise ValueError("author must be provided when dropping a view")
        if isinstance(identifier, (tuple, list)):
            collection, view_name = identifier[0], identifier[1]
        else:
            collection, view_name = identifier.split(".")

        doc_ref = self._view_doc_ref(collection, view_name)
        if not doc_ref.get().exists:
            return

        self._delete_subcollection(doc_ref.collection("statement"))
        doc_ref.delete()

        send_webhook(
            action="delete",
            workspace=self.workspace,
            collection=collection,
            resource_type=ResourceType.VIEW,
            resource_name=view_name,
            payload=view_deleted_payload(dropped_by=author),
        )

        emit_audit(
            "drop_view",
            resource_type=ResourceType.VIEW,
            workspace=self.workspace,
            collection=collection,
            resource=view_name,
            author=author,
        )

    def list_views(self, collection: str) -> Iterable[str]:
        coll = self._views_collection(collection)
        return [doc.id for doc in coll.stream()]

    # ------------------------------------------------------------------
    # Tasks
    # ------------------------------------------------------------------

    def create_task(
        self,
        identifier: str | tuple,
        sql: str,
        author: str | None = None,
        description: str | None = None,
        update_if_exists: bool = False,
    ) -> None:
        """Register a task: a statement the platform runs on its own.

        The statement is versioned in a `statement` subcollection, the same way
        a view's and a materialized view's are, so redefining a task keeps its
        history rather than overwriting it.

        A task carries NO identity of its own. A person running it with EXECUTE
        runs it as themselves; an unattended run carries the identity of the
        TRIGGER that fired it (`create_trigger`'s `runs-as`). Storing an owner
        here would be a second answer to the same question.
        """
        if author is None:
            raise ValueError("author must be provided when creating a task")
        if not sql or not sql.strip():
            raise ValueError("a task requires a statement")

        collection, task_name = self._task_parts(identifier)

        self.assert_name_free(collection, task_name, "task")
        doc_ref = self._task_doc_ref(collection, task_name)
        existing = doc_ref.get()
        data = existing.to_dict() or {} if existing.exists else {}
        if existing.exists and not update_if_exists:
            raise TaskAlreadyExists(f"Task already exists: {collection}.{task_name}")

        now_ms = int(time.time() * 1000)
        sequence_number = 1
        current_statement_id = data.get("statement-id")
        if current_statement_id:
            stmt_doc = doc_ref.collection("statement").document(str(current_statement_id)).get()
            if stmt_doc.exists:
                sequence_number = (stmt_doc.to_dict() or {}).get("sequence-number", 0) + 1

        # The statement id is the millisecond it was written, so two
        # redefinitions inside one millisecond would collide on the document id
        # and the earlier statement would be overwritten - losing a version the
        # sequence number still claims is there. Step forward to the first free
        # id instead: the id is an identifier, not a clock reading, and a
        # statement history that quietly drops entries is worse than one whose
        # timestamps are a millisecond optimistic.
        statements = doc_ref.collection("statement")
        statement_id = str(now_ms)
        while statements.document(statement_id).get().exists:
            now_ms += 1
            statement_id = str(now_ms)

        statements.document(statement_id).set(
            {
                "sql": sql,
                "timestamp-ms": now_ms,
                "author": author,
                "sequence-number": sequence_number,
            }
        )

        doc_ref.set(
            {
                "name": task_name,
                "statement-id": statement_id,
                "description": description if description is not None else data.get("description"),
                "created-by": data.get("created-by") or author,
                "created-at-ms": data.get("created-at-ms") or now_ms,
                "suspended-at-ms": data.get("suspended-at-ms"),
                "suspended-by": data.get("suspended-by"),
                "last-fired-at-ms": data.get("last-fired-at-ms"),
                "last-fired-status": data.get("last-fired-status"),
                # The `current_version` of the last run that succeeded. Not a
                # consumer offset - nothing reads it to decide a window - but the
                # breadcrumb that makes a MISSED window detectable: a firing whose
                # parent_version does not meet this has a gap behind it.
                "last-window-to": data.get("last-window-to"),
            }
        )

        emit_audit(
            "update_task" if existing.exists else "create_task",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=collection,
            resource=task_name,
            author=author,
            statement_id=statement_id,
        )

    def get_task(self, identifier: str | tuple) -> dict:
        """The task's registration record, including its current statement.

        `last-updated-by`/`last-updated-at-ms` describe who last changed the
        statement, which is a different question - and often a different person -
        from who created the task. There is no `runs-as`: the identity of an
        unattended run belongs to the trigger that fires it.
        """
        collection, task_name = self._task_parts(identifier)

        doc_ref = self._task_doc_ref(collection, task_name)
        doc = doc_ref.get()
        if not doc.exists:
            raise TaskNotFound(f"Task not found: {collection}.{task_name}")
        data = doc.to_dict() or {}

        statement_id = data.get("statement-id")
        sql = None
        last_updated_by = None
        last_updated_at_ms = None
        if statement_id:
            statement = (
                doc_ref.collection("statement").document(str(statement_id)).get().to_dict() or {}
            )
            sql = statement.get("sql")
            last_updated_by = statement.get("author")
            last_updated_at_ms = statement.get("timestamp-ms")

        return {
            "identifier": f"{self.workspace}.{collection}.{task_name}",
            "name": task_name,
            "collection": collection,
            "workspace": self.workspace,
            "sql": sql,
            "statement-id": statement_id,
            "description": data.get("description"),
            "created-by": data.get("created-by"),
            "created-at-ms": data.get("created-at-ms"),
            # Who last changed the STATEMENT, which is a different question from
            # who created the task and often a different person.
            "last-updated-by": last_updated_by,
            "last-updated-at-ms": last_updated_at_ms,
            "suspended-at-ms": data.get("suspended-at-ms"),
            "suspended-by": data.get("suspended-by"),
            "last-fired-at-ms": data.get("last-fired-at-ms"),
            "last-fired-status": data.get("last-fired-status"),
            "last-window-to": data.get("last-window-to"),
        }

    def list_tasks(self, collection: str) -> list:
        return [doc.id for doc in self._tasks_collection(collection).stream()]

    def drop_task(self, identifier: str | tuple, author: str | None = None) -> None:
        """Drop a task. A task owns no storage, so nothing is reclaimed.

        Triggers that fire it are NOT swept here: a trigger lives on the dataset
        that fires it, and reaching across to delete other datasets' documents
        from a drop is how a partial failure leaves a dataset with a trigger
        nobody can see. `fire_tasks` treats a trigger whose task is gone as a
        failure it reports, which is recoverable; a silently half-dropped
        trigger is not.
        """
        if not author:
            raise ValueError("author must be provided when dropping a task")
        collection, task_name = self._task_parts(identifier)

        doc_ref = self._task_doc_ref(collection, task_name)
        if not doc_ref.get().exists:
            return

        self._delete_subcollection(doc_ref.collection("statement"))
        doc_ref.delete()

        emit_audit(
            "drop_task",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=collection,
            resource=task_name,
            author=author,
        )

    def mark_task_fired(
        self,
        identifier: str | tuple,
        status: str,
        window_to: int | None = None,
    ) -> None:
        """Stamp a task's last-fired fields, and its window breadcrumb on success.

        `window_to` is recorded ONLY when the run succeeded: a failed run
        consumed nothing, and advancing the breadcrumb past it would hide the
        very gap the breadcrumb exists to expose.
        """
        collection, task_name = self._task_parts(identifier)

        update = {
            "last-fired-at-ms": int(time.time() * 1000),
            "last-fired-status": status,
        }
        if window_to is not None:
            update["last-window-to"] = window_to
        self._task_doc_ref(collection, task_name).update(update)

    def view_exists(
        self, identifier_or_collection: str | tuple, view_name: str | None = None
    ) -> bool:
        """Return True if the view exists.

        Supports two call forms:
        - view_exists("collection.view")
        - view_exists(("collection", "view"))
        - view_exists("collection", "view")
        """
        # Normalize inputs
        if view_name is None:
            if isinstance(identifier_or_collection, (tuple, list)):
                collection, view_name = identifier_or_collection[0], identifier_or_collection[1]
            else:
                if "." not in identifier_or_collection:
                    raise ValueError(
                        "identifier must be 'collection.view' or pass view_name separately"
                    )
                collection, view_name = identifier_or_collection.rsplit(".", 1)
        else:
            collection = identifier_or_collection

        try:
            doc_ref = self._view_doc_ref(collection, view_name)
            return doc_ref.get().exists
        except Exception as exc:  # noqa: BLE001 - Firestore client boundary, see above
            logger.debug("view_exists(%s) failed (%s)", collection, exc)
            return False

    # ------------------------------------------------------------------
    # Triggers
    # ------------------------------------------------------------------

    def _triggers_collection(self, collection: str, dataset_name: str):
        return self._dataset_doc_ref(collection, dataset_name).collection(TRIGGERS_SUBCOLLECTION)

    def _relationships_collection(self, collection: str, dataset_name: str):
        return self._dataset_doc_ref(collection, dataset_name).collection(
            RELATIONSHIPS_SUBCOLLECTION
        )

    def _relationship_suppressions_collection(self, collection: str, dataset_name: str):
        return self._dataset_doc_ref(collection, dataset_name).collection(
            RELATIONSHIP_SUPPRESSIONS_SUBCOLLECTION
        )

    def _materialized_views_reading(self, dataset_identifier: str) -> set[str]:
        """Qualified names of the materialized views that read this dataset.

        Read from the dataset's own triggers subcollection, not by scanning for
        views whose source list mentions it: an MV's refresh trigger lives on
        each of its sources, so the answer is already here as one keyed read -
        the same read the commit path makes on every write.
        """
        try:
            collection, dataset_name = self._local_parts(dataset_identifier)
        except MaterializedViewError:
            return set()
        readers = set()
        for doc in self._triggers_collection(collection, dataset_name).stream():
            trigger = doc.to_dict() or {}
            if trigger.get("kind") != MV_REFRESH_TRIGGER_KIND:
                continue
            target = trigger.get("target-view")
            if target and "." in target:
                readers.add(self._qualify(target))
        return readers

    def _qualify(self, table: str) -> str:
        """The fully-qualified 'workspace.collection.dataset' form of a name.

        The single place a workspace is ever inferred, and the only shape the
        materialized-view and trigger records store. A 3+-part name already
        carries its workspace and is returned untouched; a 2-part name is read
        as relative to this catalog.

        This replaced a rule that reduced names the other way, toward
        'collection.dataset'. That rule was lossy and could not be made
        otherwise: `a.b.c` was returned unchanged whether `a` was a collection
        here holding a dataset called `b.c`, or the name of another workspace
        entirely - so a foreign source silently became a local lookup. Storing
        the qualified form removes the guess rather than improving it.

        Idempotent, so callers may hand over either form and methods can
        normalize on entry without caring which they were given.
        """
        parts = table.split(".")
        if len(parts) >= 3:
            return table
        if len(parts) == 2:
            return f"{self.workspace}.{table}"
        raise MaterializedViewError(f"table name must be at least 'collection.dataset': {table}")

    @staticmethod
    def _split_qualified(table: str) -> tuple[str, str, str]:
        """Split a qualified name into (workspace, collection, dataset).

        Left-anchored with maxsplit, so a dataset name containing dots stays
        whole: 'ws.coll.a.b' is dataset 'a.b' in collection 'coll'. That is
        unambiguous only because workspace and collection names may not contain
        dots - enforced in `create_collection`, and the precondition the whole
        qualified-name scheme rests on.
        """
        parts = table.split(".", 2)
        if len(parts) != 3:
            raise MaterializedViewError(
                f"expected a qualified 'workspace.collection.dataset' name: {table}"
            )
        return parts[0], parts[1], parts[2]

    def _local_parts(self, table: str) -> tuple[str, str]:
        """(collection, dataset) for a name that must live in THIS workspace.

        The replacement for every `_relative_identifier(x).split(".", 1)` in the
        trigger and materialized-view paths. A name belonging to another
        workspace raises here rather than being written into this one - the
        failure the old reduction produced silently.
        """
        workspace, collection, dataset_name = self._split_qualified(self._qualify(table))
        if workspace != self.workspace:
            raise MaterializedViewError(
                f"{table} belongs to workspace {workspace}, not {self.workspace}; "
                "this catalog handle cannot read or write it"
            )
        return collection, dataset_name

    def _source_workspace(self, table: str) -> str:
        """Which workspace a source-table name refers to.

        Now a split rather than a guess. This used to probe Firestore to
        resolve the ambiguity in the old relative form - a name that resolved
        to a local dataset was read as local - which was the best available
        answer while sources did not carry their workspace. They do now, so the
        probe is gone along with its Firestore read.
        """
        return self._split_qualified(self._qualify(table))[0]

    def enforce_materialized_view_egress(
        self, identifier: str, source_tables: Iterable[str], operation: str = "refresh"
    ) -> None:
        """Egress gate for a materialized view, at creation and at refresh.

        Resolves each source's workspace and hands the answer to
        `enforce_egress_policy`. The destination is always this catalog's
        workspace: an MV materializes into its own workspace.

        Checked at BOTH ends of the view's life because the lock and the view
        move independently - a workspace can be locked long after a view that
        reads it was registered, and each refresh is a fresh copy that the lock
        has to be able to stop.

        Where this stands relative to the feature it guards: cross-workspace MV
        sources are not representable yet - `_relative_identifier` collapses a
        foreign prefix into a relative name, and the engine's
        `register_materialized_view` rejects one outright - but they are coming,
        and this is the guard waiting for them. Building it first is the point:
        the flag defaults to ON, so the day a view can read another workspace,
        it needs that workspace's owner to have said yes, and there is no
        window in which the feature ships ahead of the boundary. Until then it
        fires for a caller driving this library directly with a
        foreign-qualified source, and CTAS (in the engine) is the path where
        `enforce_egress_policy` is load-bearing today.
        """
        self.enforce_egress_policy(
            (self._source_workspace(source) for source in source_tables),
            self.workspace,
            f"{operation} materialized view {identifier}",
        )

    def _assert_no_trigger_cycle(self, dataset_identifier: str, target_view: str) -> None:
        """Refuse a refresh trigger that would close a loop in the trigger graph.

        The trigger graph is the one that actually fires: a node is a dataset,
        and an edge D -> V is a refresh trigger on D targeting view V, meaning
        a user commit on D enqueues a refresh that commits on V. It must stay a
        DAG. A cycle in it refreshes forever - every refresh lands a
        user-created commit, `_after_commit` fires that dataset's triggers
        afresh, and nothing carries a hop count or lineage that could notice
        the loop at run time.

        Checked here rather than only over `source-tables` because the two
        graphs are not the same graph. `create_materialized_view` keeps them
        consistent by reconciling triggers against the source list, but a
        trigger written through this method directly - or one left behind by a
        reconciliation that failed partway - is a firing edge that appears in
        no source list, and a source-based walk cannot see it.

        The edge being added is `dataset_identifier -> target_view`, so it
        closes a cycle exactly when `dataset_identifier` is already reachable
        FROM `target_view`. Walks forward from the target, one subcollection
        read per node, terminating at datasets with no refresh triggers - which
        is every leaf of a chain, and the common case at depth one.
        """
        source = self._qualify(dataset_identifier)
        target = self._qualify(target_view)
        stack: list[tuple[str, list[str]]] = [(target, [source])]
        seen: set[str] = set()
        while stack:
            current, path = stack.pop()
            if current == source:
                raise MaterializedViewError(
                    "trigger cycle: a refresh trigger on "
                    f"{dataset_identifier} targeting {target_view} would make "
                    f"{source} refresh itself ({' -> '.join([*path, current])})"
                )
            if current in seen:
                continue
            seen.add(current)
            if self._split_qualified(current)[0] != self.workspace:
                # Another workspace's dataset - its triggers are unreadable from
                # this handle, so the walk cannot continue through it. It also
                # cannot close a cycle back to us without a trigger on a foreign
                # dataset targeting one of ours, which is not representable.
                continue
            coll, name = self._local_parts(current)
            for doc in self._triggers_collection(coll, name).stream():
                trigger = doc.to_dict() or {}
                if trigger.get("kind") != MV_REFRESH_TRIGGER_KIND:
                    continue
                downstream = trigger.get("target-view")
                # A target too short to name a dataset is a broken trigger, not
                # an edge; leave it to whoever fires it to complain.
                if not downstream or "." not in downstream:
                    continue
                stack.append((self._qualify(downstream), [*path, current]))

    def create_trigger(
        self,
        dataset_identifier: str,
        name: str,
        target_view: str | None = None,
        statement_id: str | None = None,
        author: str | None = None,
        kind: str = MV_REFRESH_TRIGGER_KIND,
        target_task: str | None = None,
    ) -> None:
        """Attach a trigger to a dataset.

        A trigger is an instruction to the commit path: when this dataset takes
        a user-created data commit, enqueue the reaction described by `kind` -
        in v1 always a materialized-view refresh of `target_view`. Creating a
        trigger is an update to the dataset that carries it, which is why the
        caller-side permission model demands writer on that dataset, not on the
        target view.

        Refuses to close a cycle in the trigger graph - see
        `_assert_no_trigger_cycle` - and refuses to overwrite a trigger of the
        same name aimed at a DIFFERENT view. Trigger names are derived from the target's collection and
        dataset (`_mv_trigger_name`), so two views can in principle want the
        same document; a blind write would leave the first view with no trigger
        and nothing to report it - it would simply never refresh again. The
        digest in the generated name makes this collision implausible, and this
        guard makes it impossible.
        """
        if author is None:
            raise ValueError("author must be provided when creating a trigger")
        # The author becomes `runs-as` below, so the ownership rule applies here
        # too - and applies at creation rather than only on transfer, which is
        # the ordering that actually keeps a platform identity off the field.
        #
        # Gated by EXEMPTION rather than by an allowlist of kinds: every kind
        # carries `runs-as` except the refresh, which resolves its identity from
        # the view's own record and ignores the field. A kind added later
        # therefore arrives gated, and an author who cannot be billed is refused
        # by default rather than by someone remembering to extend a list. That
        # is the direction the mistake should fall in - `http_endpoint` posts a
        # workspace's data outward under a stored credential, and it would have
        # slipped straight through an allowlist keyed on `task`.
        if kind != MV_REFRESH_TRIGGER_KIND:
            _assert_can_own(
                author, f"trigger {name} on {dataset_identifier}", workspace=self.workspace
            )
        collection, dataset_name = self._local_parts(dataset_identifier)
        if not self._dataset_doc_ref(collection, dataset_name).get().exists:
            raise DatasetNotFound(f"Dataset not found: {collection}.{dataset_name}")

        # Which field names the target depends on what the trigger runs. Both are
        # written - one of them None - so a reader never has to know the kind to
        # find the field, and a trigger can never carry two targets at once.
        if target_task is not None:
            if target_view is not None:
                raise ValueError("a trigger targets a view or a task, never both")
            target_task = self._qualify(target_task)
            target, target_field = target_task, "target-task"
        else:
            if target_view is None:
                raise ValueError("a trigger requires a target")
            target_view = self._qualify(target_view)
            target, target_field = target_view, "target-view"

        # Before any write: the trigger graph must stay a DAG. Task targets are
        # not nodes in it - a task declares only its SQL, never what that SQL
        # writes, so there is no edge to draw and no walk that could find a loop
        # through one.
        if target_view is not None:
            self._assert_no_trigger_cycle(dataset_identifier, target_view)

        trigger_ref = self._triggers_collection(collection, dataset_name).document(name)
        existing = trigger_ref.get()
        if existing.exists:
            claimed = (existing.to_dict() or {}).get(target_field)
            if claimed and claimed != target:
                raise MaterializedViewError(
                    f"trigger {name} on {collection}.{dataset_name} already refreshes "
                    f"{claimed}; refusing to repoint it at {target}"
                )

        trigger_ref.set(
            {
                "name": name,
                "kind": kind,
                "target-view": target_view,
                "target-task": target_task,
                "statement-id": statement_id,
                # Preserved across re-registration: repointing or recreating a
                # trigger must not quietly un-pause one an operator paused.
                "suspended-at-ms": (existing.to_dict() or {}).get("suspended-at-ms")
                if existing.exists
                else None,
                "suspended-by": (existing.to_dict() or {}).get("suspended-by")
                if existing.exists
                else None,
                # The identity an UNATTENDED run executes as. On the trigger and
                # not the task, because that is what separates unattended from
                # attended: a person running EXECUTE runs the task as themselves
                # and answers for it, so there is nothing to pin - a trigger
                # fires with nobody present.
                #
                # ALWAYS the author on first registration, with no override.
                # There used to be a `runs_as` argument "for trusted platform
                # callers", and every caller that used it pinned `federator` -
                # which is how five ops ingest triggers came to run unattended
                # with the one credential shipped to every committing service.
                # A creation path that can name someone other than the person
                # creating it is a path for pinning work on an identity that
                # never agreed to carry it; moving an owner is a deliberate,
                # audited act, so it belongs to `set_trigger_owner` alone.
                #
                # Preserved across re-registration so an edit never silently
                # transfers whose authority the work runs with. MV refresh
                # triggers ignore this - a refresh resolves its identity from
                # the view's own record.
                "runs-as": (
                    ((existing.to_dict() or {}).get("runs-as") if existing.exists else None)
                    or author
                ),
                "created-by": author,
                "created-at-ms": int(time.time() * 1000),
                "last-fired-at-ms": None,
                "last-fired-status": None,
            }
        )

        # The existence check at the top and the write above are separate
        # operations, so a concurrent `drop_dataset` can interleave: its sweep
        # of the triggers subcollection runs before this document lands, then
        # the dataset document is deleted - and this trigger survives as the
        # only thing under a ghost path, invisible to every code path but
        # `list_documents()`. Re-checking after the write turns that
        # interleaving into a clean failure instead of an orphan.
        if not self._dataset_doc_ref(collection, dataset_name).get().exists:
            trigger_ref.delete()
            raise DatasetNotFound(f"Dataset not found: {collection}.{dataset_name}")

        emit_audit(
            "create_trigger",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            trigger=name,
            kind=kind,
            target=target,
        )

    def drop_trigger(
        self,
        dataset_identifier: str,
        name: str,
        author: str | None = None,
        missing_ok: bool = False,
    ) -> None:
        """Remove a trigger from a dataset.

        Dropping a materialized view's refresh trigger orphans the view: it
        stays queryable but stops refreshing. That is the supported way to
        pause an MV, and `information_schema.triggers` is where the absence
        shows.

        An author is required, as it is to create one.
        """
        if not author:
            raise ValueError("author must be provided when dropping a trigger")
        collection, dataset_name = self._local_parts(dataset_identifier)
        doc_ref = self._triggers_collection(collection, dataset_name).document(name)
        if not doc_ref.get().exists:
            if missing_ok:
                return
            raise TriggerNotFound(f"Trigger not found: {name} on {collection}.{dataset_name}")
        doc_ref.delete()

        emit_audit(
            "drop_trigger",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            trigger=name,
        )

    def list_triggers(self, dataset_identifier: str) -> list[dict]:
        """All triggers attached to a dataset, as plain dicts."""
        collection, dataset_name = self._local_parts(dataset_identifier)
        results = []
        for doc in self._triggers_collection(collection, dataset_name).stream():
            data = doc.to_dict() or {}
            data.setdefault("name", doc.id)
            results.append(data)
        return results

    # ------------------------------------------------------------------
    # Declared relationships
    # ------------------------------------------------------------------

    def declare_relationship(
        self,
        dataset_identifier: str,
        constraint_name: str,
        column: str,
        references_dataset: str,
        references_column: str,
        cardinality: str,
        author: str | None = None,
    ) -> None:
        """Declare, without enforcing, that `column` corresponds to
        `references_column` on another dataset in this workspace.

        `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... NOT ENFORCED`.
        Nothing validates it now or later: a write that breaks the relationship
        succeeds, and no query plan consults it. It exists so tools and people
        can see how datasets relate.

        Both ends are in this workspace - `_local_parts` refuses anything else,
        the same boundary a trigger's target has - which is what lets one
        dataset's subcollection hold the whole relationship.

        The constraint name is the document id rather than a field, so
        uniqueness is Firestore's rather than ours: a create-if-absent write
        with no read-then-write race to lose, the same reason snapshot tags key
        on the tag name.

        THIS STATEMENT ALSO CONFIRMS A PROPOSAL, and that is why it is not a
        plain create. Inference writes candidate rows that already exist in this
        subcollection (`propose_relationship`), and the design's first answer -
        that an owner confirms one "by issuing the DDL, which flips the row to
        asserted" - could not work against a create-if-absent write: the
        document is already there, so the statement raised "already exists" and
        there was no way to confirm anything. So `ADD CONSTRAINT` means CREATE
        OR UPGRADE:

        - no row for this name and no proposal for this pair -> create, asserted
        - an INFERRED row, by name or by pair -> upgrade it in place to asserted
        - an ASSERTED row -> ValueError, unchanged

        The duplicate-name error therefore still fires for the case it was
        written for. What it no longer does is fire for a proposal, which was
        never a duplicate declaration - it was the thing being confirmed.

        Matching by PAIR as well as by name is what stops a confirmed proposal
        coming back. An owner who types their own constraint name rather than
        the generated one is making exactly the claim the proposal made; if that
        wrote a second document, the proposal would sit beside the asserted row
        and be re-proposed on every subsequent run.
        """
        if not author:
            raise ValueError("author must be provided when declaring a relationship")

        collection, dataset_name = self._local_parts(dataset_identifier)
        if not self._dataset_doc_ref(collection, dataset_name).get().exists:
            raise DatasetNotFound(f"Dataset not found: {collection}.{dataset_name}")

        references = self._qualify(references_dataset)
        # Refuses a far end in another workspace, and does it before writing.
        far_collection, far_dataset = self._local_parts(references)
        if not self._dataset_doc_ref(far_collection, far_dataset).get().exists:
            raise DatasetNotFound(f"Dataset not found: {far_collection}.{far_dataset}")

        relationships = self._relationships_collection(collection, dataset_name)
        digest = relationship_pair_digest(
            column, far_collection, far_dataset, references_column
        )

        # The proposal this statement might be confirming, found either way it
        # can be named: by the generated name the Studio's confirm button sends
        # back, or by the pair, for an owner who typed a name of their own.
        proposal = None
        named = relationships.document(constraint_name).get()
        if named.exists:
            existing = named.to_dict() or {}
            if existing.get("origin") == "inferred":
                proposal = named
            else:
                # The case the original error was written for, and it still
                # fires: a second declaration cannot share a handle with the
                # first, because the handle is all `DROP CONSTRAINT` has.
                raise ValueError(
                    f"constraint {constraint_name} already exists on {collection}.{dataset_name}"
                )
        else:
            # Streamed and filtered here rather than asked of Firestore. A
            # `where` on `pair-digest` would need the field path backtick-quoted
            # (every field this catalog stores is hyphenated, and an unquoted
            # path must match `[a-zA-Z_][a-zA-Z_0-9]*`), and it would cost the
            # same one read: this subcollection holds a dataset's own
            # relationships, which number in the tens.
            for candidate in relationships.stream():
                data = candidate.to_dict() or {}
                if data.get("origin") == "inferred" and data.get("pair-digest") == digest:
                    proposal = candidate
                    break

        # An upgrade REPLACES the proposal's document rather than editing it,
        # so a confirmation under a name of the owner's own choosing ends up
        # under that name and the generated one is gone. Anything else leaves
        # two handles for one relationship, only one of which drops it.
        if proposal is not None and proposal.id != constraint_name:
            relationships.document(proposal.id).delete()

        proposed = (proposal.to_dict() or {}) if proposal is not None else {}

        relationships.document(constraint_name).set(
            {
                "name": constraint_name,
                "kind": "maps",
                # The NEAR end, denormalised from the path this document already
                # lives at. Redundant for the forward read, where the parent
                # says it - and required for the reverse one, which arrives via
                # a collection group query holding a document and no context
                # about whose child it is. This is not a mirrored row: it is one
                # row carrying its own address, so there is nothing to drift.
                "workspace": self.workspace,
                "collection": collection,
                "dataset": dataset_name,
                "column": column,
                # Stored SPLIT, never as a dotted string: a dataset name may
                # contain dots and every consumer that re-parsed one would have
                # to agree where the boundaries are.
                "references-workspace": self.workspace,
                "references-collection": far_collection,
                "references-dataset": far_dataset,
                "references-column": references_column,
                # Declared, never derived - nothing samples the data to check.
                # A confirmation OVERWRITES whatever cardinality was inferred:
                # the person saying many-to-one is the authority, and getting
                # this wrong is how a fan-out join silently inflates a number.
                "cardinality": cardinality,
                "origin": "asserted",
                "status": "active",
                "asserted-by": author,
                "asserted-at-ms": int(time.time() * 1000),
                "verified-at-ms": None,
                # Keyed on the pair so a later proposal of the same claim finds
                # this row and does not write itself beside it.
                "pair-digest": digest,
                # What was proposed, kept after confirmation rather than
                # cleared. `confidence` and `evidence` are how this row came to
                # be looked at, and an owner reviewing the graph later is
                # entitled to see that it began as a machine proposal and on
                # what evidence. Both are absent on a relationship nobody
                # proposed, which is the honest thing for a row that was typed
                # from knowledge rather than measured.
                "confidence": proposed.get("confidence"),
                "evidence": proposed.get("evidence"),
                "proposed-at-ms": proposed.get("proposed-at-ms"),
                "proposed-by": proposed.get("proposed-by"),
            }
        )

        emit_audit(
            "confirm_relationship" if proposal is not None else "declare_relationship",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            constraint=constraint_name,
            references=f"{far_collection}.{far_dataset}",
        )

    def propose_relationship(
        self,
        dataset_identifier: str,
        column: str,
        references_dataset: str,
        references_column: str,
        cardinality: str,
        confidence: float,
        evidence: dict,
        proposer: str,
    ) -> str | None:
        """Record a relationship inference SUGGESTS, for a person to judge.

        Returns the constraint name written, or None if the pair was already
        settled - suppressed by a rejection, or already asserted by someone.

        NOT REACHABLE FROM DDL, and that is the point.
        `ALTER TABLE ... ADD CONSTRAINT` means asserted by definition: someone
        wrote the statement, so a person is standing behind the claim. A
        proposal has nobody standing behind it, so it gets its own entrance and
        the inference job calls this directly. There is no SQL for it, and there
        should not be - a statement that wrote `origin='inferred'` would let a
        person disclaim a claim they had in fact made.

        `status='unverified'`, so the row is VISIBLE BUT INERT: it appears in
        `information_schema.column_relationships` and in the Studio carrying its
        evidence, and it is filtered out of every OData surface that a BI client
        would act on. A `NavigationProperty` is a promise that joining two
        entity sets returns rows, and that is not something a guess may say.

        Idempotent on the pair. The constraint name is generated from the pair
        alone, so a second run over unchanged data addresses the same document
        and updates its score; it does not accumulate a proposal per run. What
        does NOT survive a re-proposal is anything a person did to the row - an
        asserted row is left alone, and a suppressed pair is not written at all.

        Args:
            column: the near end, on this dataset
            references_dataset: the far end's dataset, in this workspace
            confidence: 0..1, the job's own score
            evidence: what was observed - overlap ratio, sample size, the
                cardinalities compared. Stored as a map and shown to the owner
                in full, because a bare score is not something anyone can judge.
            proposer: the job identity, recorded where a person's name goes on
                an assertion. It must be visibly not a person.
        """
        if not proposer:
            raise ValueError("proposer must be provided when proposing a relationship")
        if not 0.0 <= float(confidence) <= 1.0:
            raise ValueError(f"confidence must be between 0 and 1, got {confidence!r}")

        collection, dataset_name = self._local_parts(dataset_identifier)
        if not self._dataset_doc_ref(collection, dataset_name).get().exists:
            raise DatasetNotFound(f"Dataset not found: {collection}.{dataset_name}")

        far_collection, far_dataset = self._local_parts(self._qualify(references_dataset))
        if not self._dataset_doc_ref(far_collection, far_dataset).get().exists:
            raise DatasetNotFound(f"Dataset not found: {far_collection}.{far_dataset}")

        digest = relationship_pair_digest(column, far_collection, far_dataset, references_column)

        # A rejected pair is not proposed again. Checked here rather than left
        # to the job because the job is not the only caller that could exist,
        # and a suppression that only holds when the caller remembers to look
        # is not a suppression.
        suppression = self._relationship_suppressions_collection(
            collection, dataset_name
        ).document(digest)
        if suppression.get().exists:
            return None

        relationships = self._relationships_collection(collection, dataset_name)
        constraint_name = generated_constraint_name(
            column, far_collection, far_dataset, references_column
        )

        # Never overwrite a person. An asserted row for this pair - under the
        # generated name or under one an owner chose - means the question is
        # already answered, and a proposal beside it would ask it again.
        # Streamed rather than queried for the reason `declare_relationship`
        # gives: the field is hyphenated and the subcollection is tens of rows.
        for existing_doc in relationships.stream():
            data = existing_doc.to_dict() or {}
            if data.get("origin") == "inferred":
                continue
            if existing_doc.id == constraint_name or data.get("pair-digest") == digest:
                return None
            if (
                (data.get("column") or "").casefold() == (column or "").casefold()
                and (data.get("references-collection") or "").casefold()
                == far_collection.casefold()
                and (data.get("references-dataset") or "").casefold() == far_dataset.casefold()
                and (data.get("references-column") or "").casefold()
                == (references_column or "").casefold()
            ):
                # An asserted row written before `pair-digest` existed. Matched
                # on its parts so the first inference run against an existing
                # catalog does not re-propose every relationship already
                # declared by hand.
                return None

        relationships.document(constraint_name).set(
            {
                "name": constraint_name,
                "kind": "maps",
                "workspace": self.workspace,
                "collection": collection,
                "dataset": dataset_name,
                "column": column,
                "references-workspace": self.workspace,
                "references-collection": far_collection,
                "references-dataset": far_dataset,
                "references-column": references_column,
                # Inferred from the shape of the data, and the weakest part of
                # the proposal: a cardinality read off statistics is a guess
                # about a join's fan-out, which is why confirming overwrites it
                # with what the person says.
                "cardinality": cardinality,
                "origin": "inferred",
                "status": "unverified",
                "confidence": float(confidence),
                "evidence": dict(evidence or {}),
                "pair-digest": digest,
                "proposed-by": proposer,
                "proposed-at-ms": int(time.time() * 1000),
                # An assertion's fields, deliberately empty. A proposal has no
                # author and has not been verified; filling either in with the
                # job's name would make a guess look like a person's statement
                # to every consumer that reads these fields.
                "asserted-by": None,
                "asserted-at-ms": None,
                "verified-at-ms": None,
            }
        )

        emit_audit(
            "propose_relationship",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=proposer,
            constraint=constraint_name,
            references=f"{far_collection}.{far_dataset}",
        )
        return constraint_name

    def reject_relationship(
        self,
        dataset_identifier: str,
        constraint_name: str,
        author: str | None = None,
        reason: str | None = None,
    ) -> bool:
        """Reject one proposal, and record that the pair is not to be proposed again.

        Returns True if a proposal was rejected.

        Two writes, and the second is the one that matters. Deleting the
        proposal without recording the rejection would leave the owner facing
        the same claim after the next run, which is how a review queue stops
        being reviewed.

        THE SUPPRESSION KEYS ON THE PAIR, not on this constraint name - see
        `RELATIONSHIP_SUPPRESSIONS_SUBCOLLECTION`. The name is generated, so a
        record against it would be defeated by the next run generating another.

        Only a proposal can be rejected. An asserted relationship is removed
        with `DROP CONSTRAINT`, which says what it does and leaves no
        suppression behind - dropping a relationship someone declared should not
        quietly forbid inference from ever raising it again.
        """
        if not author:
            raise ValueError("author must be provided when rejecting a relationship")

        collection, dataset_name = self._local_parts(dataset_identifier)
        doc_ref = self._relationships_collection(collection, dataset_name).document(constraint_name)
        snapshot = doc_ref.get()
        if not snapshot.exists:
            raise ConstraintNotFound(
                f"Constraint not found: {constraint_name} on {collection}.{dataset_name}"
            )

        data = snapshot.to_dict() or {}
        if data.get("origin") != "inferred":
            raise ValueError(
                f"constraint {constraint_name} on {collection}.{dataset_name} was asserted, "
                "not proposed - remove it with DROP CONSTRAINT"
            )

        digest = data.get("pair-digest") or relationship_pair_digest(
            data.get("column") or "",
            data.get("references-collection") or "",
            data.get("references-dataset") or "",
            data.get("references-column") or "",
        )

        # The suppression goes in FIRST. If the second write fails the owner
        # sees the proposal again and can reject it again, which is a nuisance;
        # the other order loses the rejection and re-proposes forever, which is
        # the failure this method exists to prevent.
        self._relationship_suppressions_collection(collection, dataset_name).document(digest).set(
            {
                "pair-digest": digest,
                "workspace": self.workspace,
                "collection": collection,
                "dataset": dataset_name,
                "column": data.get("column"),
                "references-collection": data.get("references-collection"),
                "references-dataset": data.get("references-dataset"),
                "references-column": data.get("references-column"),
                # What was rejected, kept so the record can be read back as a
                # decision rather than as a bare prohibition: someone reviewing
                # this later needs to know the claim was rejected at 0.94
                # confidence, not merely that it was rejected.
                "rejected-confidence": data.get("confidence"),
                "rejected-evidence": data.get("evidence"),
                "rejected-by": author,
                "rejected-at-ms": int(time.time() * 1000),
                "reason": reason,
            }
        )
        doc_ref.delete()

        emit_audit(
            "reject_relationship",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            constraint=constraint_name,
            references=(
                f"{data.get('references-collection')}.{data.get('references-dataset')}"
            ),
        )
        return True

    def list_relationship_suppressions(self, dataset_identifier: str) -> list[dict]:
        """The column pairs this dataset's owner has rejected.

        Read by the inference job before it proposes, and by nothing else -
        a suppression is not part of the graph and does not appear in the
        projection. It records the absence of a relationship, and
        `information_schema.column_relationships` lists relationships.
        """
        collection, dataset_name = self._local_parts(dataset_identifier)
        results = []
        for doc in self._relationship_suppressions_collection(collection, dataset_name).stream():
            data = doc.to_dict() or {}
            data.setdefault("pair-digest", doc.id)
            results.append(data)
        return results

    def drop_relationship(
        self,
        dataset_identifier: str,
        constraint_name: str,
        author: str | None = None,
        missing_ok: bool = False,
    ) -> bool:
        """Remove one declared relationship by name.

        Returns True if one was removed, False if there was none and
        `missing_ok` allowed that - a drop that silently matched nothing would
        let a typo read as success.

        DROPPING A PROPOSAL IS REJECTING IT, and that is why this branches. An
        `origin='inferred'` row is a question nobody has answered; there is no
        reason to remove one except to answer "no", and if the removal left no
        record the next inference run would ask again. So a drop against a
        proposal records the suppression `reject_relationship` writes.

        This also gives the Studio a write path that needs no new grammar. The
        confirm button emits `ADD CONSTRAINT` and the dismiss button emits
        `DROP CONSTRAINT`, both of which the engine already parses, and both of
        which are a person making a statement - which is the only kind of thing
        DDL should carry (see `propose_relationship` for the other side of
        that rule).

        Dropping an ASSERTED relationship is unchanged and leaves nothing
        behind. Removing something a person declared should not quietly forbid
        inference from ever raising it again.
        """
        if not author:
            raise ValueError("author must be provided when dropping a relationship")

        collection, dataset_name = self._local_parts(dataset_identifier)
        doc_ref = self._relationships_collection(collection, dataset_name).document(constraint_name)
        snapshot = doc_ref.get()
        if not snapshot.exists:
            if missing_ok:
                return False
            raise ConstraintNotFound(
                f"Constraint not found: {constraint_name} on {collection}.{dataset_name}"
            )
        if (snapshot.to_dict() or {}).get("origin") == "inferred":
            return self.reject_relationship(dataset_identifier, constraint_name, author=author)
        doc_ref.delete()

        emit_audit(
            "drop_relationship",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            constraint=constraint_name,
        )
        return True

    # -- Verification and decay (design §9) -----------------------------
    #
    # SPLIT INTO TWO MECHANISMS, deliberately, and the design's single periodic
    # checker was wrong for half of it.
    #
    # EXISTENCE is checked WHEN THE THING STOPS EXISTING - here, on the write
    # that removes it. A rename, a drop or a dropped column is the moment the
    # fact is known for certain and for nothing: the write is already holding
    # the identifiers, and the reverse lookup answers "what points at this" in
    # one query. Scanning for missing endpoints later would re-derive, at a
    # cost and after a delay, something a write path had in hand.
    #
    # VALUES are re-checked by the inference job, which builds the sketches a
    # re-score needs anyway - see `opteryx_catalog.inference.relationships`.
    #
    # A BROKEN EDGE IS KEPT. "These used to correspond and no longer do" is
    # information; deleting it hides a data problem instead of surfacing one.

    def _mark_relationship_broken(
        self,
        collection: str,
        dataset_name: str,
        constraint_name: str,
        *,
        reason: str,
        detail: str,
        author: str | None,
    ) -> None:
        """Flip one relationship to `broken`, keeping everything it recorded.

        A MERGE, never a set. The row's `evidence` is the last thing anyone
        measured about this pair, and it is what makes a break readable as
        "these used to correspond" rather than as a bare flag - so this adds
        fields and overwrites `status`, and touches nothing else.

        `verified-at-ms` is stamped because an existence check IS a check, and
        the strongest kind: it did not sample, it observed. A reader comparing
        that stamp against today is entitled to know this was settled now.
        """
        self._relationships_collection(collection, dataset_name).document(constraint_name).set(
            {
                "status": "broken",
                # Why, in two registers. `broken-reason` is a token a consumer
                # can branch on; `broken-detail` is the sentence a person
                # reads, and it names what went missing - which is the only
                # part that makes the row actionable rather than alarming.
                "broken-reason": reason,
                "broken-detail": detail,
                "broken-at-ms": int(time.time() * 1000),
                "verified-at-ms": int(time.time() * 1000),
            },
            merge=True,
        )

        emit_audit(
            "break_relationship",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            constraint=constraint_name,
            reason=reason,
            detail=detail,
        )

    def _break_inbound_relationships(
        self,
        collection: str,
        dataset_name: str,
        *,
        reason: str,
        detail: str,
        author: str | None,
        column: str | None = None,
    ) -> list[dict]:
        """Break every relationship elsewhere that points at this dataset.

        THE DESIGN SAID NOT TO DO THIS, and the reasoning has been superseded.
        `rename_dataset` used to leave inbound references stale on purpose,
        because "chasing inbound references would mean scanning every dataset
        in the workspace". That was true when it was written and is not now:
        `find_relationships_to` is a collection group query - ONE indexed read,
        the same one `information_schema.column_relationships` already makes on
        every `$metadata` build. The punt now costs more than the fix.

        `column` narrows it to references at one column, which is what
        `ALTER TABLE ... DROP COLUMN` needs; without it the whole dataset went.

        Returns the rows it broke, so the caller can say who to tell. Each one
        was ASSERTED BY SOMEONE ELSE, on a dataset this author may not own -
        which is exactly why the return value matters and a status column
        alone would not be enough.
        """
        broken = []
        for row in self.find_relationships_to(f"{collection}.{dataset_name}"):
            if column is not None:
                if (row.get("references-column") or "").casefold() != column.casefold():
                    continue
            near_collection = row.get("collection")
            near_dataset = row.get("dataset")
            name = row.get("name")
            if not (near_collection and near_dataset and name):
                # A row with no near address cannot be located to be updated.
                # It also cannot be rendered, since the projection reads these
                # same fields, so this is an integrity problem rather than a
                # verification one - `integrity.py` is where it belongs.
                logger.warning(
                    "inbound relationship with no near address on %s.%s: %r",
                    collection,
                    dataset_name,
                    name,
                )
                continue
            if row.get("status") == "broken":
                continue
            self._mark_relationship_broken(
                near_collection,
                near_dataset,
                name,
                reason=reason,
                detail=detail,
                author=author,
            )
            broken.append(row)
        return broken

    def _repoint_inbound_relationships(
        self,
        old_collection: str,
        old_dataset: str,
        new_collection: str,
        new_dataset: str,
        *,
        author: str | None,
    ) -> list[dict]:
        """Follow every inbound reference across a rename.

        REPOINTED, NOT BROKEN, and the distinction is the whole reason rename
        and drop are handled separately. A renamed dataset is the same dataset:
        its snapshots, tags and relationships all travel, so a reference to it
        is still a reference to the data it names. Marking those broken would
        report a data problem where there is none, and an owner who "fixed" it
        by dropping the constraint would lose a true relationship to a
        bookkeeping event.

        A drop is the opposite - the referenced thing is gone - and that is
        what `_break_inbound_relationships` is for.

        This writes to relationship documents under OTHER datasets, which the
        renamer may not own. That is repair, not escalation: the reference
        already named this dataset, and the alternative is leaving a row that
        points at nothing. Nothing new is disclosed - the rows are not returned
        to a caller, only to the audit trail.
        """
        repointed = []
        for row in self.find_relationships_to(f"{old_collection}.{old_dataset}"):
            near_collection = row.get("collection")
            near_dataset = row.get("dataset")
            name = row.get("name")
            if not (near_collection and near_dataset and name):
                logger.warning(
                    "inbound relationship with no near address on %s.%s: %r",
                    old_collection,
                    old_dataset,
                    name,
                )
                continue
            self._relationships_collection(near_collection, near_dataset).document(name).set(
                {
                    "references-collection": new_collection,
                    "references-dataset": new_dataset,
                },
                merge=True,
            )
            emit_audit(
                "repoint_relationship",
                resource_type=ResourceType.DATASET,
                workspace=self.workspace,
                collection=near_collection,
                resource=near_dataset,
                author=author,
                constraint=name,
                references=f"{new_collection}.{new_dataset}",
                previous_references=f"{old_collection}.{old_dataset}",
            )
            repointed.append(row)
        return repointed

    def relationships_through_column(self, dataset_identifier: str, column: str) -> list[dict]:
        """Every relationship that would point at nothing if `column` went.

        BOTH DIRECTIONS, and they are not equivalent to the caller:

          * outbound - declared ON this dataset, at this column. Its author is
            this dataset's owner, so naming it back to them discloses nothing.
          * inbound - declared elsewhere, referencing this column. Its far end
            is a dataset the asker may hold no grant on, so these rows are
            marked `"inbound": True` and a caller that renders a message MUST
            NOT name them (§8.2). They are here to be broken and to be
            notified about, not to be shown to whoever dropped the column.

        Read at plan time by `ALTER TABLE ... DROP COLUMN`, which is the whole
        point: telling someone before the damage beats recording it after.
        """
        collection, dataset_name = self._local_parts(dataset_identifier)
        target = (column or "").casefold()

        through = []
        for row in self.list_relationships(dataset_identifier):
            if (row.get("column") or "").casefold() == target:
                through.append({**row, "inbound": False})
        for row in self.find_relationships_to(f"{collection}.{dataset_name}"):
            if (row.get("references-column") or "").casefold() == target:
                through.append({**row, "inbound": True})
        return through

    def break_relationships_through_column(
        self,
        dataset_identifier: str,
        column: str,
        *,
        author: str | None = None,
    ) -> list[dict]:
        """Mark broken every relationship that ran through a dropped column.

        Called AFTER the column is gone, by whoever executed the drop. The
        plan-time check refuses the case worth refusing (an asserted outbound
        relationship, §6.1); what reaches here is what was allowed through -
        proposals, and inbound references the dropper was deliberately not
        told about because naming them would disclose a dataset they cannot
        read.

        Returns what it broke, including inbound rows, so the caller can notify
        the people who asserted them. That notification is the point: an
        inbound reference nobody is told about is a status field nobody reads.
        """
        collection, dataset_name = self._local_parts(dataset_identifier)
        detail = f"column {collection}.{dataset_name}.{column} was dropped"

        broken = []
        for row in self.list_relationships(dataset_identifier):
            if (row.get("column") or "").casefold() != (column or "").casefold():
                continue
            if row.get("status") == "broken":
                continue
            self._mark_relationship_broken(
                collection,
                dataset_name,
                row.get("name"),
                reason="column-dropped",
                detail=detail,
                author=author,
            )
            broken.append({**row, "inbound": False})

        broken.extend(
            {**row, "inbound": True}
            for row in self._break_inbound_relationships(
                collection,
                dataset_name,
                reason="column-dropped",
                detail=detail,
                author=author,
                column=column,
            )
        )
        return broken

    def record_relationship_verification(
        self,
        dataset_identifier: str,
        constraint_name: str,
        *,
        status: str,
        evidence: dict | None = None,
        confidence: float | None = None,
        cardinality_observed: str | None = None,
        verifier: str | None = None,
    ) -> None:
        """Write back what a re-score of a declared pair observed.

        A NARROW MERGE over a row the job already located, which is the one
        bulk write the cluster key does nothing for - and the reason this takes
        a constraint name rather than re-deriving one.

        `status=None` records the MEASUREMENT and leaves the status alone,
        which is the honest outcome more often than it looks. A score that is
        neither good enough to confirm nor bad enough to condemn should move
        `verified-at-ms` and the evidence and nothing else - and a proposal
        stays 'unverified' whatever it scores, because a machine re-measuring
        its own guess does not turn it into a claim; only a person's
        confirmation does (§7.4).

        `cardinality_observed` is recorded BESIDE the declared cardinality and
        never over it. The person who wrote `many_to_one` is the authority on
        what the relationship means; a fan-out read off statistics is a
        measurement of what the data currently is. Where they disagree, both
        belong on the row - overwriting would destroy the disagreement, which
        is the finding.
        """
        if status is not None and status not in {"active", "broken", "unverified"}:
            raise ValueError(f"unknown verification status: {status!r}")

        collection, dataset_name = self._local_parts(dataset_identifier)
        now_ms = int(time.time() * 1000)
        payload: dict = {
            "verified-at-ms": now_ms,
            "verified-by": verifier,
            "cardinality-observed": cardinality_observed,
        }
        if status is not None:
            payload["status"] = status
        if evidence is not None:
            payload["evidence"] = dict(evidence)
        if confidence is not None:
            payload["confidence"] = float(confidence)
        if status == "broken":
            payload["broken-reason"] = "values-diverged"
            payload["broken-detail"] = (
                "the values no longer correspond - see this row's evidence for "
                "what was compared"
            )
            payload["broken-at-ms"] = now_ms
        elif status == "active":
            # A repair is as much a fact as a break. Clearing these stops a row
            # that has come back from reading as broken forever, which is how a
            # status column stops being believed. Only an explicit 'active'
            # clears them: an inconclusive re-score (`status=None`) knows
            # nothing about why the row broke and must not erase the answer.
            payload["broken-reason"] = None
            payload["broken-detail"] = None
            payload["broken-at-ms"] = None

        self._relationships_collection(collection, dataset_name).document(constraint_name).set(
            payload, merge=True
        )

        emit_audit(
            "verify_relationship",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=verifier,
            constraint=constraint_name,
            status=status,
            cardinality_observed=cardinality_observed,
        )

    def find_relationships_to(self, dataset_identifier: str) -> list[dict]:
        """Every relationship pointing AT this dataset, from anywhere in this
        workspace - the reverse of `list_relationships`.

        A collection group query, which matches every `relationships`
        subcollection in the database whatever its parent, so this is one
        indexed read rather than a walk over every dataset in the workspace.
        That is what makes it affordable to store one row per relationship
        instead of a mirrored copy keyed the other way.

        THE WORKSPACE FILTER IS LOAD-BEARING. Workspaces are root collections in
        a single database, so a collection group query crosses all of them: this
        would return other workspaces' relationships if the filter were dropped
        or mistyped. It is sound because a relationship cannot cross a workspace
        - refused at plan time - so `references-workspace` always equals the
        workspace whose datasets are being asked about. That makes the filter an
        invariant rather than a hope, but it is still the one place in this
        feature where visibility depends on a predicate being right, and a
        caller must not treat these rows as already authorized: whoever renders
        them still owes a read check on each near end.

        REQUIRES A COLLECTION GROUP INDEX on `relationships` over
        (references-workspace, references-collection, references-dataset).
        Collection-group scope is not automatic; until the index exists Firestore
        refuses the query outright rather than answering it slowly.

        THE BACKTICKS ARE LOAD-BEARING. Every field this catalog stores is
        hyphenated (`created-by`, `target-view`, and these), which is fine until
        one is used in a QUERY: an unquoted Firestore property path must match
        `[a-zA-Z_][a-zA-Z_0-9]*`, so a bare "references-workspace" is rejected
        outright with InvalidArgument. Quoting is the documented escape and
        changes nothing about how the field is stored.
        """
        from google.cloud.firestore_v1 import FieldFilter

        collection, dataset_name = self._local_parts(dataset_identifier)
        query = (
            self.firestore_client.collection_group(RELATIONSHIPS_SUBCOLLECTION)
            .where(filter=FieldFilter("`references-workspace`", "==", self.workspace))
            .where(filter=FieldFilter("`references-collection`", "==", collection))
            .where(filter=FieldFilter("`references-dataset`", "==", dataset_name))
        )
        results = []
        for doc in query.stream():
            data = doc.to_dict() or {}
            data.setdefault("name", doc.id)
            results.append(data)
        return results

    def list_relationships(self, dataset_identifier: str) -> list[dict]:
        """Every relationship declared ON this dataset, as plain dicts.

        Only the near side - what this dataset points at. The reverse question
        is `find_relationships_to`, which is a collection group query rather
        than the mirrored row an earlier draft of the design proposed.
        """
        collection, dataset_name = self._local_parts(dataset_identifier)
        results = []
        for doc in self._relationships_collection(collection, dataset_name).stream():
            data = doc.to_dict() or {}
            data.setdefault("name", doc.id)
            results.append(data)
        return results

    def list_workspace_relationships(self) -> list[dict]:
        """Every relationship declared anywhere in this workspace, as plain dicts.

        ONE collection group query, not a walk. The alternative - enumerate the
        collections, enumerate each one's datasets, then read each dataset's
        subcollection - costs `1 + collections + datasets` sequential round
        trips to return a set of rows that numbers in the tens, and it grows
        with the size of the workspace rather than with the number of
        relationships in it. Measured on a 25-dataset workspace: 31 round trips
        and 10.1s, against 150ms for this query, for an identical row set.

        Every document carries its own near address (`workspace`, `collection`,
        `dataset`), denormalised by `declare_relationship` from the path it
        lives at, so a row that arrives here with no context about whose child
        it is still says which dataset declared it. That is what makes the
        walk redundant rather than merely slow.

        THE WORKSPACE FILTER IS LOAD-BEARING, exactly as in
        `find_relationships_to`. Workspaces are root collections in a single
        database, so a collection group query crosses all of them and this
        would return other tenants' relationships if the filter were dropped or
        mistyped. It is sound because a relationship cannot cross a workspace -
        refused at plan time - so `workspace` is an invariant rather than a
        hope. The re-check below cannot fire against a correct query; it is
        there so that an edit which breaks the filter fails loudly here instead
        of quietly widening what the caller downstream is handed.

        Unlike `find_relationships_to`'s three, this field needs NO backticks:
        `workspace` already matches Firestore's bare property path grammar. It
        needs no declared composite index either - the automatic single-field
        index carries it, provided collection group scope has not been removed
        by a field exemption.

        The rows are NOT authorized. Both ends of every row still owe a read
        check to whoever renders them - see
        `information_schema.column_relationships`, which is this method's only
        caller and makes both.
        """
        from google.cloud.firestore_v1 import FieldFilter

        query = self.firestore_client.collection_group(RELATIONSHIPS_SUBCOLLECTION).where(
            filter=FieldFilter("workspace", "==", self.workspace)
        )
        results = []
        for doc in query.stream():
            data = doc.to_dict() or {}
            if data.get("workspace") != self.workspace:
                raise ValueError(
                    "collection group query returned a relationship from workspace "
                    f"{data.get('workspace')!r} while reading {self.workspace!r} - "
                    "the workspace filter is the only thing confining this query to "
                    "one tenant and it is not holding"
                )
            data.setdefault("name", doc.id)
            results.append(data)
        return results

    def set_trigger_suspended(
        self,
        dataset_identifier: str,
        name: str,
        suspended: bool,
        author: str | None = None,
    ) -> None:
        """Suspend or resume one trigger.

        A suspended trigger still exists and `fire_triggers` still reaches it;
        it enqueues nothing and records why. Deliberately distinct from dropping
        it - the suppression stays visible where an operator looks for the reason
        a table stopped updating, rather than leaving no trace at all.
        """
        collection, dataset_name = self._local_parts(dataset_identifier)
        ref = self._triggers_collection(collection, dataset_name).document(name)
        if not ref.get().exists:
            raise TriggerNotFound(f"Trigger not found: {name} on {collection}.{dataset_name}")
        ref.update(
            {
                "suspended-at-ms": int(time.time() * 1000) if suspended else None,
                "suspended-by": author if suspended else None,
            }
        )

        emit_audit(
            "suspend_trigger" if suspended else "resume_trigger",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            trigger=name,
        )

    def set_trigger_owner(
        self,
        dataset_identifier: str,
        name: str,
        new_owner: str,
        author: str | None = None,
    ) -> None:
        """Repoint the identity a trigger's unattended runs execute as.

        A platform identity is refused here, not merely by whatever surface the
        caller came in through. The engine's binder asks the same question of
        `ALTER TRIGGER ... OWNER TO`, but this method is reachable by anything
        holding the library, and the direct calls are exactly how the ops ingest
        triggers came to be pinned to `federator` - a check only the SQL path
        applied was a check the SQL path alone obeyed.
        """
        _assert_can_own(
            new_owner, f"trigger {name} on {dataset_identifier}", workspace=self.workspace
        )
        collection, dataset_name = self._local_parts(dataset_identifier)
        ref = self._triggers_collection(collection, dataset_name).document(name)
        if not ref.get().exists:
            raise TriggerNotFound(f"Trigger not found: {name} on {collection}.{dataset_name}")
        ref.update({"runs-as": new_owner})

        emit_audit(
            "alter_trigger_owner",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            trigger=name,
            new_owner=new_owner,
        )

    def mark_trigger_fired(self, dataset_identifier: str, name: str, status: str) -> None:
        """Stamp a trigger's last-fired fields. Called by the enqueue path."""
        collection, dataset_name = self._local_parts(dataset_identifier)
        self._triggers_collection(collection, dataset_name).document(name).update(
            {
                "last-fired-at-ms": int(time.time() * 1000),
                "last-fired-status": status,
            }
        )

    # ------------------------------------------------------------------
    # Snapshot tags
    # ------------------------------------------------------------------

    def _tags_collection(self, collection: str, dataset_name: str):
        return self._dataset_doc_ref(collection, dataset_name).collection(TAGS_SUBCOLLECTION)

    @staticmethod
    def normalize_tag_name(name: str) -> str:
        """Validate a tag name and return its canonical (lowercase) spelling.

        Tag names are SQL identifiers, and they are normalized to lowercase on
        the way in: `MyTag` and `mytag` are one tag with one spelling, and
        nothing downstream has to remember which casing was typed. The
        normalized name is also the Firestore document id, so document-id
        uniqueness and tag-name uniqueness are the same constraint rather than
        two that could disagree.
        """
        if not isinstance(name, str) or not name:
            raise ValueError("A tag name is required.")
        if len(name) > MAX_TAG_NAME_LENGTH:
            raise ValueError(
                f"Tag name is {len(name)} characters; the maximum is {MAX_TAG_NAME_LENGTH}."
            )
        if not _TAG_NAME_PATTERN.match(name):
            raise ValueError(
                f"'{name}' is not a valid tag name. A tag name starts with a letter and "
                "contains only letters, digits and underscores - no dots, no hyphens."
            )
        normalized = name.lower()
        if normalized in RETIRED_TAG_NAMES:
            raise ValueError(
                f"'{normalized}' is a retired name and cannot be used for a tag. "
                "The snapshot pointer once spelled `latest` is now `current`; use "
                "that. The old spelling is not accepted as a tag either, because a "
                "tag of that name would make `VERSION AS OF latest` resolve again - "
                "to one frozen snapshot - instead of failing."
            )
        if normalized in RESERVED_TAG_NAMES:
            raise ValueError(
                f"'{normalized}' is a reserved name and cannot be used for a tag. "
                "It names the snapshot the dataset points at now, or the version "
                "behind it, and a tag is immutable - one holding that word would "
                "take it over and then never move again. Reserved: "
                f"{', '.join(sorted(RESERVED_TAG_NAMES))}."
            )
        return normalized

    def create_tag(
        self,
        dataset_identifier: str,
        name: str,
        snapshot_id: int,
        author: str | None = None,
        comment: str | None = None,
    ) -> dict:
        """Bind a name to one snapshot, and pin that snapshot from expiry.

        A tag is immutable and immortal: the binding never changes, and nothing
        ages it out. It holds its snapshot - and every data file that snapshot
        references - alive until someone drops it, and the storage it holds is
        charged. Creating one is therefore an open-ended cost commitment, which
        is why the returned record carries the pinned byte count for the caller
        to report.

        The liveness check and the create are ONE transaction across two
        documents. Expiration retires a snapshot by writing its snapshot
        document; a read-then-write here could create a tag against a snapshot
        being tombstoned in the same instant, which is precisely the dangling
        tag that pinning exists to make impossible.
        """
        if author is None:
            raise ValueError("author must be provided when creating a tag")

        tag_name = self.normalize_tag_name(name)
        collection, dataset_name = self._local_parts(dataset_identifier)
        if not self._dataset_doc_ref(collection, dataset_name).get().exists:
            raise DatasetNotFound(f"Dataset not found: {collection}.{dataset_name}")

        snapshot_id = int(snapshot_id)
        tags_collection = self._tags_collection(collection, dataset_name)
        tag_ref = tags_collection.document(tag_name)
        snapshot_ref = self._snapshots_collection(collection, dataset_name).document(
            str(snapshot_id)
        )
        qualified = f"{collection}.{dataset_name}"

        @firestore.transactional
        def _create(transaction) -> dict:
            # Every read first: a Firestore transaction refuses a read that
            # follows a write in the same transaction.
            snapshot_doc = snapshot_ref.get(transaction=transaction)
            existing = tag_ref.get(transaction=transaction)
            held = sum(1 for _ in tags_collection.stream(transaction=transaction))

            if not snapshot_doc.exists:
                raise SnapshotMissingError(
                    f"No snapshot {snapshot_id} for {qualified} - it may not exist, "
                    "or may have expired."
                )
            if snapshot_is_tombstoned(snapshot_doc.to_dict() or {}):
                raise SnapshotMissingError(
                    f"Snapshot {snapshot_id} of {qualified} has expired and cannot be tagged."
                )
            if existing.exists:
                bound = (existing.to_dict() or {}).get("snapshot-id")
                raise TagAlreadyExists(
                    f"Tag {tag_name} already exists on {qualified} and names snapshot "
                    f"{bound}. Tags are immutable - drop it first to release that "
                    "snapshot, then create it again."
                )
            if held >= MAX_TAGS_PER_DATASET:
                raise TagLimitExceeded(
                    f"{qualified} already holds {held} tags, the maximum is "
                    f"{MAX_TAGS_PER_DATASET}. Each tag pins its snapshot's storage "
                    "indefinitely; drop one that is no longer needed."
                )

            summary = (snapshot_doc.to_dict() or {}).get("summary") or {}
            record = {
                "name": tag_name,
                "snapshot-id": snapshot_id,
                "created-by": author,
                "created-at-ms": int(time.time() * 1000),
                "comment": comment,
            }
            transaction.set(tag_ref, record)
            # The LOGICAL size, because that is what the pin will be CHARGED.
            # Storage billing meters `uncompressed_size_in_bytes` deliberately -
            # a customer is billed for the data they handed over, whatever we
            # manage to compress it to, and the spread is margin (see the
            # "Logical bytes, deliberately" note in xb500.opteryx
            # app/operations/record_storage_billing.py). Reporting the on-disk
            # size here would quote a number ~96% below the invoice, which is
            # the one way this figure could mislead the person taking on the
            # cost. The physical size is carried alongside, never as the answer.
            record["pinned-bytes"] = _as_int(summary.get("total-data-size")) or 0
            record["pinned-bytes-on-disk"] = _as_int(summary.get("total-files-size")) or 0
            return record

        record = _create(self.firestore_client.transaction())

        emit_audit(
            "create_tag",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            tag=tag_name,
            snapshot_id=snapshot_id,
            pinned_bytes=record["pinned-bytes"],
            pinned_bytes_on_disk=record["pinned-bytes-on-disk"],
        )
        return record

    def drop_tag(
        self,
        dataset_identifier: str,
        name: str,
        author: str | None = None,
        missing_ok: bool = False,
    ) -> None:
        """Remove a tag, releasing the snapshot it pinned.

        The snapshot returns to the ordinary retention rules immediately, and
        if it is already past the window it expires on the next run. That is
        the intended consequence, not a side effect: dropping a tag IS how you
        agree to lose the data it was holding.
        """
        if not author:
            raise ValueError("author must be provided when dropping a tag")

        tag_name = self.normalize_tag_name(name)
        collection, dataset_name = self._local_parts(dataset_identifier)
        tag_ref = self._tags_collection(collection, dataset_name).document(tag_name)

        existing = tag_ref.get()
        if not existing.exists:
            if missing_ok:
                return
            raise TagNotFound(f"Tag not found: {tag_name} on {collection}.{dataset_name}")
        snapshot_id = (existing.to_dict() or {}).get("snapshot-id")
        tag_ref.delete()

        emit_audit(
            "drop_tag",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            tag=tag_name,
            snapshot_id=snapshot_id,
        )

    def list_tags(self, dataset_identifier: str) -> list[dict]:
        """Every STORED tag on a dataset, as plain dicts, ordered by name.

        One subcollection read. Expiration reads these rows to know which
        snapshots it may not retire, which makes this a protected input: every
        row here pins its snapshot.

        Stored tags only. The virtual `current` and `previous` pin nothing and
        are not returned - see `virtual_tags`. A display listing (what
        `SHOW SNAPSHOTS` groups by `snapshot-id`) wants both, and gets them from
        `list_tags_for_display`.
        """
        collection, dataset_name = self._local_parts(dataset_identifier)
        tags = []
        for doc in self._tags_collection(collection, dataset_name).stream():
            data = doc.to_dict() or {}
            data.setdefault("name", doc.id)
            tags.append(data)
        return sorted(tags, key=lambda tag: tag["name"])

    def virtual_tags(
        self, dataset_identifier: str, dataset: SimpleDataset | None = None
    ) -> list[dict]:
        """`current` and `previous` as tag-shaped rows, for display beside real tags.

        These are computed on every call, never stored. That is the whole point:
        a stored tag is immutable and pins its snapshot from expiry, and both of
        these words must instead MOVE as the dataset moves. Writing them down
        would freeze them at the moment of writing and pin two snapshots nobody
        asked to keep.

        `previous` is the previous VERSION OF THE DATA, not the previous
        snapshot. Compaction and statistics refresh write a new snapshot without
        changing a single row, so the snapshot immediately behind the head is
        very often the same data the unqualified read already returns - the
        worst possible answer, because it looks like a successful time-travel
        read. `SimpleDataset.previous_user_snapshot` does the skipping, and the
        reasoning lives there.

        A row is emitted only when the word resolves to a snapshot that exists.
        `previous` is absent for a dataset with one version, and absent when the
        version behind the current one has expired: there is no row in the
        listing to attach it to either way, and inventing one would name a
        snapshot the caller cannot read.

        Deliberately NOT part of `list_tags`. That method is a protected input to
        expiration - it decides what may not be deleted - and a virtual tag is
        not a pin. Keeping them in separate methods means the retention path
        cannot pick one up by accident.
        """
        if dataset is None:
            dataset = self.load_dataset(dataset_identifier)

        rows: list[dict] = []

        head = dataset.metadata.current_snapshot()
        if head is not None and head.snapshot_id is not None:
            rows.append(
                _virtual_tag_row(
                    "current",
                    head.snapshot_id,
                    "The snapshot every unqualified read of this dataset sees.",
                )
            )

        try:
            previous = dataset.previous_user_snapshot()
        except (SnapshotMissingError, SnapshotAncestryTooDeep) as exc:
            # Neither is fatal to a listing. The first means the previous
            # version has expired, the second that the walk gave up; in both
            # cases there is no snapshot in this listing to label, so the row
            # is omitted rather than the whole listing failing.
            logger.debug(
                "No `previous` virtual tag for %s (%s)", dataset_identifier, exc
            )
            previous = None

        if previous is not None and previous.snapshot_id is not None:
            rows.append(
                _virtual_tag_row(
                    "previous",
                    previous.snapshot_id,
                    "The version of the data before the current one. Compaction and "
                    "statistics refresh are skipped - they rewrite files without "
                    "changing rows.",
                )
            )

        return rows

    def list_tags_for_display(
        self, dataset_identifier: str, dataset: SimpleDataset | None = None
    ) -> list[dict]:
        """Every tag on a dataset as a viewer should see it: virtual, then real.

        What `SHOW SNAPSHOTS` groups by `snapshot-id`. Virtual tags lead because
        they are the two rows a reader is looking for - where the dataset is now
        and what it was before - and real tags follow by name, as `list_tags`
        orders them.

        Nothing here may be passed to expiration: `virtual` rows are not pins.
        Use `list_tags` for that.
        """
        return self.virtual_tags(dataset_identifier, dataset=dataset) + self.list_tags(
            dataset_identifier
        )

    def resolve_tag(self, dataset_identifier: str, name: str) -> int:
        """The snapshot id a tag names.

        One document get by id - the read path pays this only when a statement
        actually names a tag.

        A tag that resolves to nothing is not a normal outcome to degrade
        through: pinning means the snapshot cannot have expired underneath it,
        so an unresolvable tag is a broken pin and says so.
        """
        tag_name = self.normalize_tag_name(name)
        collection, dataset_name = self._local_parts(dataset_identifier)
        doc = self._tags_collection(collection, dataset_name).document(tag_name).get()
        if not doc.exists:
            raise TagNotFound(f"Tag not found: {tag_name} on {collection}.{dataset_name}")
        snapshot_id = (doc.to_dict() or {}).get("snapshot-id")
        if snapshot_id is None:
            raise SnapshotMissingError(
                f"Tag {tag_name} on {collection}.{dataset_name} names no snapshot."
            )
        return int(snapshot_id)

    # ------------------------------------------------------------------
    # Rollback
    # ------------------------------------------------------------------

    def rollback_dataset(
        self,
        dataset_identifier: str,
        snapshot_id: int,
        author: str | None = None,
    ) -> dict:
        """Move the head to an existing snapshot - what a rollback is.

        Every unqualified read of a dataset sees the snapshot the head points
        at, so pointing it at an older one restores that version of the data for
        everybody, at once, without moving a single byte.

        Nothing is deleted and nothing is rewritten. The snapshots this moves
        OFF stay live: they keep their documents, their manifests and their data
        files, they still appear in `SHOW SNAPSHOTS`, and they can still be read
        by id - so a rollback is itself reversible, by rolling forward to the id
        it moved off. That id is in the returned record, because it is the only
        thing the caller needs to undo this and nothing else records it.

        What it does NOT do is protect those snapshots. Ordinary retention still
        applies to them, and once they age out they expire like any other
        snapshot - after which the rollback can no longer be undone. A rollback
        somebody may want to reverse later should be preceded by a tag, which is
        the mechanism that pins a snapshot indefinitely.

        The head moves with the schema pointer, not on its own: a dataset whose
        data is at yesterday's snapshot but which still advertises today's
        schema describes columns its files do not have, and the next append
        would be built against that schema.

        The check and the move are ONE transaction, conditional on the head
        still being where it was read. A rollback that raced a commit would
        otherwise discard that commit silently, which is exactly the failure
        `SnapshotRaceError` exists to prevent on the write path.
        """
        if not author:
            raise ValueError("author must be provided when rolling back a dataset")

        collection, dataset_name = self._local_parts(dataset_identifier)
        doc_ref = self._dataset_doc_ref(collection, dataset_name)
        snapshot_ref = self._snapshots_collection(collection, dataset_name).document(
            str(int(snapshot_id))
        )
        qualified = f"{collection}.{dataset_name}"
        snapshot_id = int(snapshot_id)

        @firestore.transactional
        def _rollback(transaction) -> dict:
            # Reads before writes - Firestore refuses the other order.
            doc = doc_ref.get(transaction=transaction)
            snapshot_doc = snapshot_ref.get(transaction=transaction)

            if not doc.exists:
                raise DatasetNotFound(f"Dataset not found: {qualified}")
            data = doc.to_dict() or {}

            # The same per-asset guard `drop_dataset` honours: a locked dataset
            # is one two people agreed not to change, and replacing every row in
            # it is a change.
            if data.get("locked-by") is not None:
                raise DatasetLocked(f"Dataset is locked: {qualified}")

            if not snapshot_doc.exists:
                raise SnapshotMissingError(
                    f"No snapshot {snapshot_id} for {qualified} - it may not exist, "
                    "or may have expired."
                )
            snapshot_data = snapshot_doc.to_dict() or {}
            if snapshot_is_tombstoned(snapshot_data):
                raise SnapshotMissingError(
                    f"Snapshot {snapshot_id} of {qualified} has expired; there is no "
                    "longer a guarantee its data files exist, so it cannot be made "
                    "the current snapshot."
                )
            if not snapshot_data.get("manifest"):
                # A snapshot with no manifest cannot be scanned. Pointing the
                # head at one would present the dataset as empty rather than
                # rolled back.
                raise SnapshotMissingError(
                    f"Snapshot {snapshot_id} of {qualified} has no manifest and cannot "
                    "be read; it cannot be made the current snapshot."
                )

            previous_head = data.get("current-snapshot-id")
            if previous_head == snapshot_id:
                # Already there. Not an error - a rollback that has to be
                # retried should be safe to retry - but reported honestly so
                # the caller does not tell someone data moved when it did not.
                return {
                    "dataset": qualified,
                    "snapshot-id": snapshot_id,
                    "previous-snapshot-id": previous_head,
                    "moved": False,
                }

            update = {
                # The head pointer - see DatasetMetadata.current_snapshot_id.
                "current-snapshot-id": snapshot_id,
            }
            schema_id = snapshot_data.get("schema-id")
            if schema_id is not None:
                update["current-schema-id"] = schema_id
            transaction.update(doc_ref, update)

            return {
                "dataset": qualified,
                "snapshot-id": snapshot_id,
                "previous-snapshot-id": previous_head,
                "moved": True,
            }

        record = _rollback(self.firestore_client.transaction())

        # The id-keyed snapshot cache is safe across this (snapshot documents
        # are write-once), but a dataset object already held by this process has
        # the OLD pointer in memory, and nothing here can reach it.
        emit_audit(
            "rollback_dataset",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            snapshot_id=snapshot_id,
            previous_snapshot_id=record["previous-snapshot-id"],
            moved=record["moved"],
        )
        return record

    # ------------------------------------------------------------------
    # Materialized views
    # ------------------------------------------------------------------

    @staticmethod
    def _mv_trigger_name(qualified_target: str) -> str:
        """The auto-generated name of an MV's refresh trigger on a source.

        The readable part is the target's collection and dataset - the name
        appears in `information_schema.triggers` and in `DROP TRIGGER <name> ON
        <table>`, which people type. The digest is what makes it unique: the
        readable part alone collides ('mart' + 'a__b' against 'mart__a' + 'b',
        and dataset names may contain dots), and a collision would silently
        hand one view's trigger document to another.
        """
        _, collection, dataset_name = OpteryxCatalog._split_qualified(qualified_target)
        digest = hashlib.sha256(qualified_target.encode("utf-8")).hexdigest()[:8]
        return f"refresh__{collection}__{dataset_name}__{digest}"

    def _assert_no_materialized_view_cycle(self, identifier: str, source_tables: list[str]) -> None:
        """Reject a source graph that reaches back to the MV being registered.

        A view may read another view, so the walk is the only thing standing
        between a chain and a loop: it follows `source-tables` transitively
        through every source that is itself a materialized view, and refuses
        the registration if it arrives back at `identifier`.

        This is the early refusal, before the registration writes anything.
        The enforcing check is `_assert_no_trigger_cycle`, on the graph that
        actually fires; the two agree for any view registered through here,
        because this method reconciles one trigger per source. Keeping this one
        means a cyclic registration is rejected with nothing half-written,
        rather than partway through creating its triggers.
        """
        stack = list(source_tables)
        seen = set()
        while stack:
            current = stack.pop()
            if current == identifier:
                raise MaterializedViewError(
                    f"materialized view source cycle: {identifier} would (transitively) "
                    "depend on itself"
                )
            if current in seen:
                continue
            seen.add(current)
            if self._source_workspace(current) != self.workspace:
                # Another workspace's dataset - unreadable from this handle, so
                # the walk cannot continue through it. It also cannot close a
                # cycle back to this view without a foreign MV reading us, which
                # the same check on that side would refuse.
                continue
            coll, name = self._local_parts(current)
            doc = self._dataset_doc_ref(coll, name).get()
            if not doc.exists:
                continue
            data = doc.to_dict() or {}
            if data.get("dataset-type") == MATERIALIZED_VIEW_TYPE:
                stack.extend(self._qualify(s) for s in data.get("source-tables") or [])

    def create_materialized_view(
        self,
        identifier: str,
        sql: str,
        source_tables: list[str],
        author: str | None = None,
        update_if_exists: bool = False,
    ) -> None:
        """Register an existing dataset as a materialized view.

        The backing table is created by the engine's CTAS write path before
        this is called - this registers what makes it an MV: the defining SQL
        (versioned in the dataset's `statement` subcollection, exactly as views
        version theirs), the source list, and one refresh trigger on each
        source dataset. Re-registration (`update_if_exists`, the CoRTAS path)
        writes a new statement version and reconciles triggers against the new
        source list.

        A source may itself be a materialized view: chains are allowed, and the
        chain refreshes a hop at a time as each view's refresh commit fires the
        triggers below it. What is not allowed is a loop - see
        `_assert_no_materialized_view_cycle`. A source in a workspace that
        restricts egress is refused with `EgressRestricted`, at refresh time as
        well as here.

        The identity a refresh runs as (`runs-as`) is pinned at creation and is
        NOT changed by re-registration - editing a view does not transfer it,
        only `set_materialized_view_owner` does. The confused-deputy risk that
        would otherwise create is closed on the caller side: the engine re-runs
        the full creation authorization against whoever is editing, so an editor
        can never repoint a view at sources they could not read themselves.

        Args:
            identifier: the (existing) backing table, 'collection.dataset' or
                fully qualified. Stored and returned fully qualified.
            sql: the defining SELECT, as executable text.
            source_tables: every catalog table the SELECT reads - triggers land
                on each. Accepted in either form, stored fully qualified.
            author: the identity registering the MV. Becomes `runs-as` on a
                first registration; on a re-registration it is recorded as the
                statement's author and the existing `runs-as` is left alone.
            update_if_exists: allow re-registration of an existing MV.
        """
        if author is None:
            raise ValueError("author must be provided when creating a materialized view")
        # The author becomes `runs-as` below on first registration, so the
        # ownership rule is applied at the point the identity is pinned rather
        # than only when one is transferred.
        _assert_can_own(author, "a materialized view")

        identifier = self._qualify(identifier)
        collection, dataset_name = self._local_parts(identifier)
        doc_ref = self._dataset_doc_ref(collection, dataset_name)
        doc = doc_ref.get()
        if not doc.exists:
            raise DatasetNotFound(
                f"Materialized view backing table not found: {identifier} "
                "(the CTAS creates it before registration)"
            )
        data = doc.to_dict() or {}
        already_mv = data.get("dataset-type") == MATERIALIZED_VIEW_TYPE
        if already_mv and not update_if_exists:
            raise MaterializedViewError(f"Materialized view already exists: {identifier}")

        # Normalize, dedupe (order-preserving), and validate sources.
        relative_sources: list[str] = []
        for table in source_tables:
            relative = self._qualify(table)
            if relative == identifier:
                raise MaterializedViewError(f"materialized view cannot read itself: {identifier}")
            if relative not in relative_sources:
                relative_sources.append(relative)
        if not relative_sources:
            raise MaterializedViewError(
                "a materialized view needs at least one catalog-resident source table "
                "- nothing could ever fire its refresh"
            )
        # Egress before existence: a source in an egress-locked workspace must
        # be told it was refused, not that it was not found. (Nothing outside
        # this workspace resolves as a source today, so in practice the
        # not-found arm is the one a cross-workspace source reaches - see
        # `enforce_materialized_view_egress`.)
        self.enforce_materialized_view_egress(identifier, relative_sources, "create")
        for relative in relative_sources:
            src_coll, src_name = self._local_parts(relative)
            if not self._dataset_doc_ref(src_coll, src_name).get().exists:
                raise DatasetNotFound(f"Source dataset not found: {relative}")
        self._assert_no_materialized_view_cycle(identifier, relative_sources)

        # Statement version, following the view convention exactly.
        now_ms = int(time.time() * 1000)
        current_statement_id = data.get("statement-id")
        sequence_number = 1
        if current_statement_id:
            stmt_doc = doc_ref.collection("statement").document(str(current_statement_id)).get()
            if stmt_doc.exists:
                sequence_number = (stmt_doc.to_dict() or {}).get("sequence-number", 0) + 1
        # The statement id is the millisecond it was written, so two
        # redefinitions inside one millisecond would collide on the document id
        # and the earlier statement would be overwritten - losing a version the
        # sequence number still claims is there. Step forward to the first free
        # id instead.
        statements = doc_ref.collection("statement")
        statement_id = str(now_ms)
        while statements.document(statement_id).get().exists:
            now_ms += 1
            statement_id = str(now_ms)
        statements.document(statement_id).set(
            {
                "sql": sql,
                "timestamp-ms": now_ms,
                "author": author,
                "sequence-number": sequence_number,
            }
        )

        doc_ref.update(
            {
                "dataset-type": MATERIALIZED_VIEW_TYPE,
                "statement-id": statement_id,
                "source-tables": relative_sources,
                # Pinned to the AUTHOR: the identity that ran the statement is
                # necessarily one that held every grant the statement needed, so
                # it is the only owner derivable here that is known to work.
                # An existing value survives re-registration, so editing a view
                # never silently transfers whose authority it refreshes with;
                # `set_materialized_view_owner` is the only way to move it.
                "runs-as": data.get("runs-as") or author,
                "last-refreshed-at-ms": data.get("last-refreshed-at-ms"),
                "last-refresh-status": data.get("last-refresh-status"),
                "last-refresh-execution-id": data.get("last-refresh-execution-id"),
            }
        )

        # Reconcile triggers: one per current source, none on former sources.
        trigger_name = self._mv_trigger_name(identifier)
        previous_sources = [self._qualify(s) for s in data.get("source-tables") or []]
        for removed in (s for s in previous_sources if s not in relative_sources):
            self.drop_trigger(removed, trigger_name, author=author, missing_ok=True)
        for source in relative_sources:
            self.create_trigger(
                source,
                trigger_name,
                target_view=identifier,
                statement_id=statement_id,
                author=author,
            )

        emit_audit(
            "update_materialized_view" if already_mv else "create_materialized_view",
            resource_type=ResourceType.MATERIALIZED_VIEW,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            statement_id=statement_id,
            source_tables=relative_sources,
        )

    def get_materialized_view(self, identifier: str) -> dict:
        """The MV's registration record: defining SQL, sources, refresh state.

        `identifier` is the qualified name, so callers stop rebuilding it from
        the decomposed parts. `runs-as` is the pinned identity a refresh
        executes as; `last-updated-by`/`last-updated-at-ms` are the author and
        time of the current statement version - who last changed the definition,
        which is a different question and a different person.
        """
        identifier = self._qualify(identifier)
        collection, dataset_name = self._local_parts(identifier)
        doc_ref = self._dataset_doc_ref(collection, dataset_name)
        doc = doc_ref.get()
        if not doc.exists:
            raise DatasetNotFound(f"Dataset not found: {identifier}")
        data = doc.to_dict() or {}
        if data.get("dataset-type") != MATERIALIZED_VIEW_TYPE:
            raise MaterializedViewError(f"Not a materialized view: {identifier}")

        statement_id = data.get("statement-id")
        sql = None
        last_updated_by = None
        last_updated_at_ms = None
        if statement_id:
            # One read, three fields - the author and timestamp were already
            # being fetched alongside the SQL and thrown away.
            statement = (
                doc_ref.collection("statement").document(str(statement_id)).get().to_dict() or {}
            )
            sql = statement.get("sql")
            last_updated_by = statement.get("author")
            last_updated_at_ms = statement.get("timestamp-ms")

        return {
            "identifier": identifier,
            "name": dataset_name,
            "collection": collection,
            "workspace": self.workspace,
            "sql": sql,
            "statement-id": statement_id,
            "source-tables": data.get("source-tables") or [],
            "runs-as": data.get("runs-as"),
            "suspended-at-ms": data.get("suspended-at-ms"),
            "suspended-by": data.get("suspended-by"),
            "last-updated-by": last_updated_by,
            "last-updated-at-ms": last_updated_at_ms,
            "last-refreshed-at-ms": data.get("last-refreshed-at-ms"),
            "last-refresh-status": data.get("last-refresh-status"),
            "last-refresh-execution-id": data.get("last-refresh-execution-id"),
            "last-read-at-ms": data.get("last-read-at-ms"),
            "last-refresh-skipped-at-ms": data.get("last-refresh-skipped-at-ms"),
        }

    def mark_materialized_view_read(self, identifier: str) -> None:
        """Stamp that somebody read this view - the signal read-gated refresh runs on.

        Written by jobs.opteryx when a submitted statement reads the view. The
        caller debounces: only the FIRST read after a refresh changes the
        gate's answer ("was this view read since it was last refreshed?"), so
        this is written at most once per view per refresh cycle - not per query.

        A targeted `update()`, never `set()`: the MV record carries fields this
        stamp must not destroy.
        """
        collection, dataset_name = self._local_parts(identifier)
        self._dataset_doc_ref(collection, dataset_name).update(
            {"last-read-at-ms": int(time.time() * 1000)}
        )

    def mark_materialized_view_refresh_skipped(self, identifier: str) -> None:
        """Stamp that a refresh was declined because nobody had read the view.

        Written by jobs.opteryx when the read gate skips a refresh. The stamp is
        what lets the NEXT reader trigger a catch-up: a view whose skip stamp is
        newer than its refresh stamp is stale-by-choice, and the read path turns
        that into a refresh submission. Without it, a skipped view being read
        again would stay stale until its sources happened to commit.

        Deliberately not a status on `last-refresh-status`: nothing was
        attempted, and overwriting the last real refresh outcome with "skipped"
        would erase the answer to "did the last refresh WORK".
        """
        collection, dataset_name = self._local_parts(identifier)
        self._dataset_doc_ref(collection, dataset_name).update(
            {"last-refresh-skipped-at-ms": int(time.time() * 1000)}
        )

    def set_materialized_view_suspended(
        self, identifier: str, suspended: bool, author: str | None = None
    ) -> None:
        """Suspend or resume a materialized view's automatic refresh.

        Suspending leaves the triggers in place and the view queryable; it simply
        stops each firing from becoming a refresh. That is the difference from
        dropping the triggers, which was previously the only way to stop a view
        refreshing: a dropped trigger is indistinguishable from one that was
        never created or that something broke, so "deliberately off" and "quietly
        broken" looked identical. A suspended view says which it is, since when,
        and by whom.

        The state lives on the VIEW, not on its triggers. A view with four
        sources has four triggers, and pausing three of them would not pause the
        view - it would refresh from a subset of its sources and produce
        silently partial data. One flag cannot be partially applied.

        `last-refresh-status` is deliberately left alone: it records the last
        real refresh outcome, which is still the truth. Suspension is a
        separate fact and readers should show both.
        """
        if not author:
            raise ValueError("author must be provided when suspending a materialized view")

        identifier = self._qualify(identifier)
        collection, dataset_name = self._local_parts(identifier)
        doc_ref = self._dataset_doc_ref(collection, dataset_name)
        doc = doc_ref.get()
        if not doc.exists:
            raise DatasetNotFound(f"Dataset not found: {identifier}")
        data = doc.to_dict() or {}
        if data.get("dataset-type") != MATERIALIZED_VIEW_TYPE:
            raise MaterializedViewError(f"Not a materialized view: {identifier}")

        doc_ref.update(
            {
                "suspended-at-ms": int(time.time() * 1000) if suspended else None,
                "suspended-by": author if suspended else None,
            }
        )

        emit_audit(
            "suspend_materialized_view" if suspended else "resume_materialized_view",
            resource_type=ResourceType.MATERIALIZED_VIEW,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
        )

    def set_materialized_view_owner(
        self, identifier: str, new_owner: str, author: str | None = None
    ) -> None:
        """Repoint a materialized view's `runs-as` identity.

        The only thing that moves a pinned owner. Deliberately narrow: it writes
        no statement version and reconciles no triggers, because the definition
        has not changed - only whose authority refreshes it.

        The caller-side permission model gates this on WORKSPACE owner rather
        than on the view. At creation `runs-as` is necessarily an identity that
        held every grant the statement needed, because it was the identity that
        ran it; this method breaks that invariant by letting a view be pointed
        at someone else's authority, and nothing here can check another
        principal's grants. A workspace owner can already grant themselves
        anything, so requiring that tier escalates nothing.
        """
        if not author:
            raise ValueError("author must be provided when changing a materialized view owner")
        if not new_owner:
            raise ValueError("new_owner must be provided")
        # Refused here as well as in the binder, for the reason set out on
        # `set_trigger_owner`: a refresh runs as this identity, unattended and
        # on a schedule, so a platform identity would be standing compute billed
        # to nobody.
        _assert_can_own(new_owner, f"materialized view {identifier}")

        identifier = self._qualify(identifier)
        collection, dataset_name = self._local_parts(identifier)
        doc_ref = self._dataset_doc_ref(collection, dataset_name)
        doc = doc_ref.get()
        if not doc.exists:
            raise DatasetNotFound(f"Dataset not found: {identifier}")
        data = doc.to_dict() or {}
        if data.get("dataset-type") != MATERIALIZED_VIEW_TYPE:
            raise MaterializedViewError(f"Not a materialized view: {identifier}")

        previous_owner = data.get("runs-as")
        doc_ref.update({"runs-as": new_owner})

        emit_audit(
            "alter_materialized_view_owner",
            resource_type=ResourceType.MATERIALIZED_VIEW,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            previous_owner=previous_owner,
            new_owner=new_owner,
        )

    def list_materialized_views(self, collection: str) -> list[str]:
        """Names of the materialized views in a collection.

        Client-side filter over the datasets subcollection rather than a
        Firestore `where`: no composite index, and dataset listings are
        already full-collection streams everywhere else in this catalog.
        """
        results = []
        for doc in self._datasets_collection(collection).stream():
            if (doc.to_dict() or {}).get("dataset-type") == MATERIALIZED_VIEW_TYPE:
                results.append(doc.id)
        return results

    def drop_materialized_view(self, identifier: str, author: str | None = None) -> None:
        """Drop a materialized view: its triggers, then its backing dataset.

        Trigger removal happens first, while the MV document's source list is
        still readable; the dataset drop then handles tombstoning and
        subcollection cleanup exactly as any other dataset drop.

        An author is required. Checked here rather than left to the
        `drop_dataset` call at the end, which would otherwise raise only after
        the triggers had already been removed.
        """
        if not author:
            raise ValueError("author must be provided when dropping a materialized view")
        identifier = self._qualify(identifier)
        collection, dataset_name = self._local_parts(identifier)
        doc = self._dataset_doc_ref(collection, dataset_name).get()
        if not doc.exists:
            return
        data = doc.to_dict() or {}
        if data.get("dataset-type") != MATERIALIZED_VIEW_TYPE:
            raise MaterializedViewError(f"Not a materialized view: {identifier} (use drop_dataset)")

        trigger_name = self._mv_trigger_name(identifier)
        for source in data.get("source-tables") or []:
            self.drop_trigger(source, trigger_name, author=author, missing_ok=True)

        emit_audit(
            "drop_materialized_view",
            resource_type=ResourceType.MATERIALIZED_VIEW,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
        )

        # `drop_dataset` is part of the general dataset API and takes the
        # relative form; only the materialized-view and trigger records are
        # stored qualified.
        self.drop_dataset(f"{collection}.{dataset_name}", author=author)

    def mark_materialized_view_refreshed(
        self,
        identifier: str,
        status: str,
        execution_id: str | None = None,
        author: str | None = None,
    ) -> None:
        """Stamp refresh state on an MV.

        Called by the engine when a refresh commits, and by the worker when a
        trigger-fired refresh fails or is denied - a denial is a status, not
        silence. The engine cannot record its own failures (a refresh that
        raised never reaches the stamp), which is why both callers exist.
        """
        collection, dataset_name = self._local_parts(identifier)
        self._dataset_doc_ref(collection, dataset_name).update(
            {
                "last-refreshed-at-ms": int(time.time() * 1000),
                "last-refresh-status": status,
                "last-refresh-execution-id": execution_id,
            }
        )

        emit_audit(
            "refresh_materialized_view",
            resource_type=ResourceType.MATERIALIZED_VIEW,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            status=status,
            execution_id=execution_id,
        )

    def update_view_execution_metadata(
        self,
        identifier: str | tuple,
        row_count: int | None = None,
        execution_time: float | None = None,
    ) -> None:
        if isinstance(identifier, (tuple, list)):
            collection, view_name = identifier[0], identifier[1]
        else:
            collection, view_name = identifier.split(".")

        doc_ref = self._view_doc_ref(collection, view_name)
        updates = {}
        now_ms = int(time.time() * 1000)
        if row_count is not None:
            updates["last-execution-records"] = row_count
        if execution_time is not None:
            updates["last-execution-time-ms"] = int(execution_time * 1000)
        updates["last-execution-ms"] = now_ms
        if updates:
            doc_ref.update(updates)

    def update_view_description(
        self,
        identifier: str | tuple,
        description: str,
        describer: str | None = None,
    ) -> None:
        """Update the description for a view.

        Args:
            identifier: View identifier ('collection.view' or tuple)
            description: The new description text
            describer: Optional identifier for who/what created the description
        """
        if isinstance(identifier, (tuple, list)):
            collection, view_name = identifier[0], identifier[1]
        else:
            collection, view_name = identifier.split(".")

        doc_ref = self._view_doc_ref(collection, view_name)
        updates = {
            "description": description,
        }
        if describer is not None:
            updates["describer"] = describer
        doc_ref.update(updates)

    def update_dataset_description(
        self,
        identifier: str | tuple,
        description: str,
        describer: str | None = None,
    ) -> None:
        """Update the description for a dataset.

        Args:
            identifier: Dataset identifier in format 'collection.dataset_name'
            description: The new description text
            describer: Optional identifier for who/what created the description
        """

        if isinstance(identifier, (tuple, list)):
            collection, dataset_name = identifier[0], identifier[1]
        else:
            collection, dataset_name = identifier.split(".")

        doc_ref = self._dataset_doc_ref(collection, dataset_name)
        updates = {
            "description": description,
        }
        if describer is not None:
            updates["describer"] = describer
        doc_ref.update(updates)

    def update_dataset_sort_order(
        self,
        identifier: str | tuple,
        columns: list[str],
        author: str | None = None,
    ) -> None:
        """Set the clustering columns for a dataset (``ALTER TABLE ... CLUSTER BY``).

        Persists a single Iceberg-style sort-order entry naming ``columns`` in
        the given order, ascending. Replaces any previously configured sort
        order outright - CLUSTER BY re-declares the physical layout, it does
        not append to it. See ``catalog.compaction.normalize_sort_order`` for
        how this shape is consumed (only the first field is currently used as
        the primary sort key for compaction).

        Args:
            identifier: Dataset identifier in format 'collection.dataset_name'
            columns: Column names to cluster by, in priority order. Must be
                non-empty and must all exist in the dataset's current schema.
            author: The identity making the change - None when unauthenticated,
                never substituted (see audit.emit_audit).

        Raises:
            DatasetNotFound: If the dataset does not exist.
            ValueError: If ``columns`` is empty or names a column that is not
                in the dataset's current schema.
        """
        if not columns:
            raise ValueError("columns must be a non-empty list of column names")

        if isinstance(identifier, (tuple, list)):
            collection, dataset_name = identifier[0], identifier[1]
        else:
            collection, dataset_name = identifier.split(".")

        doc_ref = self._dataset_doc_ref(collection, dataset_name)
        doc = doc_ref.get()
        if not doc.exists:
            raise DatasetNotFound(f"Dataset not found: {collection}.{dataset_name}")

        data = doc.to_dict() or {}
        current_schema_id = data.get("current-schema-id")
        known_columns = set()
        if current_schema_id:
            schema_doc = doc_ref.collection("schemas").document(str(current_schema_id)).get()
            if schema_doc.exists:
                known_columns = {
                    c.get("name") for c in (schema_doc.to_dict() or {}).get("columns", [])
                }
        unknown = [c for c in columns if c not in known_columns]
        if unknown:
            raise ValueError(
                f"Unknown column(s) for CLUSTER BY on {collection}.{dataset_name}: {unknown}"
            )

        doc_ref.update(
            {
                "sort-orders": [
                    {
                        "order-id": 1,
                        "fields": [{"name": c, "direction": "asc"} for c in columns],
                    }
                ],
            }
        )

        emit_audit(
            "update_sort_order",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            columns=columns,
        )

    def alter_dataset_schema(
        self,
        identifier: str | tuple,
        add: list[dict] | None = None,
        drop: list[str] | None = None,
        rename: dict | None = None,
        retype: dict | None = None,
        author: str | None = None,
    ) -> str:
        """Evolve a dataset's schema (``ALTER TABLE ... ADD/DROP/RENAME/ALTER COLUMN``).

        Writes a NEW schema document and points ``current-schema-id`` at it;
        earlier schemas stay where they are, so a snapshot taken before this
        keeps resolving the shape it was written under.

        **Field ids are the identity, not names.** A surviving column keeps the
        id it already had - including through a rename, where the name is the
        only thing that changes - and an added column takes a fresh id from
        ``next-field-id``. Manifest statistics are keyed by field id
        (``_field_id_by_name``), so preserving ids is what keeps a column's
        min/max attached to that column rather than to whatever now sits in its
        old position.

        This does NOT touch data files. The caller rewrites them (see
        ``SimpleDataset.alter_columns``, which sequences the two) - this is the
        catalog half alone.

        Args:
            identifier: Dataset identifier in format 'collection.dataset_name'
            add: New columns, appended in order. Each is a stored-column dict
                without an ``id``: ``{"name", "type", "element-type",
                "precision", "scale"}``.
            drop: Column names to remove.
            rename: ``{old_name: new_name}``; the column keeps its field id.
            retype: ``{name: {"type", "element-type", "precision", "scale"}}``;
                the column keeps its field id.
            author: The identity making the change - None when unauthenticated,
                never substituted (see audit.emit_audit).

        Returns:
            The new schema id.

        Raises:
            DatasetNotFound: If the dataset does not exist.
            ValueError: If no operation was given, a named column is absent, a
                name would collide, or every column would be dropped.
        """
        add = list(add or [])
        drop = list(drop or [])
        rename = dict(rename or {})
        retype = dict(retype or {})
        if not (add or drop or rename or retype):
            raise ValueError("alter_dataset_schema was given no changes to make")

        if isinstance(identifier, (tuple, list)):
            collection, dataset_name = identifier[0], identifier[1]
        else:
            collection, dataset_name = identifier.split(".")

        doc_ref = self._dataset_doc_ref(collection, dataset_name)
        doc = doc_ref.get()
        if not doc.exists:
            raise DatasetNotFound(f"Dataset not found: {collection}.{dataset_name}")

        data = doc.to_dict() or {}
        current_schema_id = data.get("current-schema-id")
        if not current_schema_id:
            raise ValueError(f"{collection}.{dataset_name} has no current schema to alter")
        schema_doc = doc_ref.collection("schemas").document(str(current_schema_id)).get()
        if not schema_doc.exists:
            raise ValueError(
                f"{collection}.{dataset_name} points at schema {current_schema_id}, "
                "which does not exist"
            )
        columns = list((schema_doc.to_dict() or {}).get("columns", []))
        by_name = {c.get("name") for c in columns}

        # Every name is checked against the CURRENT schema before anything is
        # written. A partially-applied schema edit is not a state to reach: the
        # dataset would be pointing at a schema nobody asked for.
        for name in list(drop) + list(rename) + list(retype):
            if name not in by_name:
                raise ValueError(f"{collection}.{dataset_name} has no column named '{name}'")

        surviving = {c["name"] for c in columns if c.get("name") not in drop}
        for old, new in rename.items():
            surviving.discard(old)
            if new in surviving:
                raise ValueError(
                    f"renaming '{old}' to '{new}' would give "
                    f"{collection}.{dataset_name} two columns called '{new}'"
                )
            surviving.add(new)
        for column in add:
            name = column.get("name")
            if name in surviving:
                raise ValueError(
                    f"{collection}.{dataset_name} already has a column called '{name}'"
                )
            surviving.add(name)
        if not surviving:
            raise ValueError(
                f"dropping every column of {collection}.{dataset_name} would leave no relation"
            )

        next_field_id = data.get("next-field-id") or (
            max((c.get("id") or 0) for c in columns) + 1 if columns else 1
        )

        new_columns = []
        for column in columns:
            name = column.get("name")
            if name in drop:
                continue
            # Copied, not rebuilt: anything this method does not understand
            # (expectation-policies, annotations, fields added later) rides
            # through untouched rather than being silently dropped.
            evolved = dict(column)
            if name in retype:
                evolved.update(_expand_column_type(retype[name]))
            if name in rename:
                evolved["name"] = rename[name]
            new_columns.append(evolved)

        for column in add:
            evolved = {
                "type": None,
                "element-type": None,
                "scale": None,
                "precision": None,
                "expectation-policies": [],
                "annotations": [],
                **_expand_column_type(column),
                "id": next_field_id,
            }
            next_field_id += 1
            new_columns.append(evolved)

        sid = self._write_schema_columns(collection, dataset_name, new_columns, author)
        doc_ref.update({"current-schema-id": sid, "next-field-id": next_field_id})

        emit_audit(
            "alter_schema",
            resource_type=ResourceType.DATASET,
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            added=[c.get("name") for c in add],
            dropped=drop,
            renamed=rename,
            retyped=sorted(retype),
        )
        return sid

    def write_parquet_manifest(
        self, snapshot_id: int, entries: list[dict], dataset_location: str
    ) -> str | None:
        """Write a Parquet manifest for the given snapshot id and entries.

        Entries should be plain dicts. The manifest will be written to
        <dataset_location>/metadata/manifest-<snapshot_id>.parquet
        """
        from draken.interop.vector_sequence import vector_from_sequence
        from draken.morsels.morsel import Morsel
        from rugo.parquet import write_parquet

        from .iops.fileio import WRITE_PARQUET_OPTIONS

        # If entries is None we skip writing; if entries is empty list, write
        # an empty Parquet manifest (represents an empty dataset for this
        # snapshot). This preserves previous manifests so older snapshots
        # remain readable.
        if entries is None:
            return None

        # manifest-<snapshot_id>-<nonce>.parquet. The nonce is what makes the
        # name unique: snapshot ids are wall-clock milliseconds bumped past the
        # highest in the WRITER'S OWN in-memory history, so two writers holding
        # the same parent compute the same id and would otherwise write the same
        # path — one silently replacing the other's manifest before either
        # reaches the commit that detects the race. With a nonce the loser only
        # leaves an orphan, which the reclamation sweeps collect.
        parquet_path = (
            f"{dataset_location}/metadata/manifest-{snapshot_id}-{secrets.token_hex(6)}.parquet"
        )

        # Use provided FileIO if it supports writing; otherwise write to GCS.
        # Nothing below is recoverable - see the note on out.close() - so this
        # runs unguarded and lets failures reach the caller.
        #
        # Explicit dtype per column (especially the nested-list stats
        # columns) so rugo's writer gets a consistent shape regardless of
        # what individual entries happen to carry.
        columns = {
            "file_path": "VARCHAR",
            "file_format": "VARCHAR",
            "record_count": "INTEGER",
            "file_size_in_bytes": "INTEGER",
            "uncompressed_size_in_bytes": "INTEGER",
            "column_uncompressed_sizes_in_bytes": "ARRAY",
            "null_counts": "ARRAY",
            "min_k_hashes": "ARRAY",
            "histogram_counts": "ARRAY",
            "histogram_bins": "INTEGER",
            "min_values": "ARRAY",
            "max_values": "ARRAY",
            "min_lengths": "ARRAY",
            "max_lengths": "ARRAY",
            # Stable per-column field-id, same order/index as every other
            # per-column stats array above (min_values[i] is field_ids[i]'s
            # min, etc.) — lets readers key stats by a schema-stable id
            # instead of assuming today's array position equals a column's
            # position in some other schema snapshot. Empty for manifest
            # rows written before this existed; readers must fall back to
            # positional indexing in that case.
            "field_ids": "ARRAY",
            # Per-column byte-class histogram (8 fixed classes) and total
            # byte count, VARCHAR/NVARCHAR/VARBINARY columns only (empty
            # list / 0 elsewhere) — backs the LIKE '%needle%' selectivity
            # char-class estimator. See catalog/manifest.py's
            # _compute_column_stats / Vector.char_class_stats().
            "char_class_counts": "ARRAY",
            "char_total_bytes": "ARRAY",
            # ARRAY columns only: statistics over the flat CHILD vector, i.e.
            # the elements pooled across every row's list. An ARRAY has no
            # ordinal encoding of its own, so min_values/histogram_counts are
            # the sentinel/empty for it and it can be pruned on nothing; its
            # elements are an ordinary vector and take the ordinary kernels.
            # See catalog/manifest.py's _compute_column_stats. Empty for
            # manifest rows written before this existed, which readers must
            # treat as "not computed", not "no elements".
            "element_min_values": "ARRAY",
            "element_max_values": "ARRAY",
            "element_min_k_hashes": "ARRAY",
            # Merge-on-read deletes: which sidecar holds this data file's
            # delete vector, and how many of its rows are deleted. NULL / 0
            # (including on every manifest written before these columns
            # existed) means "no deletes" — readers must treat absence and
            # zero identically. record_count above stays PHYSICAL rows;
            # live rows are record_count - deleted_record_count. See
            # catalog/deletes.py and MOR_DELETES_DESIGN.md.
            "delete_file_path": "VARCHAR",
            "deleted_record_count": "INTEGER",
        }

        # Normalize entries to match the column set above:
        normalized = []
        for ent in entries:
            if not isinstance(ent, dict):
                continue
            e = dict(ent)
            # Ensure the numeric scalars exist AND are non-None: every
            # column below is written for every row, so a key an entry
            # didn't carry lands as SQL NULL and reads back as None, not
            # as the 0 that `entry.get(col, 0)` readers assume. That is
            # how manifests grew NULL sizes that later raised
            # `'<' not supported between instances of 'NoneType' and 'int'`
            # inside compaction's size comparisons.
            for _numeric in (
                "record_count",
                "file_size_in_bytes",
                "uncompressed_size_in_bytes",
                "deleted_record_count",
            ):
                if e.get(_numeric) is None:
                    e[_numeric] = 0
            # Ensure list fields exist
            e.setdefault("min_k_hashes", [])
            e.setdefault("histogram_counts", [])
            e.setdefault("histogram_bins", 0)
            e.setdefault("column_uncompressed_sizes_in_bytes", [])
            e.setdefault("null_counts", [])
            e.setdefault("min_lengths", [])
            e.setdefault("max_lengths", [])
            e.setdefault("field_ids", [])
            e.setdefault("char_class_counts", [])
            e.setdefault("char_total_bytes", [])
            e.setdefault("element_min_values", [])
            e.setdefault("element_max_values", [])
            e.setdefault("element_min_k_hashes", [])
            # delete_file_path is a nullable VARCHAR: None IS the "no deletes"
            # value, so setdefault only ensures the key exists for the writer.
            e.setdefault("delete_file_path", None)

            # min/max values are stored as compressed int64 values
            mv = e.get("min_values") or []
            xv = e.get("max_values") or []

            # Ensure int64 values are properly typed for min/max
            e["min_values"] = [int(v) if v is not None else None for v in mv]
            e["max_values"] = [int(v) if v is not None else None for v in xv]
            # Element bounds are the same int64 ordinals, over the child vector.
            e["element_min_values"] = [
                int(v) if v is not None else None for v in (e.get("element_min_values") or [])
            ]
            e["element_max_values"] = [
                int(v) if v is not None else None for v in (e.get("element_max_values") or [])
            ]

            # min_k_hashes / histogram_counts are per-column lists of ints,
            # so each entry is list[list[int]] and the column is a native
            # nested ARRAY<ARRAY<...>> (rugo's writer emits the 2-level LIST
            # encoding; read_manifest_columns reads it straight back). No
            # string encoding — min_k_hashes are full-range xxhash uint64,
            # stored with an unsigned leaf so values above INT64_MAX survive.
            normalized.append(e)

        from draken import draken_native as _dn

        morsel = Morsel()
        for name, dtype in columns.items():
            values = [e.get(name) for e in normalized]
            if name == "min_k_hashes":
                # UINT64 leaf: xxhash values span the full unsigned range; a
                # signed leaf would read back negative above INT64_MAX and
                # corrupt min-k ordering.
                #
                # Entries reach here in mixed forms during migration: freshly
                # computed (int hashes), decoded from a legacy comma-joined
                # manifest (int hashes, possibly NEGATIVE where a uint64 was
                # stored signed), a per-hash decimal-string list, or a single
                # comma-joined string per column. Normalize every hash to its
                # unsigned 64-bit value: a hash is a 64-bit identifier, not an
                # arithmetic quantity, so `int(h) & mask` recovers the true
                # uint64 (no-op for correct values, fixes legacy negatives)
                # and draken's UINT64 factory then accepts it.
                def _norm_col(col):
                    if col is None:
                        return None
                    if isinstance(col, str):  # legacy comma-joined column
                        col = col.split(",") if col else []
                    return [None if h is None else (int(h) & 0xFFFFFFFFFFFFFFFF) for h in col]

                values = [
                    None if entry is None else [_norm_col(col) for col in entry] for entry in values
                ]
                morsel.append_vector(
                    name,
                    _dn.vector_array_from_sequence(
                        values, element_type=_dn.DrakenType.UINT64.value, nesting_depth=2
                    ),
                )
            else:
                morsel.append_vector(name, vector_from_sequence(values, dtype=dtype))

        data = write_parquet(morsel, **WRITE_PARQUET_OPTIONS)

        if self.io:
            out = self.io.new_output(parquet_path).create()
            out.write(data)
            # close() is where the upload actually happens - the GCS output
            # stream buffers into memory and flushes on close - so a failure
            # here means the manifest object does not exist. Swallowing it
            # returned this path to the caller, which committed a snapshot
            # pointing at a manifest that was never written; the next commit
            # then couldn't read its parent. Let it raise.
            out.close()

        # Seed the parsed-manifest cache with the bytes just written: the next
        # commit reads this manifest back as its parent, and the cache hit
        # saves that download + parse. Seeding also replaces any stale entry
        # at the same path (two commits allocating the same millisecond
        # snapshot id), which is the invalidation this used to do.
        from .catalog.manifest import seed_parsed_manifest

        seed_parsed_manifest(parquet_path, data)

        return parquet_path

    def save_snapshot(self, identifier: str, snapshot: Snapshot) -> None:
        """Persist a single snapshot document for a dataset."""
        namespace, dataset_name = identifier.split(".")
        snaps = self._snapshots_collection(namespace, dataset_name)
        snaps.document(str(snapshot.snapshot_id)).set(_snapshot_to_document(snapshot))

    def _refuse_if_pointer_moved(self, doc_ref, identifier: str, expected) -> None:
        """Refuse the commit if another writer already moved the pointer.

        Read and compare inside a Firestore transaction so the check binds the
        write that follows it. Firestore refuses a read that follows a write in
        the same transaction, so the read comes first - the same ordering
        `create_dataset` uses.
        """
        from google.cloud import firestore

        from .exceptions import SnapshotRaceError

        @firestore.transactional
        def _check(transaction) -> None:
            doc = doc_ref.get(transaction=transaction)
            stored = doc.to_dict().get("current-snapshot-id") if doc.exists else None
            if stored != expected:
                raise SnapshotRaceError(
                    f"{identifier} moved while this commit was being built: it was "
                    f"built against snapshot {expected} but the dataset now points "
                    f"at {stored}. Nothing has been published - the files this "
                    "commit wrote are orphans the reclamation sweeps will collect. "
                    "Re-read the dataset and rebuild the commit against its current "
                    "snapshot."
                )

        _check(self.firestore_client.transaction())

    def save_dataset_metadata(
        self,
        identifier: str,
        metadata: DatasetMetadata,
        expected_current_snapshot_id=_NO_SNAPSHOT_EXPECTATION,
    ) -> None:
        """Persist dataset-level metadata and snapshots to Firestore.

        This writes the dataset document and upserts snapshot documents.

        `expected_current_snapshot_id` makes the write CONDITIONAL: the stored
        `current-snapshot-id` must still be that value or the write is refused
        with SnapshotRaceError. A commit passes the parent it built its manifest
        from; anything that does not move the pointer (an annotation, a
        description) passes nothing and writes unconditionally.

        The check and the write are ONE Firestore transaction. A read-back
        followed by a write would only narrow the race, never close it - which
        is exactly what compaction's `_dataset_moved_under_us` says about itself.
        """
        collection, dataset_name = identifier.split(".")
        doc_ref = self._dataset_doc_ref(collection, dataset_name)

        if expected_current_snapshot_id is not _NO_SNAPSHOT_EXPECTATION:
            self._refuse_if_pointer_moved(doc_ref, identifier, expected_current_snapshot_id)

        doc_ref.set(
            {
                "name": dataset_name,
                "collection": collection,
                "workspace": self.workspace,
                "location": metadata.location,
                "properties": metadata.properties,
                "format-version": metadata.format_version,
                "annotations": metadata.annotations,
                # The head pointer - see DatasetMetadata.current_snapshot_id.
                "current-snapshot-id": metadata.current_snapshot_id,
                "current-schema-id": metadata.current_schema_id,
                "timestamp-ms": metadata.timestamp_ms,
                "author": metadata.author,
                "description": metadata.description,
                "describer": metadata.describer,
                "maintenance-policy": metadata.maintenance_policy,
                "sort-orders": metadata.sort_orders,
                "refresh-frequency-mins": metadata.refresh_frequency_mins,
                # Materialized-view registration. This `set()` replaces the
                # whole document, so omitting these would de-register a
                # materialized view on its very first refresh commit.
                "dataset-type": metadata.dataset_type,
                "statement-id": metadata.statement_id,
                "source-tables": metadata.source_tables,
                "runs-as": metadata.runs_as,
                "suspended-at-ms": metadata.suspended_at_ms,
                "suspended-by": metadata.suspended_by,
                "last-refreshed-at-ms": metadata.last_refreshed_at_ms,
                "last-refresh-status": metadata.last_refresh_status,
                "last-refresh-execution-id": metadata.last_refresh_execution_id,
            }
        )

        # Metadata persisted in primary `datasets` collection only.

        snaps_coll = self._snapshots_collection(collection, dataset_name)
        # Upsert snapshot documents. Do NOT delete existing snapshot documents
        # here to avoid accidental removal of historical snapshots on save.
        # Serialized via _snapshot_to_document — the SAME writer save_snapshot
        # uses. These two both `.set()` the same document, so a field missing
        # from either one is destroyed by the other (see that function).
        for snap in metadata.snapshots:
            snaps_coll.document(str(snap.snapshot_id)).set(_snapshot_to_document(snap))

        # Upsert schema documents. Do NOT delete schema documents that are not
        # in `metadata.schemas` — the same rule the snapshot upsert above
        # follows, and for the same reason.
        #
        # `metadata.schemas` is NOT the complete set. The default
        # `load_dataset(load_history=False)` — which every write path uses —
        # populates it with the CURRENT schema only, so reconciling against it
        # deleted every older schema document on any commit. Each snapshot
        # records the schema id it was written under, so deleting those makes
        # time travel resolve a schema that no longer exists: an AS OF query
        # against an older snapshot fails, from an INSERT that had nothing to
        # do with schemas. Schema documents are small and bounded by the number
        # of schema changes; keeping them costs nothing worth this.
        schemas_coll = doc_ref.collection("schemas")
        for s in metadata.schemas:
            sid = s.get("schema_id")
            if not sid:
                continue
            schemas_coll.document(sid).set(
                {
                    "columns": s.get("columns", []),
                    "timestamp-ms": s.get("timestamp-ms"),
                    "author": s.get("author"),
                    "sequence-number": s.get("sequence-number"),
                }
            )

    def _schema_to_columns(self, schema: Any, field_ids: list | None = None) -> list:
        """Convert a schema into a simple columns list for storage.

        Each column is a dict:
        ``{"id": field-id, "name", "type", "element-type", "scale",
        "precision", "expectation-policies", "annotations"}``.

        Accepts a draken ``Morsel`` (the schema of a table being written), any
        relation-schema-like object exposing a ``.columns`` list of columns with
        ``.name``/``.column_type`` (duck-typed, so a real Opteryx
        ``RelationSchema`` works without importing opteryx-core here), or
        columns already in the stored spelling - either a list of
        ``{"name", "type"}`` dicts or a mapping with a ``"columns"`` key holding
        one.

        That last form is here because a caller that already knows its types as
        strings had no way in. ``create_dataset(schema={"columns": [...]})``
        reads as the obvious call, matches exactly what this method returns, and
        is what ``_expand_column_type`` already documents as supported for
        evolution - but it arrived here as a dict, whose ``columns`` is a key and
        not an attribute, and was refused as an unsupported schema type. A
        service that holds its own type vocabulary should not have to
        manufacture a fake ColumnType, or take a dependency on the query engine,
        to say ``VARCHAR``.

        The stored ``type`` is a category/type name (``INTEGER``, ``DECIMAL``,
        ``ARRAY``, ...) that round-trips through
        ``opteryx.types.logical_type.parse_column_type`` on the query-engine
        side.

        ``field_ids``, when provided, is a stable, catalog-allocated id per
        column (same order as ``entries``) used as ``"id"`` instead of a
        freshly-recomputed position — this is what lets manifest statistics
        stay keyed correctly across schema evolution. When omitted, falls
        back to the historical ``enumerate(..., start=1)`` behavior for
        callers that haven't been updated to pass allocated ids.
        """
        stored = _stored_columns_of(schema)
        if stored is not None:
            entries = [(name, quartet) for name, quartet in stored]
        elif hasattr(schema, "columns"):
            entries = [(col.name, _core_type_to_stored(col.column_type)) for col in schema.columns]
        elif hasattr(schema, "num_rows") and hasattr(schema, "column_names"):
            # Duck-typed as a draken Morsel. Don't check for `.schema`
            # directly — older draken releases (e.g. 0.4.2) don't expose it.
            from .catalog.manifest import morsel_schema_dict

            entries = [
                (name, _morsel_type_to_stored(dtype))
                for name, dtype in morsel_schema_dict(schema).items()
            ]
        else:
            raise ValueError(
                f"Unsupported schema type {type(schema).__name__}: expected a "
                "relation-schema-like object with a `.columns` attribute, a "
                "draken.morsels.morsel.Morsel, or columns already in the stored "
                'spelling - a list of {"name", "type"} dicts, or a mapping with a '
                '"columns" key holding one'
            )

        if field_ids is not None and len(field_ids) != len(entries):
            raise ValueError(
                f"field_ids length ({len(field_ids)}) does not match column count ({len(entries)})"
            )
        ids = field_ids if field_ids is not None else range(1, len(entries) + 1)

        cols = []
        for col_id, (name, (type_name, element_type, precision, scale)) in zip(ids, entries):
            cols.append(
                {
                    "id": col_id,
                    "name": name,
                    "type": type_name,
                    "element-type": element_type,
                    "scale": scale,
                    "precision": precision,
                    "expectation-policies": [],
                    "annotations": [],
                }
            )

        return cols

    def _write_schema(
        self,
        namespace: str,
        dataset_name: str,
        schema: Any,
        author: str,
        field_ids: list | None = None,
    ) -> str:
        """Persist a schema document in the dataset's `schemas` subcollection and
        return the new schema id.
        """
        cols = self._schema_to_columns(schema, field_ids=field_ids)
        return self._write_schema_columns(namespace, dataset_name, cols, author)

    def _write_schema_columns(
        self, namespace: str, dataset_name: str, cols: list, author: str
    ) -> str:
        """Persist an already-built column list as a new schema document.

        Split out of `_write_schema` so schema EVOLUTION (alter_dataset_schema)
        and schema CREATION share one writer. Evolution builds its columns by
        editing the current schema's stored dicts - preserving each surviving
        column's field id - rather than re-deriving them from a relation
        schema, which cannot express "this column keeps id 7".
        """
        import uuid

        doc_ref = self._dataset_doc_ref(namespace, dataset_name)
        schemas_coll = doc_ref.collection("schemas")
        sid = str(uuid.uuid4())

        # Nothing below is guarded, and each step used to be. Between them they
        # could return `sid` for a schema that has no columns, that is numbered
        # behind every schema already written, or that was never written at all
        # - and the callers stamp that id onto the dataset as
        # `current-schema-id` either way. A dataset pointing at a schema
        # document that does not exist is not a state worth reaching quietly;
        # failing here leaves the caller able to retry.
        now_ms = int(time.time() * 1000)
        if author is None:
            raise ValueError("author must be provided when writing a schema")

        # Determine next sequence number by scanning existing schema docs. An
        # absent subcollection streams empty rather than raising, so a failure
        # here means Firestore itself is unhealthy - and guessing 1 would put
        # this schema behind its own predecessors.
        max_seq = 0
        for d in schemas_coll.stream():
            sd = d.to_dict() or {}
            seq = sd.get("sequence-number") or 0
            if isinstance(seq, int) and seq > max_seq:
                max_seq = seq
        new_seq = max_seq + 1

        schemas_coll.document(sid).set(
            {
                "columns": cols,
                "timestamp-ms": now_ms,
                "author": author,
                "sequence-number": new_seq,
            }
        )
        return sid
