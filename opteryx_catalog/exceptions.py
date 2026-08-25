"""Catalog-specific exceptions for opteryx_catalog.

Exceptions mirror previous behavior (they subclass KeyError where callers
may expect KeyError) but provide explicit types for datasets, views and
namespaces.
"""


class CatalogError(Exception):
    """Base class for catalog errors."""


class AlertSeverity:
    """Severities for alertable failures, spelled as Cloud Logging spells them.

    The stdout sink writes this value straight into `severity`, where Cloud
    Logging promotes a recognised level onto the LogEntry. This is deliberately
    the opposite of `audit.py`, which uses a value Cloud Logging does NOT
    recognise so that the value survives in the payload for the audit filter to
    match on. A real severity here is what routes alerts to `ops.stdout_logs`
    instead of `ops.audit_log`.

    Three levels, no INFO: an alert nobody needs to act on is a log line.
    """

    WARNING = "WARNING"  # an inconsistency was observed; the work continued
    ERROR = "ERROR"  # an operation refused or aborted; data is intact
    CRITICAL = "CRITICAL"  # data is being lost, or already has been


class Alertable:
    """Marks an exception as one a human has to be told about.

    Carries data only - no `__init__`, no behaviour, no imports. That is what
    lets it compose with the `KeyError`-derived exceptions below without MRO
    surgery, keeps constructing one free, and keeps this module import-free.
    Deciding when to report, and rendering a ticket, belong to `alerts/`; an
    exception that knew how to file itself would drag transport policy into the
    exception hierarchy.

    List the mixin FIRST in the bases so its attributes win:

        class ManifestReadError(Alertable, CatalogError): ...

    Reserved for failures that mean the platform itself is broken. Caller errors
    - a missing dataset, a locked collection - must never be alertable, or every
    404 files a ticket. `tests/test_alertable_exceptions.py` asserts that.

    Identity (which occurrences fold into one ticket) is computed by
    `alerts._identity`, from the exception type and the call path. The type name
    is already in that material, so two different Alertable types raised at the
    same site never collide. `alert_fingerprint` adds to it, and is needed when
    ONE type covers several distinct conditions reachable from the same
    function - then the auto path cannot tell them apart. If in doubt, supply
    one; over-splitting costs a duplicate ticket, under-splitting hides a
    failure behind an unrelated one.
    """

    # Defaults live on the class; a raise site can override any of them on the
    # instance, because Python looks the instance up first.
    alert_severity: str = AlertSeverity.ERROR
    alert_labels: tuple = ()
    alert_summary: str = ""
    alert_fingerprint: tuple = ()

    def alert_context(self) -> dict:
        """Detail a human needs to reproduce this. Never part of the identity."""
        return dict(getattr(self, "_alert_context", None) or {})

    def with_alert_context(self, **fields):
        """Attach reproduction detail at the raise site, chainably.

        raise ManifestReadError(msg).with_alert_context(dataset=identifier)
        """
        merged = self.alert_context()
        merged.update(fields)
        self._alert_context = merged
        return self


# The name this capability was requested under. `Alertable` is preferred: the
# mixin describes a property of the failure, not a transport, and the default
# sink is stdout rather than GitHub.
GitHubAlertable = Alertable


class DatasetError(KeyError, CatalogError):
    pass


class DatasetAlreadyExists(DatasetError):
    pass


class DatasetNotFound(DatasetError):
    pass


class ViewError(KeyError, CatalogError):
    pass


class ViewAlreadyExists(ViewError):
    pass


class ViewNotFound(ViewError):
    pass


class TriggerNotFound(KeyError, CatalogError):
    pass


class TagError(CatalogError):
    """Base for snapshot-tag failures."""


class TagNotFound(KeyError, TagError):
    pass


class TagAlreadyExists(TagError):
    """A tag of that name already exists on the dataset.

    Tags are immutable, so this is never resolved by overwriting: the caller
    drops the existing tag first, which is the act that unpins its snapshot.
    """


class TagLimitExceeded(TagError):
    """The dataset already holds the maximum number of tags.

    Tags pin their snapshot from expiry indefinitely, so the limit is the only
    bound on how much history one dataset can hold alive.
    """


class MaterializedViewError(CatalogError):
    """A materialized-view registration or drop that cannot proceed:
    the named dataset is not a materialized view, a source is invalid,
    a view would be stacked on another view, or the source graph would
    contain a cycle."""


class CollectionAlreadyExists(KeyError, CatalogError):
    pass


class CollectionNotFound(KeyError, CatalogError):
    pass


class CollectionNotEmpty(CatalogError):
    pass


class CollectionLocked(CatalogError):
    """Raised by `drop_collection` when the collection's `locked-by` field is set."""


class DatasetLocked(CatalogError):
    """Raised by `drop_dataset` when the dataset's `locked-by` field is set."""


class WorkspaceNotFound(KeyError, CatalogError):
    """Raised by `OpteryxCatalog.__init__` when the workspace has no
    `$properties` document and `create_if_missing` was not passed.

    Constructing a handle is a read, not a provisioning step: a mistyped
    workspace name must not bring a workspace into existence. Callers that
    genuinely provision pass `create_if_missing=True`."""


class WorkspaceDeletionProtected(CatalogError):
    """Raised by `drop_workspace` when the workspace is deletion-protected.

    Protection is ON unless `deletion_protection` is explicitly turned off, so
    this is the *default* answer for a workspace nobody has configured - see
    `opteryx_catalog._guard_is_on`. Deleting a workspace is a deliberate
    two-step: clear the flag, then delete.

    Scope is the workspace itself: dropping datasets, collections and views
    inside a protected workspace is unaffected. Per-asset protection is the
    `locked-by` two-person lock (`DatasetLocked`/`CollectionLocked`), a separate
    mechanism cleared by an unlock rather than by a property change."""


class WorkspaceStorageReclaimFailed(Alertable, CatalogError):
    """`drop_workspace` could not confirm every dropped dataset's storage
    was actually reclaimed.

    Raised instead of silently deleting the workspace's `$properties` doc
    anyway: once that doc is gone, nothing can construct a normal handle on
    this workspace again to retry a leftover tombstone, so a file that
    failed to delete here would be orphaned with no path back to it short
    of a manual, out-of-band fix. The message names every tombstone that
    did not fully reclaim.

    Alertable because this means storage is now permanently unreachable
    through the normal lifecycle, not that anything was left "still there"
    in a way a caller can retry on their own.
    """

    alert_severity = AlertSeverity.ERROR
    alert_summary = "A workspace drop could not confirm all storage was reclaimed."


class EgressRestricted(CatalogError):
    """A copy would move data out of a workspace whose `egress_protection`
    property is set.

    Raised by the materialized-view creation and refresh paths, and intended
    for the CTAS path too (`OpteryxCatalog.enforce_egress_policy` is the shared
    gate) - anything that writes a *durable copy* of a source workspace's data
    into a different workspace.

    Deliberately its own type rather than a `MaterializedViewError`: the
    condition has nothing to do with the view being well-formed, and CTAS -
    which has no materialized view anywhere in it - needs to raise the same
    thing.

    Not `Alertable`. A blocked copy is the setting working, not the platform
    breaking. The trigger-firing path alerts on it separately, because there a
    blocked refresh also means a view is going stale."""


class MaterializedViewOwnerMissing(Alertable, CatalogError):
    """A registered materialized view has no `runs-as` identity.

    Every registration writes one, and nothing but `set_materialized_view_owner`
    rewrites it, so its absence means the record was damaged - a document edited
    out of band, or a write path that replaced the dataset document without
    carrying the field (the trap `DatasetMetadata` exists to close).

    `Alertable` because it is a should-be-impossible state, not a caller error.
    And raised rather than defaulted, deliberately: the obvious fallback is the
    identity of whoever's commit fired the refresh, which is exactly the invoker
    behaviour a pinned owner exists to remove. A refresh under the wrong
    identity either fails confusingly - the failure looks like a permissions
    problem hours away from the write that caused it - or, worse, succeeds with
    more authority than the view was granted. Refusing to guess turns a silent
    revert into one loud, findable failure."""


class ManifestReadError(Alertable, CatalogError):
    """A parent snapshot's manifest could not be read while building a commit.

    Manifests are cumulative: every commit carries the previous snapshot's
    entries forward. A manifest that cannot be read is NOT an empty manifest —
    treating it as one writes a manifest listing only the newly added files and
    silently orphans everything committed before it. Because the snapshot
    summary is derived from the entries actually written, the loss leaves no
    trace in the catalog and the next commit builds on the truncated state.

    Raised instead of degrading, so the commit fails and the data files stay
    referenced by the previous snapshot.
    """

    alert_severity = AlertSeverity.CRITICAL
    alert_labels = ("data-loss-risk",)
    alert_summary = "A parent manifest was unreadable; the commit was refused."


class AddFilesReadError(CatalogError):
    """A file named in `add_files` could not be read to compute its statistics.

    Those methods register files that are already in storage, so a file that
    cannot be read means the caller named something that is not there, or not
    readable with these credentials. That is a caller error, which is why this
    is deliberately NOT `Alertable` - see the note on that class.

    Both call sites used to substitute a placeholder manifest entry recording
    zero rows and zero bytes. The commit then succeeded, the snapshot summary
    undercounted by however many rows the file actually held, and no signal
    was emitted anywhere. Refusing leaves the dataset exactly as it was, which
    is a state the caller can retry from.
    """


class ManifestProtectionError(Alertable, CatalogError):
    """Garbage collection could not establish which files are still in use.

    Expiration and deep-clean decide what to delete by subtracting the set of
    files referenced by retained snapshots from what is physically present. If
    a retained snapshot's manifest cannot be read, that set comes back short
    and every file it protected looks like an orphan — so an unreadable
    manifest turns a reclaim pass into a delete-everything pass.

    Raised to abort the pass for that dataset. Nothing is deleted; the files
    remain and the run can be retried once the manifest is readable.
    """

    alert_severity = AlertSeverity.CRITICAL
    alert_labels = ("data-loss-risk", "gc")
    alert_summary = "Garbage collection could not establish what to protect."


class ManifestRefreshError(Alertable, CatalogError):
    """A statistics refresh could not recompute every file's statistics.

    Raised by `SimpleDataset.refresh_manifest` when one or more data files
    could not be re-read or re-analyzed. No snapshot is committed when this
    is raised: a manifest where some files carry fresh statistics and others
    silently kept stale ones is indistinguishable, downstream, from one that
    fully succeeded — so the refresh fails whole rather than committing a
    partial result. The message names every file that failed.
    """

    alert_severity = AlertSeverity.ERROR
    alert_summary = "A statistics refresh could not recompute every file."


class QuarantineUnavailable(Alertable, RuntimeError, CatalogError):
    """The orphan quarantine record could not be read or written.

    Raised rather than returning an empty record: an empty record is
    indistinguishable from "nothing is quarantined", which would let a caller
    quarantine everything afresh each run and never delete anything, or - worse,
    if the failure were on the write side - delete on a first sighting.

    Alertable because a permanently unreadable record means garbage collection
    stops reclaiming anything, silently and indefinitely. It stays a
    `RuntimeError` as well, so the `except QuarantineUnavailable` handlers that
    predate the hierarchy keep behaving identically; joining `CatalogError` is
    what brings it in line with every other failure this package raises.

    Defined here rather than beside its raiser so the exception hierarchy has
    one home; `catalog.orphan_quarantine` re-exports it.
    """

    alert_severity = AlertSeverity.ERROR
    alert_labels = ("gc",)
    alert_summary = "Orphan quarantine unavailable; reclamation is stalled."


class CompactionInvariantError(Alertable, CatalogError):
    """A compaction pass violated an invariant it checks before committing.

    Covers the row-count balance (input rows != written rows) and a failure to
    rebuild a corrupted surviving manifest entry. Either means the compactor
    would have committed a manifest that does not describe the data - an
    inverted predicate, a decoder regression, a mis-derived chunk group. The
    pass aborts and deletes what it wrote, so nothing is lost; the alert exists
    because a compactor quietly declining to compact looks exactly like a
    compactor with nothing to do.

    CONSTRUCTED FOR REPORTING, NOT RAISED. The abort paths already return None
    and their callers handle it; raising here would change compaction's control
    flow to gain an alert it can have without that.

    One type covers two conditions, so raise sites must name the condition in
    the fingerprint rather than relying on the automatic call-path split.
    """

    alert_severity = AlertSeverity.CRITICAL
    alert_labels = ("data-loss-risk", "compaction")
    alert_summary = "A compaction invariant failed; the pass was aborted."


class SummaryInconsistencyError(Alertable, CatalogError):
    """A snapshot's recorded totals disagree with the manifest they describe.

    Evidence that an earlier commit wrote a manifest inconsistent with its
    summary - the shape of the 2026-08-05 truncation, where summaries kept
    reporting the pre-loss row count over a table that had lost its history.

    CONSTRUCTED FOR REPORTING, NOT RAISED, and deliberately non-fatal: totals
    for the snapshot being written are derived from the manifest, so proceeding
    repairs the counters, while refusing would strand the dataset in its
    corrupt state with no way to commit its way out.

    Named `...Error` rather than `...Warning` because it descends from
    `CatalogError`; a `Warning` suffix would suggest Python's warnings module.
    """

    alert_severity = AlertSeverity.WARNING
    alert_labels = ("data-integrity",)
    alert_summary = "A snapshot summary disagrees with its manifest."


class SnapshotMissingError(Alertable, CatalogError):
    """A dataset names a current snapshot whose document does not exist.

    The metastore-side analogue of a manifest 404: the dataset loads as empty
    rather than failing, so every reader sees a table with no data and no error.
    Garbage collection then sees nothing to protect.

    CONSTRUCTED FOR REPORTING, NOT RAISED in this tranche - making it fatal
    would change what `load_dataset` does to callers who currently get an empty
    dataset, which needs its own change.
    """

    alert_severity = AlertSeverity.CRITICAL
    alert_labels = ("data-loss-risk", "metastore")
    alert_summary = "A dataset's current snapshot document is missing."


class CredentialsUnavailable(Alertable, OSError, CatalogError):
    """No usable credential could be obtained for a storage request.

    Separated from `StorageReadError` because the two send you to opposite
    places: a 403 that reaches the caller means the identity was rejected, and
    this means no identity was presented at all. Conflating them cost an evening
    - a swallowed refresh failure produced anonymous requests, GCS answered 403,
    and the 403 pointed at an IAM configuration that turned out to be correct.

    Alertable: a service that cannot authenticate to its own storage is broken
    outright, and every read it attempts will fail until someone intervenes.
    """

    alert_severity = AlertSeverity.CRITICAL
    alert_labels = ("storage", "auth")
    alert_summary = "Storage credentials could not be obtained; reads are failing."


class StorageReadError(OSError, CatalogError):
    """An object read failed for a reason that is NOT "the object isn't there".

    The distinction matters because the whole catalog treats `FileNotFoundError`
    as a fact about the data: a missing manifest means a dataset really has lost
    its history, and callers act on that. A 403 from a permissions change, a 429
    from throttling, a 503 from the storage service - none of those say anything
    about whether the object exists, and reporting them as "not found" sends
    every one of those callers down the wrong branch with an error message that
    names a path which is sitting right there in the bucket.

    Carries the HTTP status and the response body so the failure names its own
    cause. `status` is None when the read never got a response at all (a
    connection error or timeout that survived the retries).

    Subclasses `OSError` so the broad `except OSError` handlers at the storage
    boundary keep catching it, and so it matches what the S3 implementation has
    always raised for the same condition.

    Deliberately NOT `Alertable`. This is raised from the IO layer, under both
    platform-driven reads and caller-supplied paths (`add_files`), so it cannot
    know which it is - see the note on `Alertable`. The paths where an
    unreadable object does mean the platform is broken already wrap it in
    something that alerts: `ManifestReadError`, `ManifestProtectionError`.
    """

    def __init__(self, message: str, *, status: int | None = None, body: str = ""):
        super().__init__(message)
        self.status = status
        self.body = body


class InvalidCatalogBinding(ValueError, CatalogError):
    """A workspace catalog-binding write was rejected before reaching Firestore.

    Raised by `opteryx_catalog.binding`'s validation: an unusable `kind`, a
    reserved key in `config` (`workspace`/`connector`/`prefix` - the engine
    injects or strips those itself), or an inconsistent auth block (a stored
    credential missing its ciphertext/key/target, an ambient one carrying
    them). Validation happens at write time on purpose: a malformed binding
    must fail where its author can see the error, not at some later query's
    resolution.

    Subclasses ValueError so callers that validate inputs generically catch it
    without importing catalog-specific types. Deliberately NOT Alertable: a
    rejected write is a caller error, not a platform failure.
    """


class SnapshotRaceError(CatalogError):
    """Raised when a commit's parent snapshot is no longer the dataset's current one.

    Every commit writes NEW immutable files - `manifest-<id>-<nonce>.parquet`,
    `deletes-<id>-<nonce>.parquet` - and then moves one pointer: the dataset
    document's `current-snapshot-id`. Nothing is ever overwritten, so the only
    contended resource is that pointer.

    Two writers reading the same parent each build a complete manifest from it,
    and whichever moves the pointer second wins outright: the loser's data files
    are intact on disk and referenced by nothing, its rows are gone, and it
    reported success. The dataset document is written whole, so the loser's
    entry in the snapshot history goes with it - there is not even a snapshot to
    point back at.

    The commit is therefore conditional on the pointer still holding the parent
    it was built against. This is a real compare-and-swap at the store, not a
    read-back: a read-back can only narrow the window, never close it (see
    compaction's `_dataset_moved_under_us`, which says as much).

    Retrying is usually correct but is NOT automatic, because whether the work
    survives the race depends on what won it. A winner that appended leaves an
    earlier writer's assumptions intact; a winner that compacted has moved rows
    between files, which invalidates any row addresses computed against the old
    manifest - the case MERGE cares about.
    """
