"""Proposing `maps` relationships from the data, for an owner to confirm.

Phase 1 let people declare relationships. Fourteen were, which proves the
mechanism and not the feature: a graph that only grows by hand does not grow.
This proposes candidates so that the owner's job is judging rather than
authoring.

WHAT THIS COSTS, measured on the production catalog (2026-08-29, 71 datasets,
1,265 columns, 2.67 billion rows):

    schema walk, every dataset                    ~6 min   (Firestore, serial)
    manifest statistics, every dataset              59 s   (one GCS read each)
    bounded value sketch, every dataset          18.6 min  (one bounded read each)
    the entire pair cascade over the account      0.09 s   (arithmetic)

The pair search is NOT the expensive part and never was. 1,265 columns give
799,480 naive pairs, which the statistics filters cut to about 4,700 measurable
ones, and each comparison is ~20 microseconds of set arithmetic. What costs is
building one sketch per column, and that is linear in DATASETS, not quadratic in
columns - which is the whole reason this design is affordable.

THE SKETCH IS THE MECHANISM. Every column already carries a 32-value KMV sketch
in the manifest (`min_k_hashes`), and it is free to read. It is also too small:
scored against the fifteen relationships people declared by hand, it recovered
seven. A KMV sketch samples the bottom of the hash space, so two sketches only
see the same region when their cardinalities are within roughly `k` of each
other - and `cisa_kev.cve_id` (1,685 values) against
`nvd_vulnerabilities.cve_id` (385,207) is 230x apart. The misses were not noise;
they were the estimator failing silently, which is the worst way for it to fail.

So this builds its own, at k=4096, from a bounded read. That recovered twelve of
the fifteen. Of the remaining three, one is a relationship the DATA DOES NOT
SUPPORT - `ops.stdout_log.insert_id -> ops.stderr_log.insert_id` scores 0.00
containment over 952 compared values, because the two log streams do not share
insert ids - and two are on a dataset whose sampled portion was empty.

WHAT IT CANNOT DO, and no amount of sampling would fix: value overlap cannot
tell a customer id from a star catalogue number. Two integer surrogate keys over
similar ranges contain each other perfectly and mean nothing by it. Two filters
carry that weight, and both are here rather than left to the reviewer:
a cardinality floor that applies only to numbers (a nine-value string domain
like a planet name is a real key; a nine-value integer domain is a counter), and
a same-collection restriction - all fifteen hand-declared relationships have
both ends in one collection, because a collection is how people group tables
that belong together.
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from dataclasses import field
from itertools import combinations
from typing import Any
from typing import Iterable
from typing import Iterator

logger = logging.getLogger(__name__)

#: How many hashes each column's sketch keeps. The manifest's own sketch is 32,
#: which is fine for pruning and demonstrably too small for judging - see the
#: module docstring. 4096 puts the usable cardinality ratio around 4096:1, which
#: covers every pair in the production catalog.
SKETCH_K = 4096

#: The most rows to read from one column before the sketch is called done.
#:
#: BOUNDED BY ROWS, NOT BY FILES, and that distinction is not academic: bounding
#: by files was measured at 315 seconds for a single file of
#: `test.nyc_taxicab_2021` against 1.4 seconds for a file of `cisa_kev`. A file
#: count is not a cost bound because file sizes vary by three orders of
#: magnitude; a row count is.
MAX_ROWS_SAMPLED = 2_000_000

#: Below this many distinct values a NUMERIC column is a counter, a status code
#: or a row ordinal - not an identity. Every obviously-wrong candidate in the
#: measured run came from two small integer domains coinciding.
NUMERIC_CARDINALITY_FLOOR = 256

#: Text needs no such floor. Nine planet names are a real key, and two text
#: domains do not coincide by arithmetic accident the way two integer ones do.
TEXT_CARDINALITY_FLOOR = 4

#: How close to unique the far end must be to be a plausible key, as a fraction
#: of its own non-null rows. Not 1.0: the cardinality is an estimate, and
#: demanding exactness would reject real keys on estimator noise.
KEY_UNIQUENESS = 0.80

#: How much wider the near end may be than the far end. A child column cannot
#: hold more distinct values than the key it references, give or take the
#: estimator.
WIDTH_SLACK = 1.5

#: The fewest values that may be compared before a containment score is
#: reported as evidence. A containment of 1.00 computed over three values is not
#: evidence, however confident it looks. Waived when the near sketch never
#: saturated, because then the sketch IS the column's complete distinct set and
#: the score is exact rather than sampled.
MIN_COMPARED = 32

#: The containment below which nothing is proposed. Measured: at 0.90 the
#: production catalog yields 36 proposals, twelve of which are relationships
#: people had already declared by hand.
MIN_CONTAINMENT = 0.90

#: Type families that can hold an identity. A BOOLEAN or a float is not one: two
#: boolean columns overlap perfectly and say nothing, and float equality is not
#: a join.
_FAMILY = {
    "INT8": "int", "INT16": "int", "INT32": "int", "INT64": "int",
    "UINT8": "int", "UINT16": "int", "UINT32": "int", "UINT64": "int",
    "INTEGER": "int",
    "DECIMAL": "decimal",
    "VARCHAR": "varchar", "NVARCHAR": "varchar",
    "BLOB": "blob", "VARBINARY": "blob",
    "IPV4": "ipv4",
}
_JOIN_SHAPED = frozenset({"int", "varchar", "decimal", "blob", "ipv4"})
_NUMERIC = frozenset({"int", "decimal"})


def type_family(spelling: Any) -> str:
    """The broad family of a stored column type, for compatibility checks.

    Parameterised and bracketed spellings both reach here - `DECIMAL(38, 9)`,
    `TIMESTAMP[us]` - so the base word is taken before either.
    """
    base = str(spelling or "").upper().split("(")[0].split("[")[0].strip()
    return _FAMILY.get(base, base.lower())


def value_hash(value: Any) -> int:
    """A 64-bit hash of one value, comparable ACROSS columns and datasets.

    Comparability is the whole requirement, and it is why this does not reuse
    the manifest's hash: two columns hold corresponding values only if the same
    value hashes identically on both sides, whatever their physical width. So an
    INT32 42 and an INT64 42 must collide, and the integer 42 and the string
    "42" must not.

    Canonical text plus a one-letter family tag does both. Slower than a native
    hash over a vector, and the cost lands on a bounded sample rather than on
    the whole column.
    """
    if isinstance(value, bool):
        payload = f"b:{value}"
    elif isinstance(value, int):
        payload = f"i:{value}"
    elif isinstance(value, (bytes, bytearray, memoryview)):
        payload = "s:" + bytes(value).decode("utf-8", errors="replace")
    else:
        payload = f"s:{value}"
    return int.from_bytes(hashlib.blake2b(payload.encode("utf-8"), digest_size=8).digest(), "big")


@dataclass
class ColumnProfile:
    """One column, as much of it as inference needs.

    `sketch` holds the SMALLEST `SKETCH_K` value hashes the column produced,
    sorted. That specific choice is what makes two profiles comparable: the
    smallest-k of a set is a deterministic sample of it, so two columns'
    sketches sample the same region of the hash space and can be intersected
    directly. A random sample of each would not intersect at all.
    """

    workspace: str
    collection: str
    dataset: str
    column: str
    family: str
    #: Distinct-value estimate, from the manifest's own KMV sketch.
    cardinality: int
    #: Rows minus nulls, for the key-likeness test.
    non_null: int
    sketch: list[int] = field(default_factory=list)
    #: False when the column produced fewer than SKETCH_K distinct values, which
    #: means the sketch is the column's COMPLETE distinct set rather than a
    #: sample of it - and a containment computed against it is exact.
    saturated: bool = False
    #: How many rows were actually read. Reported as evidence, because a
    #: proposal from a truncated read is weaker than one from a whole column and
    #: the owner should see which they have.
    rows_sampled: int = 0
    rows_total: int = 0

    @property
    def table(self) -> str:
        return f"{self.collection}.{self.dataset}"

    @property
    def qualified(self) -> str:
        return f"{self.workspace}.{self.collection}.{self.dataset}.{self.column}"


@dataclass
class RelationshipCandidate:
    """One directed pair that survived the cascade, with what was measured."""

    near: ColumnProfile
    far: ColumnProfile
    containment: float
    compared: int
    cardinality: str

    def evidence(self) -> dict:
        """What the owner is shown, and what gets stored on the proposal.

        The overlap ratio AND the sample size, never the score alone. "94% of
        1,685 values matched" is something a person can weigh; "confidence 0.94"
        is a number they can only take on trust, and taking a machine's word for
        it is the thing this whole review step exists to avoid.
        """
        return {
            "method": "value-overlap",
            "overlap": round(self.containment, 4),
            "values-compared": self.compared,
            "near-cardinality": self.near.cardinality,
            "far-cardinality": self.far.cardinality,
            "near-rows-sampled": self.near.rows_sampled,
            "near-rows-total": self.near.rows_total,
            "far-rows-sampled": self.far.rows_sampled,
            "far-rows-total": self.far.rows_total,
            # True when both sketches held their column's complete distinct set,
            # so the overlap is exact rather than estimated. Worth surfacing:
            # it is the difference between "we checked" and "we sampled".
            "exact": not self.near.saturated and not self.far.saturated,
        }


def build_column_profiles(
    catalog,
    read_column_values,
    *,
    collections: Iterable[str] | None = None,
) -> list[ColumnProfile]:
    """Profile every join-shaped column in one workspace.

    `read_column_values(dataset, column_names, max_rows)` is injected rather
    than implemented here, and that is the seam the deployment question turns
    on: this module decides WHAT to sample, and whoever runs the job decides how
    to read it - the engine, a worker, or a test harness handing back lists.
    It must yield `(column_name, values)` batches and respect `max_rows`.

    One read per DATASET, not per column: the columns of one dataset come out of
    the same parquet pass, so profiling all of them together costs what
    profiling one would.
    """
    profiles: list[ColumnProfile] = []

    for collection in collections if collections is not None else catalog.list_collections():
        for dataset_name in catalog.list_datasets(collection):
            identifier = f"{collection}.{dataset_name}"
            try:
                dataset = catalog.load_dataset(identifier)
                statistics = dataset.describe()
                # The type comes from the SCHEMA, not from describe() - which
                # reports what the manifest measured and says nothing about the
                # declared type. Without it every column would fall through
                # type_family as "unknown" and nothing would ever be proposed.
                relation_schema = dataset.schema()
                declared_types = {
                    column.name: getattr(column, "type", None)
                    for column in getattr(relation_schema, "columns", None) or []
                }
            except Exception as exc:  # noqa: BLE001 - one bad dataset is not the run
                # A dataset with no manifest, no schema or an unreadable one is
                # skipped rather than fatal. The job runs over a whole account
                # and stopping on the first oddity would mean it never completes
                # once - which is how a job like this quietly stops running.
                logger.warning("skipping %s: %s: %s", identifier, type(exc).__name__, exc)
                continue

            wanted = {}
            for column, stat in statistics.items():
                family = type_family(declared_types.get(column))
                if family not in _JOIN_SHAPED:
                    continue
                rows = int(stat.get("row_count") or 0)
                wanted[column] = ColumnProfile(
                    workspace=catalog.workspace,
                    collection=collection,
                    dataset=dataset_name,
                    column=column,
                    family=family,
                    cardinality=int(stat.get("cardinality") or 0),
                    non_null=max(rows - int(stat.get("null_count") or 0), 0),
                    rows_total=rows,
                )
            if not wanted:
                continue

            sketches: dict[str, set[int]] = {name: set() for name in wanted}
            sampled: dict[str, int] = {name: 0 for name in wanted}
            try:
                for column, values in read_column_values(
                    identifier, list(wanted), MAX_ROWS_SAMPLED
                ):
                    bucket = sketches.get(column)
                    if bucket is None:
                        continue
                    for value in values:
                        if value is None:
                            continue
                        sampled[column] += 1
                        bucket.add(value_hash(value))
                    if len(bucket) > SKETCH_K:
                        # Trimmed as it grows rather than at the end: an
                        # unbounded set over a high-cardinality column is how a
                        # profiling job runs a worker out of memory.
                        sketches[column] = set(sorted(bucket)[:SKETCH_K])
            except Exception as exc:  # noqa: BLE001 - as above
                logger.warning("could not sample %s: %s: %s", identifier, type(exc).__name__, exc)
                continue

            for column, profile in wanted.items():
                ordered = sorted(sketches[column])[:SKETCH_K]
                profile.sketch = ordered
                profile.saturated = len(ordered) >= SKETCH_K
                profile.rows_sampled = sampled[column]
                profiles.append(profile)

    return profiles


def _containment(near: ColumnProfile, far: ColumnProfile) -> tuple[float, int]:
    """(what fraction of near's values are in far, how many were compared).

    Counted directly, with no estimator. Each sketch holds the smallest hashes
    its column produced, so below the smaller of the two thresholds BOTH are
    complete - every value either column has in that region is in its sketch.
    Inside that region the answer is a straight count, and outside it neither
    sketch knows anything, so nothing outside it is counted.

    That shared region is also why the second return value matters. When the far
    column is far larger its threshold is far lower, and the region shrinks to
    almost nothing - the containment is then computed over a handful of values
    and means correspondingly little. `MIN_COMPARED` is the floor, and the count
    goes to the owner as evidence rather than being hidden inside a score.

    An UNSATURATED sketch has no threshold: it holds everything the column
    produced, so it is complete everywhere and imposes no limit.
    """
    if not near.sketch or not far.sketch:
        return 0.0, 0
    limit = min(
        near.sketch[-1] if near.saturated else float("inf"),
        far.sketch[-1] if far.saturated else float("inf"),
    )
    comparable = [value for value in near.sketch if value <= limit]
    if not comparable:
        return 0.0, 0
    far_values = set(far.sketch)
    matched = sum(1 for value in comparable if value in far_values)
    return matched / len(comparable), len(comparable)


def _plausible(near: ColumnProfile, far: ColumnProfile) -> bool:
    """The statistics-only filters, applied before anything is compared.

    Cheap by construction: every input already rides on the manifest, so this
    settles roughly 95% of pairs without reading a value. Ordered by how much
    each one cuts, which the measured run says is type first.
    """
    if near.family != far.family or near.family not in _JOIN_SHAPED:
        return False

    floor = NUMERIC_CARDINALITY_FLOOR if near.family in _NUMERIC else TEXT_CARDINALITY_FLOOR
    if near.cardinality < floor or far.cardinality < floor:
        return False

    # The far end has to look like a key. A column that repeats its values is
    # not something the near end references - it is another fact table.
    if far.non_null and far.cardinality < KEY_UNIQUENESS * far.non_null:
        return False

    # And the near end cannot be wider than the key it points at.
    if near.cardinality > WIDTH_SLACK * max(far.cardinality, 1):
        return False

    # Same collection. The single cheapest precision filter there is: all
    # fifteen relationships declared by hand in production have both ends in one
    # collection, and value overlap alone cannot distinguish a customer id from
    # a star catalogue number. It is a restriction, not a truth - a genuine
    # cross-collection relationship will not be proposed, and has to be declared
    # by hand exactly as it is today.
    return near.collection == far.collection


def _cardinality_of(near: ColumnProfile, far: ColumnProfile) -> str:
    """The fan-out this pair looks like, which the owner then overrides.

    A guess, and the weakest thing in the proposal - it is read off distinct
    counts, and getting it wrong is how a join silently inflates a number. That
    is why confirming takes the cardinality from the statement rather than from
    here.
    """
    if near.non_null and near.cardinality >= KEY_UNIQUENESS * near.non_null:
        return "one_to_one"
    return "many_to_one"


def score_candidates(profiles: list[ColumnProfile]) -> Iterator[RelationshipCandidate]:
    """Every directed pair worth proposing, best first.

    Both orientations are tried, because either end could be the key, and a pair
    can qualify in one direction and not the other - which is the direction the
    relationship actually runs.
    """
    by_workspace: dict[str, list[ColumnProfile]] = {}
    for profile in profiles:
        by_workspace.setdefault(profile.workspace, []).append(profile)

    candidates: list[RelationshipCandidate] = []
    for columns in by_workspace.values():
        for left, right in combinations(columns, 2):
            if left.table == right.table:
                continue
            for near, far in ((left, right), (right, left)):
                if not _plausible(near, far):
                    continue
                containment, compared = _containment(near, far)
                if containment < MIN_CONTAINMENT:
                    continue
                if compared < MIN_COMPARED and near.saturated:
                    continue
                if compared == 0:
                    continue
                candidates.append(
                    RelationshipCandidate(
                        near=near,
                        far=far,
                        containment=containment,
                        compared=compared,
                        cardinality=_cardinality_of(near, far),
                    )
                )

    candidates.sort(key=lambda candidate: (-candidate.containment, -candidate.compared))
    return iter(candidates)


def confidence_of(candidate: RelationshipCandidate) -> float:
    """One number for sorting, derived from the two that mean something.

    Deliberately NOT the thing an owner is asked to judge - `evidence()` is.
    This exists so a review queue has an order, and it is the containment
    discounted by how little was compared: a perfect overlap over 40 values is
    ranked below a 0.95 overlap over four thousand, because it is.
    """
    if candidate.compared <= 0:
        return 0.0
    # Saturates around a few thousand compared values, where more evidence stops
    # changing the answer.
    weight = min(candidate.compared / 1024.0, 1.0)
    return round(candidate.containment * (0.75 + 0.25 * weight), 4)


#: The containment at or above which a DECLARED relationship is confirmed to
#: still hold. Same bar as a proposal has to clear, deliberately: "good enough
#: to propose" and "good enough to still believe" are the same claim about the
#: same measurement, and two numbers here would only invite drift.
VERIFY_ACTIVE_ABOVE = MIN_CONTAINMENT

#: The containment below which a declared relationship is marked BROKEN.
#:
#: Far below the proposal bar, and that gap is the point. Breaking a person's
#: declaration is an accusation - it stops the relationship reaching BI clients
#: immediately - so it should fire when the columns plainly do not correspond,
#: not when sampling noise clipped a real key. `ops.stdout_log.insert_id ->
#: ops.stderr_log.insert_id`, the production row this whole mechanism exists to
#: catch, scores 0.00 over 952 compared values; nothing marginal is anywhere
#: near here.
#:
#: Between the two thresholds a row is INCONCLUSIVE: the score is recorded, the
#: row is stamped as checked, and the status is left exactly as it was. Sorting
#: that band out is a person's job, and the evidence is now on the row for them.
VERIFY_BROKEN_BELOW = 0.50


@dataclass
class VerificationResult:
    """One declared relationship, re-scored against the data as it is now."""

    #: The stored relationship document, as it was before this run.
    relationship: dict
    #: 'active' | 'broken' | 'inconclusive' | 'not-verifiable'
    outcome: str
    containment: float
    compared: int
    cardinality_declared: str | None
    #: What the fan-out actually looks like, or None when nothing was measured.
    cardinality_observed: str | None
    evidence: dict = field(default_factory=dict)

    @property
    def cardinality_contradicted(self) -> bool:
        """Whether the data disagrees with the declared fan-out.

        Worth its own flag rather than being folded into `outcome`, because it
        is a DIFFERENT failure with a different remedy and it can fire while
        the values correspond perfectly. A `many_to_one` that is really
        `many_to_many` inflates every number joined through it, silently, and
        the join still returns rows - so nothing else would ever notice.
        """
        if not self.cardinality_declared or not self.cardinality_observed:
            return False
        return self.cardinality_declared != self.cardinality_observed

    @property
    def status_to_write(self) -> str | None:
        """The `status` this outcome should store, or None to leave it alone.

        Only an ASSERTED row moves. A proposal stays 'unverified' whatever it
        scores: re-measuring a machine's own guess does not turn it into a
        claim, and only a person's confirmation does (§7.4). Its refreshed
        evidence still lands, which is what an owner reviewing the queue reads.
        """
        if self.relationship.get("origin") != "asserted":
            return None
        if self.outcome in ("active", "broken"):
            return self.outcome
        return None


def observed_cardinality(near: ColumnProfile, far: ColumnProfile) -> str:
    """The fan-out this pair actually has, assuming nothing about either end.

    NOT `_cardinality_of`, which is only ever reached after `_plausible` has
    already established that the far end is key-like. Verification has no such
    guarantee - a declared `many_to_one` whose far end has stopped being unique
    is precisely the thing being looked for - so the far end is tested here.
    """
    far_is_key = not far.non_null or far.cardinality >= KEY_UNIQUENESS * far.non_null
    if not far_is_key:
        # The far end repeats its values, so one near row matches many far
        # rows. Whatever was declared, this joins as many-to-many.
        return "many_to_many"
    if near.non_null and near.cardinality >= KEY_UNIQUENESS * near.non_null:
        return "one_to_one"
    return "many_to_one"


def _profile_index(profiles: list[ColumnProfile]) -> dict[tuple, ColumnProfile]:
    """Profiles keyed by (collection, dataset, column), case-folded.

    Case-folded because the stored relationship and the schema are two
    different people's spellings of the same name, and a verification that
    missed on case would report a real relationship as unverifiable forever.
    """
    return {
        (
            profile.collection.casefold(),
            profile.dataset.casefold(),
            profile.column.casefold(),
        ): profile
        for profile in profiles
    }


def verify_declared(
    catalog,
    profiles: list[ColumnProfile],
) -> list[VerificationResult]:
    """Re-score every declared `maps` pair against the profiles just built.

    THIS IS THE SAME OPERATION AS SCORING A CANDIDATE, which is why it lives
    here and rides this job rather than a second one. Re-checking a declared
    pair needs the same sketches, the same bounded read per dataset and the
    same containment arithmetic that inference already did; a standing
    verifier would re-derive all of it at full cost to answer a question this
    run can answer for microseconds. The proof is in the module docstring: the
    inference run FOUND the broken `ops` edge, while inferring.

    The plausibility cascade is deliberately NOT applied. Its filters exist to
    keep a review queue short - same collection, cardinality floors, far end
    key-like - and every one of them is a reason not to PROPOSE a pair, never a
    reason to disbelieve one a person declared. A cross-collection relationship
    is unproposable and perfectly true, and a far end that has stopped being
    key-like is a finding rather than a disqualification.

    Existence is not checked here. A missing profile means the column was not
    sampled - it may be the wrong type, on a dataset that failed to read, or in
    a collection this run was scoped away from - and inferring "gone" from "not
    measured" would mark half a catalog broken the first time a read failed.
    Existence is caught at DDL time instead, where it is known rather than
    guessed.
    """
    index = _profile_index(profiles)
    results: list[VerificationResult] = []

    for row in catalog.list_workspace_relationships():
        if row.get("kind") != "maps":
            # `concept` is unfalsifiable from data by construction and
            # `derives` is not scored from values. Both decay by their
            # endpoints disappearing, which is Half 1's business.
            continue

        near = index.get(
            (
                str(row.get("collection") or "").casefold(),
                str(row.get("dataset") or "").casefold(),
                str(row.get("column") or "").casefold(),
            )
        )
        far = index.get(
            (
                str(row.get("references-collection") or "").casefold(),
                str(row.get("references-dataset") or "").casefold(),
                str(row.get("references-column") or "").casefold(),
            )
        )
        declared = row.get("cardinality")

        if near is None or far is None:
            results.append(
                VerificationResult(
                    relationship=row,
                    outcome="not-verifiable",
                    containment=0.0,
                    compared=0,
                    cardinality_declared=declared,
                    cardinality_observed=None,
                )
            )
            continue

        containment, compared = _containment(near, far)

        # Too little was compared to mean anything. `MIN_COMPARED` is the same
        # floor a proposal has to clear, and for the same reason: a containment
        # of 0.00 computed over three values is not evidence of a break any
        # more than 1.00 over three is evidence of a relationship. The waiver
        # is the same too - an unsaturated near sketch IS the column's complete
        # distinct set, so its score is exact however few values it holds.
        if compared == 0 or (compared < MIN_COMPARED and near.saturated):
            results.append(
                VerificationResult(
                    relationship=row,
                    outcome="not-verifiable",
                    containment=containment,
                    compared=compared,
                    cardinality_declared=declared,
                    cardinality_observed=None,
                )
            )
            continue

        if containment >= VERIFY_ACTIVE_ABOVE:
            outcome = "active"
        elif containment < VERIFY_BROKEN_BELOW:
            outcome = "broken"
        else:
            outcome = "inconclusive"

        candidate = RelationshipCandidate(
            near=near,
            far=far,
            containment=containment,
            compared=compared,
            cardinality=observed_cardinality(near, far),
        )
        results.append(
            VerificationResult(
                relationship=row,
                outcome=outcome,
                containment=containment,
                compared=compared,
                cardinality_declared=declared,
                cardinality_observed=candidate.cardinality,
                # The same evidence shape a proposal carries, so one renderer
                # reads both and "94% of 1,685 values matched" means the same
                # thing whether it was proposed or re-checked.
                evidence={**candidate.evidence(), "method": "value-overlap-verification"},
            )
        )

    return results


def _write_verifications(
    catalog,
    results: list[VerificationResult],
    *,
    verifier: str,
) -> None:
    """Persist what verification observed, one narrow merge per row.

    Nothing is deleted, ever. A broken edge is KEPT with its last evidence -
    "these used to correspond and no longer do" is information, and removing
    the row would hide a data problem instead of surfacing one.

    A row nothing could be measured about is not written at all, not even to
    stamp `verified-at-ms`. That stamp is a claim that the pair was checked,
    and a reader comparing it against today is entitled to take it literally.
    """
    for result in results:
        if result.outcome == "not-verifiable":
            continue
        catalog.record_relationship_verification(
            f"{result.relationship.get('collection')}.{result.relationship.get('dataset')}",
            result.relationship.get("name"),
            status=result.status_to_write,
            evidence=result.evidence,
            cardinality_observed=result.cardinality_observed,
            verifier=verifier,
        )


def run_for_workspace(
    catalog,
    read_column_values,
    *,
    actor: str,
    collections: Iterable[str] | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    verify: bool = True,
) -> dict:
    """One pass over a workspace: profile once, then verify AND propose.

    ONE JOB, TWO OUTPUTS, and that is the whole point of putting them here.
    Profiling is the entire cost of this run - 18.6 minutes of bounded reads
    against 0.09 seconds of arithmetic on the production catalog - and both
    questions are answered from the same profiles. Verification asks "do these
    two columns still correspond" of pairs a person declared; inference asks it
    of pairs nobody has. Same sketches, same maths, same read. A separate
    verifier would pay the 18.6 minutes again to learn nothing new.

    `dry_run` scores and writes nothing, which is how this should be run the
    first time against any real catalog: the output is a review queue a person
    will have to work through, and one badly-tuned threshold turns it into a
    queue nobody works through at all. It is doubly worth it now that a run can
    also mark a declared relationship broken.

    Returns `{"profiled", "verified", "proposed"}`. `verified` is every
    `VerificationResult`, including the ones nothing could be measured about,
    because "we could not check this" is what a caller needs in order to tell a
    person their edge has not actually been looked at.

    Writing proposals goes through `catalog.propose_relationship`, which
    refuses a suppressed pair and refuses to overwrite anything a person
    asserted. Those checks are deliberately NOT repeated here - a suppression
    that only holds when the caller remembers to look is not a suppression.
    """
    profiles = build_column_profiles(catalog, read_column_values, collections=collections)
    logger.info("profiled %d columns in %s", len(profiles), catalog.workspace)

    verified: list[VerificationResult] = []
    if verify:
        verified = verify_declared(catalog, profiles)
        if not dry_run:
            _write_verifications(catalog, verified, verifier=actor)
        broken = [result for result in verified if result.outcome == "broken"]
        contradicted = [result for result in verified if result.cardinality_contradicted]
        logger.info(
            "verified %d relationships in %s: %d broken, %d with a contradicted "
            "cardinality, %d not measurable",
            len(verified),
            catalog.workspace,
            len(broken),
            len(contradicted),
            sum(1 for result in verified if result.outcome == "not-verifiable"),
        )

    proposed = []
    for candidate in score_candidates(profiles):
        if limit is not None and len(proposed) >= limit:
            break
        record = {
            "near": candidate.near.qualified,
            "far": candidate.far.qualified,
            "cardinality": candidate.cardinality,
            "confidence": confidence_of(candidate),
            "evidence": candidate.evidence(),
        }
        if not dry_run:
            name = catalog.propose_relationship(
                dataset_identifier=candidate.near.table,
                column=candidate.near.column,
                references_dataset=candidate.far.table,
                references_column=candidate.far.column,
                cardinality=candidate.cardinality,
                confidence=record["confidence"],
                evidence=record["evidence"],
                proposer=actor,
            )
            if name is None:
                # Already settled - suppressed, or asserted by someone. Not an
                # error and not worth a log line per pair; it is the normal
                # steady state once a catalog has been reviewed once.
                continue
            record["constraint_name"] = name
        proposed.append(record)

    logger.info("proposed %d relationships in %s", len(proposed), catalog.workspace)
    return {"profiled": len(profiles), "verified": verified, "proposed": proposed}


def propose_for_workspace(
    catalog,
    read_column_values,
    *,
    proposer: str,
    collections: Iterable[str] | None = None,
    limit: int | None = None,
    dry_run: bool = False,
) -> list[dict]:
    """Inference only, without the verification pass. Returns what it proposed.

    Kept as the narrow entrance for a caller that genuinely wants proposals and
    nothing else - a test, or a first run against a workspace with nothing
    declared in it yet. `run_for_workspace` is what a scheduled job should
    call: it answers both questions off one set of profiles, and calling this
    and then verifying separately pays for the profiling twice.
    """
    return run_for_workspace(
        catalog,
        read_column_values,
        actor=proposer,
        collections=collections,
        limit=limit,
        dry_run=dry_run,
        verify=False,
    )["proposed"]
