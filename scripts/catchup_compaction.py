#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Catch up a backlog of compaction on one or more datasets, directly against
``opteryx_catalog`` - no SQL, no planner, no operator, no opteryx query
session at all. ``DatasetCompactor.compact()`` only performs ONE pass per
call (one read -> select -> execute -> commit cycle, see
``opteryx_catalog/catalog/compaction.py``), so this script just calls it in a
loop, per dataset, until a pass finds nothing left to do. Meant for a
long-running VM instead of a container with a request timeout - same
rationale as ``dev/refresh_catalog_manifest.py`` in opteryx-core.

Unlike a manifest refresh, compaction commits a NEW snapshot after every
single pass, so progress is durable as it goes: a crash, Ctrl-C, or one
dataset erroring out only loses the in-flight pass, not prior ones. This
script leans on that - it logs and moves on to the next dataset on error
instead of aborting the whole run, and Ctrl-C exits cleanly rather than
dumping a traceback.

Usage:

    python scripts/catchup_compaction.py opteryx.prod.github_events opteryx.prod.gdelt_events

Requires the same environment production compaction needs: GCP credentials
(``GOOGLE_APPLICATION_CREDENTIALS``) plus ``GCP_PROJECT_ID``,
``FIRESTORE_DATABASE``, and ``GCS_BUCKET`` - either exported in the shell or
in a ``.env`` file (``import opteryx`` loads one via dotenv, same as
production and ``tests/integration/test_catalog_gcs_scan.py``).
"""

from __future__ import annotations

import argparse
import getpass
import logging
import os
import sys
import time

sys.path.insert(1, os.path.join(os.path.dirname(__file__), ".."))

# Importing opteryx loads .env via dotenv, which is where GCP credentials and
# the catalog identifiers below typically come from. Import before reading
# os.environ. Nothing else about opteryx (session, planner, connector
# registry) is used below - the catalog is driven directly.
import opteryx  # noqa: E402,F401
from opteryx_catalog import OpteryxCatalog  # noqa: E402
from opteryx_catalog.catalog.compaction import DatasetCompactor  # noqa: E402

_REQUIRED_ENV = ("GCP_PROJECT_ID", "FIRESTORE_DATABASE", "GCS_BUCKET")

log = logging.getLogger("catchup_compaction")


def _parse_identifier(name: str) -> tuple:
    """Split ``workspace.namespace.dataset`` into (workspace, relative_id),
    matching OpteryxConnector._parse_identifier: split on the FIRST dot."""
    parts = name.split(".", 1)
    if len(parts) != 2:
        raise ValueError(
            f"'{name}' is not a fully qualified dataset name "
            "(expected workspace.namespace.dataset)"
        )
    return parts[0], parts[1]


def catchup(
    dataset_name: str,
    agent: str,
    author: str,
    strategy: str | None,
    max_passes: int,
    sleep_seconds: float,
) -> tuple:
    """Drive one dataset's ``DatasetCompactor`` until a pass finds nothing
    left to compact, or ``max_passes`` is hit (a safety valve, not an
    expected outcome for a converging backlog).

    Returns (passes_run, files_deleted, files_added) for the run summary.
    """
    missing = [k for k in _REQUIRED_ENV if not os.environ.get(k)]
    if missing:
        raise RuntimeError(f"missing required environment variable(s): {', '.join(missing)}")

    workspace, relative_id = _parse_identifier(dataset_name)

    catalog = OpteryxCatalog(
        workspace=workspace,
        firestore_project=os.environ["GCP_PROJECT_ID"],
        firestore_database=os.environ["FIRESTORE_DATABASE"],
        gcs_bucket=os.environ["GCS_BUCKET"],
    )
    dataset = catalog.load_dataset(relative_id)

    # One compactor per dataset, reused across passes: compact() mutates
    # dataset.metadata in place on commit (appends the new snapshot, updates
    # current_snapshot_id), so the next pass's manifest read already sees it -
    # no need to reload the dataset between passes.
    compactor = DatasetCompactor(dataset, strategy=strategy, author=author, agent=agent)
    log.info(
        "[%s] starting catch-up: strategy=%s (decision=%s)",
        dataset_name, compactor.strategy, compactor.decision,
    )

    passes = 0
    total_deleted = 0
    total_added = 0
    dataset_started = time.monotonic()

    while max_passes <= 0 or passes < max_passes:
        plan = compactor.compact(dry_run=True)
        if not plan:
            log.info("[%s] no more compaction opportunities - caught up", dataset_name)
            break

        passes += 1
        log.info(
            "[%s] pass %d: %s (%s), %d input file(s)%s",
            dataset_name, passes, plan.get("type"), plan.get("reason"),
            len(plan.get("files", [])),
            f", sort_column={plan['sort_column']}" if plan.get("sort_column") else "",
        )

        started = time.monotonic()
        snapshot = compactor.compact(dry_run=False)
        elapsed = time.monotonic() - started

        if not snapshot:
            # The dry-run plan existed but execution declined or failed (e.g.
            # a merge too large for the RAM gate, see MAX_SELECTED_BUDGET_BYTES).
            # Nothing was committed; stop rather than spin on the same plan.
            log.warning(
                "[%s] pass %d: plan found but execution produced no snapshot - stopping",
                dataset_name, passes,
            )
            break

        summary = snapshot.summary
        deleted = summary.get("deleted-data-files", 0)
        added = summary.get("added-data-files", 0)
        total_deleted += deleted
        total_added += added
        log.info(
            "[%s] pass %d committed in %.1fs: %d files -> %d files (total now %d files)",
            dataset_name, passes, elapsed, deleted, added,
            summary.get("total-data-files", -1),
        )

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    else:
        log.warning(
            "[%s] hit --max-passes=%d with more compaction likely remaining - "
            "rerun to continue", dataset_name, max_passes,
        )

    dataset_elapsed = time.monotonic() - dataset_started
    log.info(
        "[%s] done: %d pass(es) in %.1fs, %d files deleted, %d files added",
        dataset_name, passes, dataset_elapsed, total_deleted, total_added,
    )
    return passes, total_deleted, total_added


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "dataset",
        nargs="+",
        help="Fully qualified dataset name(s), e.g. opteryx.prod.github_events",
    )
    parser.add_argument(
        "--strategy",
        default=None,
        choices=["brute", "performance"],
        help="Force a compaction strategy instead of auto-detecting from the "
        "dataset's sort order (default: auto).",
    )
    parser.add_argument(
        "--agent",
        default="opteryx-compaction-catchup",
        help="Recorded in the catalog audit log / snapshot summary as who/what "
        "performed the compaction (default: %(default)s).",
    )
    parser.add_argument(
        "--author",
        default=None,
        help="Snapshot author. Defaults to the current OS user; pass an explicit "
        "value or an empty string to leave it unattributed.",
    )
    parser.add_argument(
        "--max-passes",
        type=int,
        default=0,
        help="Safety cap on compaction passes per dataset. 0 (default) means "
        "unlimited - loop until a pass finds nothing left to compact.",
    )
    parser.add_argument(
        "--sleep-between-passes",
        type=float,
        default=0.0,
        help="Seconds to sleep between passes on the same dataset, to ease off "
        "GCS/Firestore (default: 0, no sleep).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Root log level (default: %(default)s).",
    )
    args = parser.parse_args()
    author = getpass.getuser() if args.author is None else (args.author or None)

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        stream=sys.stdout,
        force=True,
    )

    run_started = time.monotonic()
    grand_passes = grand_deleted = grand_added = 0
    failed = []

    try:
        for dataset_name in args.dataset:
            try:
                passes, deleted, added = catchup(
                    dataset_name,
                    agent=args.agent,
                    author=author,
                    strategy=args.strategy,
                    max_passes=args.max_passes,
                    sleep_seconds=args.sleep_between_passes,
                )
                grand_passes += passes
                grand_deleted += deleted
                grand_added += added
            except Exception:
                # Each committed pass is durable, so one dataset's failure
                # doesn't undo prior progress on it or block the rest of the
                # batch - log and move on rather than aborting the whole run.
                log.exception("[%s] failed - skipping to next dataset", dataset_name)
                failed.append(dataset_name)
    except KeyboardInterrupt:
        log.warning(
            "interrupted - all completed passes are already committed; "
            "rerun the same command to resume"
        )
        sys.exit(130)

    elapsed = time.monotonic() - run_started
    log.info(
        "ALL DONE in %.1fs: %d dataset(s), %d pass(es) total, "
        "%d files deleted, %d files added",
        elapsed, len(args.dataset), grand_passes, grand_deleted, grand_added,
    )
    if failed:
        log.error("failed dataset(s): %s", ", ".join(failed))
        sys.exit(1)


if __name__ == "__main__":
    main()
