#!/usr/bin/env python3
"""Sweep workspaces for ghost datasets and orphaned/dangling triggers.

Usage:
    python scripts/find_catalog_orphans.py <workspace> [<workspace> ...]
    python scripts/find_catalog_orphans.py --all
    python scripts/find_catalog_orphans.py --all --json

Read-only: prints findings and exits 1 if any were found (0 when clean), so
it can run on a schedule and fail loudly. It deletes nothing - each finding
is a path and a reason, and the repair is a human decision.

See `opteryx_catalog.integrity` for what each finding kind means and why
`list_documents()` is the only read that can see a ghost.
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from opteryx_catalog.integrity import audit_workspace  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workspaces", nargs="*", help="workspaces to audit")
    parser.add_argument("--all", action="store_true", help="audit every workspace")
    parser.add_argument("--json", action="store_true", help="emit findings as JSON lines")
    parser.add_argument("--project", default="mabeldev")
    parser.add_argument("--database", default="catalogs")
    args = parser.parse_args()

    if not args.workspaces and not args.all:
        parser.error("name at least one workspace, or pass --all")

    from google.cloud import firestore

    client = firestore.Client(project=args.project, database=args.database)

    workspaces = args.workspaces
    if args.all:
        workspaces = sorted(col.id for col in client.collections())

    findings = []
    for workspace in workspaces:
        findings.extend(audit_workspace(client, workspace))

    if args.json:
        for finding in findings:
            print(json.dumps(finding))
    else:
        for finding in findings:
            print(f"[{finding['kind']}] {finding['path']}: {finding['detail']}")
        print(f"{len(findings)} finding(s) across {len(workspaces)} workspace(s)")

    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
