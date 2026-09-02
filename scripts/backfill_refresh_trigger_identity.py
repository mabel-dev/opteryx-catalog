#!/usr/bin/env python3
"""Move a materialized view's `runs-as` off the view and onto its refresh triggers.

Usage:
    python scripts/backfill_refresh_trigger_identity.py                     # dry run
    python scripts/backfill_refresh_trigger_identity.py --apply             # pin the triggers
    python scripts/backfill_refresh_trigger_identity.py --apply --retire    # ...then drop the view field
    python scripts/backfill_refresh_trigger_identity.py <workspace> ... [--apply] [--retire]

WHY THIS EXISTS. A trigger is an event plus a `runs-as`: the event has no
identity of its own, so the trigger carries the one the work it starts will
run as. Tasks always worked that way. A materialized view's refresh did not -
the identity sat on the VIEW document and its refresh triggers carried none -
and `_fire_refresh` now reads the trigger, never the view. A view registered
under the old model therefore has triggers that refuse to fire (`owner-missing`
on the trigger, an alert in the stream) until this has run.

TWO PHASES, so the catalog library and the services that read it can roll in
any order:

  1. `--apply` COPIES the view's `runs-as` onto every refresh trigger of that
     view that lacks one. Safe under old code (which reads the view and ignores
     the trigger) and new code (the reverse) alike; the view keeps its field.
  2. `--retire` (with `--apply`) REMOVES the view-level field, only from a view
     whose every refresh trigger now carries an identity. Run this once nothing
     still reads the view's field - old `trigger_firing.py`, and jobs.opteryx
     before its own change - or those readers refuse with `owner-missing`.

DERIVED, NEVER DECLARED. The value written onto a trigger is the one the view
already held, so a run changes no identity: every refresh runs as exactly who
it ran as before. That is what makes it safe to re-run - it is idempotent, and
a trigger that already agrees is left alone.

WHAT IT REFUSES TO TOUCH:

- A trigger whose `runs-as` DISAGREES with the view's. Somebody moved that
  trigger on its own (ALTER TRIGGER ... OWNER TO), or the view was edited by a
  different author after new code pinned its triggers. Which answer is right is
  a question about intent that no record answers, so the trigger is left as it
  is, the view is reported, and its field is not retired. Reconcile by hand:
  `ALTER MATERIALIZED VIEW <view> OWNER TO <principal>` repoints every refresh
  trigger of the view at once.
- A source with NO refresh trigger for the view. There is nothing to pin, and
  the view cannot refresh from that source anyway - the integrity audit
  (`integrity.py`, `missing-source-trigger`) is where that is reported. The
  view's field is not retired while a source is in that state.
- A view with no `runs-as` at all. Nothing to copy; its triggers either carry
  their own already or are damaged, and `owner-missing` on the trigger says
  which. Reported so the count is visible, and otherwise left alone.

Dry run by DEFAULT. `--apply` is the only thing that writes. Exits 1 if any
view was skipped for one of the reasons above (whether or not it wrote), 0
when everything was either pinned or already correct.

Companion to `opteryx_catalog/integrity.py`, whose `platform-identity-owner`
finding now covers refresh triggers too: expect it clean after this has run.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from opteryx_catalog.opteryx_catalog import MATERIALIZED_VIEW_TYPE
from opteryx_catalog.opteryx_catalog import MV_REFRESH_TRIGGER_KIND
from opteryx_catalog.opteryx_catalog import TRIGGERS_SUBCOLLECTION

DATASETS_SUBCOLLECTION = "datasets"


def _qualified(reference) -> str:
    """The qualified name of a dataset document.

    Path is `{workspace}/{collection}/datasets/{dataset}`.
    """
    parts = reference.path.split("/")
    return f"{parts[0]}.{parts[1]}.{parts[3]}"


def _dataset_ref(client, qualified: str):
    """The dataset document a qualified `workspace.collection.dataset` names.

    Split left-anchored with maxsplit, the same way `_split_qualified` does it:
    workspace and collection names may not contain dots, so a dataset name that
    does stays whole.
    """
    parts = qualified.split(".", 2)
    if len(parts) != 3:
        return None
    workspace, collection, dataset = parts
    return (
        client.collection(workspace)
        .document(collection)
        .collection(DATASETS_SUBCOLLECTION)
        .document(dataset)
    )


def collect(client, workspaces=None) -> list[dict]:
    """Every materialized view in the catalog, with the identity it holds.

    One collection-group stream over dataset documents, filtered client-side:
    a `where` on a collection group needs an index this database does not
    keep, and a one-off migration is not the reason to add one.
    """
    views = []
    for doc in client.collection_group(DATASETS_SUBCOLLECTION).stream():
        data = doc.to_dict() or {}
        if data.get("dataset-type") != MATERIALIZED_VIEW_TYPE:
            continue
        qualified = _qualified(doc.reference)
        if workspaces and qualified.split(".", 1)[0] not in workspaces:
            continue
        views.append(
            {
                "view": qualified,
                "reference": doc.reference,
                "runs_as": data.get("runs-as"),
                "sources": list(data.get("source-tables") or []),
            }
        )
    return sorted(views, key=lambda v: v["view"])


def _refresh_triggers_for(client, view: str, source: str) -> list[dict]:
    """The refresh triggers on `source` that target `view`.

    Matched by TARGET, not by name, the way the integrity audit matches them:
    the generated name has changed shape before, and a trigger written under
    the old scheme fires exactly as one written under the new.
    """
    source_ref = _dataset_ref(client, source)
    if source_ref is None or not source_ref.get().exists:
        return []
    found = []
    for doc in source_ref.collection(TRIGGERS_SUBCOLLECTION).stream():
        trigger = doc.to_dict() or {}
        if trigger.get("kind") != MV_REFRESH_TRIGGER_KIND:
            continue
        if trigger.get("target-view") != view:
            continue
        found.append(
            {"name": doc.id, "reference": doc.reference, "runs_as": trigger.get("runs-as")}
        )
    return found


def plan(client, views: list[dict]) -> list[dict]:
    """What each view needs, decided from the records as they stand.

    Reads only. Each write in `--apply` re-reads inside a transaction and makes
    the same decision there, so a plan that went stale between the two is
    refused rather than applied against changed records.

    One entry per view: `pin` lists the triggers to write, `problems` the
    reasons the view's field must not be retired, and `retirable` whether it
    may be once the pins have landed.
    """
    actions = []
    for entry in views:
        view, owner = entry["view"], entry["runs_as"]
        pin, current, problems = [], [], []
        for source in entry["sources"]:
            triggers = _refresh_triggers_for(client, view, source)
            if not triggers:
                problems.append(f"source {source} has no refresh trigger targeting the view")
                continue
            for trigger in triggers:
                held = trigger["runs_as"]
                if not held:
                    if owner:
                        pin.append({"source": source, **trigger})
                    else:
                        problems.append(
                            f"{trigger['name']} ON {source} has no runs-as and the view "
                            "holds none to copy"
                        )
                elif owner and held != owner:
                    problems.append(
                        f"{trigger['name']} ON {source} runs as {held}, the view says {owner}"
                    )
                else:
                    current.append({"source": source, **trigger})
        if not entry["sources"]:
            problems.append("the view records no sources")
        actions.append(
            {
                "view": view,
                "reference": entry["reference"],
                "owner": owner,
                "pin": pin,
                "current": current,
                "problems": problems,
                # Only a view that HOLDS a field can have it retired, and only
                # when nothing about its triggers is in question.
                "retirable": bool(owner) and not problems,
            }
        )
    return actions


def pin(client, trigger_reference, owner: str) -> str | None:
    """Write one trigger's `runs-as`, conditional on it still being empty.

    A transaction rather than a bare update because the decision was made from
    a read taken earlier: `set_trigger_owner` or a re-registration may have
    pinned the trigger since, and overwriting THAT would be the silent
    transfer this whole exercise exists to avoid.
    """
    from google.cloud import firestore

    @firestore.transactional
    def _pin(transaction) -> str | None:
        doc = trigger_reference.get(transaction=transaction)
        if not doc.exists:
            return "the trigger was dropped since the plan was made"
        held = (doc.to_dict() or {}).get("runs-as")
        if held:
            return None if held == owner else f"pinned to {held} since the plan was made"
        transaction.update(trigger_reference, {"runs-as": owner})
        return None

    return _pin(client.transaction())


def retire(client, view_reference, owner: str) -> str | None:
    """Remove the view-level field, conditional on it still saying `owner`.

    The triggers were verified in this run and every pin landed before this
    is reached; the transaction guards the one record it writes.
    """
    from google.cloud import firestore

    @firestore.transactional
    def _retire(transaction) -> str | None:
        doc = view_reference.get(transaction=transaction)
        if not doc.exists:
            return "the view was dropped since the plan was made"
        held = (doc.to_dict() or {}).get("runs-as")
        if held != owner:
            return f"the view's runs-as changed to {held!r} since the plan was made"
        transaction.update(view_reference, {"runs-as": firestore.DELETE_FIELD})
        return None

    return _retire(client.transaction())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "workspaces", nargs="*", help="limit the backfill to these workspaces (default: all)"
    )
    parser.add_argument("--apply", action="store_true", help="pin the triggers (default: dry run)")
    parser.add_argument(
        "--retire",
        action="store_true",
        help="with --apply: remove the view-level field from every view whose "
        "refresh triggers all carry an identity",
    )
    parser.add_argument("--project", default="mabeldev")
    parser.add_argument("--database", default="catalogs")
    args = parser.parse_args()
    if args.retire and not args.apply:
        parser.error("--retire writes, so it needs --apply")

    from google.cloud import firestore

    client = firestore.Client(project=args.project, database=args.database)

    views = collect(client, set(args.workspaces) or None)
    actions = plan(client, views)

    pinned = current = skipped = retired = 0
    for action in actions:
        view, owner = action["view"], action["owner"]
        current += len(action["current"])
        for problem in action["problems"]:
            print(f"[SKIPPED] {view}: {problem}")
        if action["problems"]:
            skipped += 1

        landed = True
        for trigger in action["pin"]:
            where = f"{trigger['name']} ON {trigger['source']}"
            if not args.apply:
                pinned += 1
                print(f"[would]   {view}: pin {where} -> {owner}")
                continue
            refused = pin(client, trigger["reference"], owner)
            if refused:
                landed = False
                skipped += 1
                print(f"[SKIPPED] {view}: {where}: {refused}")
            else:
                pinned += 1
                print(f"[pinned]  {view}: {where} -> {owner}")

        if not owner:
            if not action["problems"]:
                print(f"[ok]      {view}: no view-level runs-as; triggers carry their own")
            continue
        if not action["retirable"] or not landed:
            print(f"[held]    {view}: view-level runs-as kept ({owner})")
        elif not args.retire:
            print(f"[ready]   {view}: view-level runs-as ({owner}) can be retired with --retire")
        else:
            refused = retire(client, action["reference"], owner)
            if refused:
                skipped += 1
                print(f"[SKIPPED] {view}: {refused}")
            else:
                retired += 1
                print(f"[retired] {view}: view-level runs-as removed")

    verb = "pinned" if args.apply else "to pin"
    print(
        f"{pinned} triggers {verb}, {current} already correct, {retired} views retired, "
        f"{skipped} skipped"
    )
    if not args.apply and pinned:
        print("dry run - nothing was written; re-run with --apply")
    return 1 if skipped else 0


if __name__ == "__main__":
    raise SystemExit(main())
