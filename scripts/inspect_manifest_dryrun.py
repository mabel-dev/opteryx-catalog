"""Dry-run manifest inspector

Usage:
  python scripts/inspect_manifest_dryrun.py <dataset_identifier> <output_jsonl_path>

Example:
  python scripts/inspect_manifest_dryrun.py opteryx.ops.audit_log /tmp/audit_log_manifest_dryrun.jsonl

This script is non-mutating: it reads the manifest and referenced data files (when readable),
recomputes per-file statistics via `build_parquet_manifest_entry`, and writes per-file results
and a dataset-level summary to the output JSON-lines file.
"""

import json
import sys
import time
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from opteryx_catalog.opteryx_catalog import OpteryxCatalog
from opteryx_catalog.catalog.manifest import build_parquet_manifest_entry


def safe_read_manifest(ds) -> list:
    snap = ds.snapshot()
    if snap is None or not getattr(snap, "manifest_list", None):
        raise ValueError("No manifest available for this dataset/snapshot")
    manifest_path = snap.manifest_list
    try:
        inp = ds.io.new_input(manifest_path)
        with inp.open() as f:
            data = f.read()
        if not data:
            return []
        table = pq.read_table(pa.BufferReader(data))
        rows = table.to_pylist()
        return rows
    except Exception as e:
        return [{"__error": f"failed to read manifest: {e}"}]


def inspect_dataset(dataset_identifier: str, output_path: str, catalog_kwargs: dict):
    out_file = open(output_path, "w", encoding="utf-8")

    meta = {"dataset": dataset_identifier, "timestamp": int(time.time() * 1000)}
    out_file.write(json.dumps({"_meta": meta}) + "\n")

    # Allow dataset identifiers in forms:
    # - 'collection.dataset'
    # - 'workspace.collection.dataset' (fully-qualified)
    # If fully-qualified, override workspace from identifier.
    parts = dataset_identifier.split(".")
    if len(parts) == 3:
        wk, collection, dname = parts
        catalog_kwargs = dict(catalog_kwargs)
        catalog_kwargs["workspace"] = wk
        dataset_identifier = f"{collection}.{dname}"

    catalog = OpteryxCatalog(**catalog_kwargs)

    try:
        ds = catalog.load_dataset(dataset_identifier)
    except Exception as e:
        out_file.write(json.dumps({"error": f"failed to load dataset: {e}"}) + "\n")
        out_file.close()
        return

    # Describe before
    try:
        describe_before = ds.describe()
    except Exception as e:
        describe_before = {"__error": f"describe failed: {e}"}

    out_file.write(json.dumps({"describe_before": describe_before}) + "\n")

    # Read manifest rows
    rows = safe_read_manifest(ds)
    out_file.write(json.dumps({"manifest_rows_count": len(rows)}) + "\n")

    # Get schema mapping
    try:
        orso_schema = ds.schema()
        col_to_idx = {c.name: i for i, c in enumerate(orso_schema.columns)} if orso_schema else {}
    except Exception:
        col_to_idx = {}

    # Aggregators for recomputed per-column uncompressed bytes
    recomputed_col_bytes = {name: 0 for name in col_to_idx}

    for i, ent in enumerate(rows):
        rec = {"file_index": i}
        if not isinstance(ent, dict):
            rec["error"] = "manifest row not a dict"
            out_file.write(json.dumps(rec) + "\n")
            continue
        rec["file_path"] = ent.get("file_path")

        def _preview(lst, n=6):
            if lst is None:
                return None
            if isinstance(lst, list):
                if len(lst) > n:
                    return {"len": len(lst), "preview": lst[:n], "truncated": True}
                return {"len": len(lst), "preview": lst, "truncated": False}
            return lst

        rec["manifest_entry_summary"] = {
            "uncompressed_size": int(ent.get("uncompressed_size_in_bytes") or 0),
            "column_uncompressed_sizes": _preview(ent.get("column_uncompressed_sizes_in_bytes")),
            "null_counts": _preview(ent.get("null_counts")),
            "min_values": _preview(ent.get("min_values")),
            "max_values": _preview(ent.get("max_values")),
            "min_values_display": _preview(ent.get("min_values_display")),
            "max_values_display": _preview(ent.get("max_values_display")),
            "min_k_hashes": _preview(ent.get("min_k_hashes")),
            "histogram_counts_len": len(ent.get("histogram_counts") or []),
        }

        fp = ent.get("file_path")
        if not fp:
            rec["recomputed"] = {"error": "no file_path"}
            out_file.write(json.dumps(rec) + "\n")
            continue

        # Try to read the data file and recompute stats
        try:
            inp = ds.io.new_input(fp)
            with inp.open() as f:
                data = f.read()
            if not data:
                rec["recomputed"] = {"error": "empty file"}
                out_file.write(json.dumps(rec) + "\n")
                continue
            table = pq.read_table(pa.BufferReader(data))
            recomputed_entry = build_parquet_manifest_entry(table, fp, len(data)).to_dict()

            # Build a compact summary for output
            recomputed_summary = {
                "uncompressed_size": int(recomputed_entry.get("uncompressed_size_in_bytes") or 0),
                "column_uncompressed_sizes": _preview(recomputed_entry.get("column_uncompressed_sizes_in_bytes")),
                "null_counts": _preview(recomputed_entry.get("null_counts")),
                "min_values": _preview(recomputed_entry.get("min_values")),
                "max_values": _preview(recomputed_entry.get("max_values")),
                "min_values_display": _preview(recomputed_entry.get("min_values_display")),
                "max_values_display": _preview(recomputed_entry.get("max_values_display")),
                "min_k_hashes": _preview(recomputed_entry.get("min_k_hashes")),
                "histogram_counts_len": len(recomputed_entry.get("histogram_counts") or []),
            }

            rec["recomputed"] = recomputed_summary

            # Compare some fields safely and do per-column comparisons (sampled)
            diffs = {}
            try:
                manifest_us = int(ent.get("uncompressed_size_in_bytes") or 0)
                recomputed_us = recomputed_summary["uncompressed_size"]
                if manifest_us != recomputed_us:
                    diffs["uncompressed_size_mismatch"] = {"manifest": manifest_us, "recomputed": recomputed_us}
            except Exception:
                pass

            try:
                manifest_cols = len(ent.get("column_uncompressed_sizes_in_bytes") or [])
                recomputed_cols = (recomputed_entry.get("column_uncompressed_sizes_in_bytes") or [])
                if manifest_cols != len(recomputed_cols):
                    diffs["column_uncompressed_length_mismatch"] = {
                        "manifest_len": manifest_cols,
                        "recomputed_len": len(recomputed_cols),
                    }
            except Exception:
                pass

            # Per-column array comparisons: sample up to N mismatches
            def _cmp_lists(manifest_dict, recomputed_dict, field, max_samples=5):
                man_list = manifest_dict.get(field) or []
                rec_list = recomputed_dict.get(field) or []
                mismatches = []
                for idx in range(min(len(man_list), len(rec_list))):
                    if man_list[idx] != rec_list[idx]:
                        mismatches.append({"index": idx, "manifest": man_list[idx], "recomputed": rec_list[idx]})
                        if len(mismatches) >= max_samples:
                            break
                if mismatches or len(man_list) != len(rec_list):
                    return {
                        "mismatch_count": len(mismatches),
                        "sample_mismatches": mismatches,
                        "manifest_len": len(man_list),
                        "recomputed_len": len(rec_list),
                    }
                return None

            for field in ("null_counts", "min_values", "max_values", "min_values_display", "max_values_display"):
                cmp_res = _cmp_lists(ent, recomputed_entry, field)
                if cmp_res:
                    diffs[f"{field}_mismatch"] = cmp_res

            # Compare column uncompressed sizes (sample mismatches)
            try:
                man_cols = ent.get("column_uncompressed_sizes_in_bytes") or []
                rec_cols = recomputed_entry.get("column_uncompressed_sizes_in_bytes") or []
                col_mismatches = []
                for idx in range(min(len(man_cols), len(rec_cols))):
                    if int(man_cols[idx]) != int(rec_cols[idx]):
                        col_mismatches.append({"index": idx, "manifest": int(man_cols[idx]), "recomputed": int(rec_cols[idx])})
                        if len(col_mismatches) >= 5:
                            break
                if col_mismatches or len(man_cols) != len(rec_cols):
                    diffs["column_uncompressed_size_mismatch"] = {"count": len(col_mismatches), "sample": col_mismatches, "manifest_len": len(man_cols), "recomputed_len": len(rec_cols)}
            except Exception:
                pass

            rec["diffs"] = diffs

            # Accumulate recomputed per-column bytes by index -> by name when schema available
            col_sizes = recomputed_entry.get("column_uncompressed_sizes_in_bytes") or []
            for cname, cidx in col_to_idx.items():
                try:
                    val = int((col_sizes or [0])[cidx])
                except Exception:
                    val = 0
                recomputed_col_bytes[cname] = recomputed_col_bytes.get(cname, 0) + val

        except Exception as e:
            rec["recomputed"] = {"error": f"failed to read/recompute: {e}"}

        out_file.write(json.dumps(rec) + "\n")

    # Write recomputed per-column summary
    out_file.write(json.dumps({"recomputed_column_uncompressed_bytes": recomputed_col_bytes}) + "\n")

    # Attempt to build a recomputed describe-like summary for comparison
    recompute_describe = {}
    try:
        for cname in col_to_idx:
            recompute_describe[cname] = {"uncompressed_bytes": recomputed_col_bytes.get(cname, 0)}
    except Exception as e:
        recompute_describe = {"__error": str(e)}

    out_file.write(json.dumps({"describe_recomputed": recompute_describe}) + "\n")

    out_file.close()


if __name__ == "__main__":
    # Usage: second arg optional; if omitted or outside the repo we write into ./artifacts/
    if len(sys.argv) < 2:
        print("Usage: python scripts/inspect_manifest_dryrun.py <dataset_identifier> [output_jsonl_path]")
        sys.exit(2)
    dataset_identifier = sys.argv[1]
    out_arg = sys.argv[2] if len(sys.argv) >= 3 else ""
    # Use environment defaults; allow overriding via env or pass-through edits.
    import os
    import sys

    # Allow using local packages (same logic as tests) so we can exercise local OpteryxCatalog
    sys.path.insert(0, os.path.join(sys.path[0], ".."))  # parent dir (pyiceberg_firestore_gcs)
    sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-core"))
    sys.path.insert(1, os.path.join(sys.path[0], "../pyiceberg-firestore-gcs"))

    # Default to the same test-friendly defaults when env vars are unset
    catalog_kwargs = {
        "workspace": os.getenv("OPTERYX_WORKSPACE", "opteryx"),
        "firestore_project": os.getenv("OPTERYX_FIRESTORE_PROJECT", "mabeldev"),
        "firestore_database": os.getenv("OPTERYX_FIRESTORE_DATABASE", "catalogs"),
        "gcs_bucket": os.getenv("OPTERYX_GCS_BUCKET", "opteryx_data"),
    }

    # Always write into the repository's `artifacts/` directory unless an explicit repo-local path was provided
    repo_root = os.getcwd()
    artifacts_dir = os.path.join(repo_root, "artifacts")
    os.makedirs(artifacts_dir, exist_ok=True)

    if out_arg:
        candidate = os.path.abspath(out_arg)
        if candidate.startswith(repo_root):
            output_path = candidate
        else:
            output_path = os.path.join(artifacts_dir, f"{dataset_identifier.replace('.', '_')}_manifest_dryrun.jsonl")
            print(f"Provided output path {out_arg} is outside the repo; writing to {output_path}")
    else:
        output_path = os.path.join(artifacts_dir, f"{dataset_identifier.replace('.', '_')}_manifest_dryrun.jsonl")

    print(f"Using catalog workspace={catalog_kwargs['workspace']} firestore_project={catalog_kwargs['firestore_project']} gcs_bucket={catalog_kwargs['gcs_bucket']}")
    print(f"Writing results to {output_path}")
    inspect_dataset(dataset_identifier, output_path, catalog_kwargs)
