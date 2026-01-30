"""Inspect a single file by scanning manifests in GCS and recomputing stats for that file only.

Usage:
  python scripts/inspect_single_file_gcs.py <bucket> <manifest_prefix> <target_gs_path> <output_jsonl>

Example:
  python scripts/inspect_single_file_gcs.py opteryx_data opteryx/ops/audit_log/metadata/ gs://opteryx_data/opteryx/ops/audit_log/data/188f... artifacts/out.jsonl
"""
import json
import sys
import os
import time
import pyarrow.parquet as pq
from google.cloud import storage
from opteryx_catalog.catalog.manifest import build_parquet_manifest_entry


def _preview(lst, n=6):
    if lst is None:
        return None
    if isinstance(lst, list):
        if len(lst) > n:
            return {"len": len(lst), "preview": lst[:n], "truncated": True}
        return {"len": len(lst), "preview": lst, "truncated": False}
    return lst


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python scripts/inspect_single_file_gcs.py <bucket> <manifest_prefix> <target_gs_path> <output_jsonl>")
        sys.exit(2)

    bucket_name = sys.argv[1]
    manifest_prefix = sys.argv[2]
    target_fp = sys.argv[3]
    out_path = sys.argv[4]

    repo_root = os.getcwd()
    artifacts = os.path.join(repo_root, "artifacts")
    os.makedirs(artifacts, exist_ok=True)
    if not os.path.abspath(out_path).startswith(repo_root):
        out_path = os.path.join(artifacts, os.path.basename(out_path))

    client = storage.Client()

    blobs = list(client.list_blobs(bucket_name, prefix=manifest_prefix))
    if not blobs:
        print("No manifest blobs found", file=sys.stderr)
        sys.exit(3)

    blobs.sort(key=lambda b: b.updated or b.time_created or 0, reverse=True)

    match_row = None
    match_idx = None
    match_manifest = None

    for b in blobs:
        data = b.download_as_bytes()
        try:
            table = pq.read_table(data)
            rows = table.to_pylist()
        except Exception:
            continue
        for i, r in enumerate(rows):
            if r.get("file_path") == target_fp:
                match_row = r
                match_idx = i
                match_manifest = b
                break
        if match_row:
            break

    if match_row is None:
        print("Target file not found in manifests", file=sys.stderr)
        sys.exit(4)

    # download target file
    _, rest = target_fp.split("://", 1)
    bucket2, path = rest.split("/", 1)
    blob2 = client.bucket(bucket2).blob(path)
    data = blob2.download_as_bytes()

    table = pq.read_table(data)
    recomputed = build_parquet_manifest_entry(table, target_fp, len(data)).to_dict()

    rec = {"file_index": match_idx, "file_path": target_fp}
    ent = match_row
    rec["manifest_entry_summary"] = {
        "uncompressed_size": int(ent.get("uncompressed_size_in_bytes") or 0),
        "min_k_hashes": _preview(ent.get("min_k_hashes")),
    }
    rec["recomputed"] = {
        "uncompressed_size": int(recomputed.get("uncompressed_size_in_bytes") or 0),
        "min_k_hashes": _preview(recomputed.get("min_k_hashes")),
    }
    man_k = ent.get("min_k_hashes") or []
    rec_k = recomputed.get("min_k_hashes") or []
    rec["diffs"] = {"min_k_hashes_nonempty_counts": {"manifest_nonempty": sum(1 for x in man_k if x), "recomputed_nonempty": sum(1 for x in rec_k if x)}}

    with open(out_path, "w", encoding="utf-8") as of:
        of.write(json.dumps({"_meta": {"source": "gcs-manifest-scan", "manifest_blob": match_manifest.name, "timestamp": int(time.time() * 1000)}}) + "\n")
        of.write(json.dumps(rec) + "\n")

    print("WROTE", out_path)
