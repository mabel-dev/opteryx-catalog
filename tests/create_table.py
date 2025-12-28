import os
import sys

# Add local paths to sys.path to use local code instead of installed packages
sys.path.insert(0, os.path.join(sys.path[0], ".."))  # Add parent dir for pyiceberg_firestore_gcs
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-core"))

try:
    import opteryx

    print("Opteryx version:", opteryx.__version__)

    from pyiceberg_firestore_gcs import FirestoreCatalog
    from opteryx.connectors.iceberg_connector import IcebergConnector

    workspace = "opteryx"
    schema_name = "ops"
    table = "audit_log"

    opteryx.set_default_connector(
        IcebergConnector,
        catalog=FirestoreCatalog,
        firestore_project=os.environ["GCP_PROJECT_ID"],
        firestore_database=os.environ["FIRESTORE_DATABASE"],
        gcs_bucket=os.environ["GCS_BUCKET"],
    )
    _use_opteryx = True
except Exception as _e:
    # Fallback: run directly against FirestoreCatalog when opteryx-core isn't available
    print(
        "Opteryx unavailable or failed to initialize, falling back to FirestoreCatalog directly:",
        _e,
    )
    from pyiceberg_firestore_gcs import FirestoreCatalog

    workspace = os.environ.get("OPTERYX_WORKSPACE", "opteryx")
    schema_name = os.environ.get("OPTERYX_SCHEMA", "ops")
    table = os.environ.get("OPTERYX_TABLE", "audit_log")

    _use_opteryx = False


import cProfile
import pstats


with cProfile.Profile(subcalls=False) as pr:
    if _use_opteryx:
        # Use opteryx path when available
        df = opteryx.query_to_arrow("SELECT count(*) FROM personal.bastian.space_missions")
        print(df)
    else:
        # Fallback: operate directly on FirestoreCatalog to exercise manifest reads
        catalog = FirestoreCatalog(
            catalog_name=workspace,
            firestore_project=os.environ.get("GCP_PROJECT_ID"),
            firestore_database=os.environ.get("FIRESTORE_DATABASE"),
            gcs_bucket=os.environ.get("GCS_BUCKET"),
        )
        try:
            tbl = catalog.load_table((schema_name, table))
            print("Loaded table:", tbl.identifier)
        except Exception as e:
            print("Failed to load table via FirestoreCatalog:", e)

    stats = pstats.Stats(pr).sort_stats("tottime")

    func_list = [
        (k, v)
        for k, v in stats.stats.items()
        if "~" not in k[0] and "aiohttp" not in k[0] and "asyncio" not in k[0]
    ]
    sorted_funcs = sorted(func_list, key=lambda x: x[1][2], reverse=True)

    header = ["Line", "Function", "Calls", "Total (ms)", "Cumulative (ms)"]
    divider = "-" * 110
    print(divider)
    print("{:<45} {:<20} {:>10} {:>12} {:>17}".format(*header))
    print(divider)

    limit = 25
    for func, func_stats in sorted_funcs:
        file_name, line_number, function_name = func
        total_calls, _, total_time, cumulative_time, _ = func_stats
        file_name = file_name.split("opteryx")[-1]
        file_name = "..." + file_name[-37:] if len(file_name) > 40 else file_name
        function_name = function_name[:17] + "..." if len(function_name) > 20 else function_name
        row = [
            f"{file_name}:{line_number}",
            function_name,
            total_calls,
            f"{(total_time * 1000):.6f}",
            f"{(cumulative_time * 1000):.4f}",
        ]
        print("{:<42} {:<20} {:>10} {:>12} {:>17}".format(*row))
        limit -= 1
        if limit == 0:
            break
    print(divider)
