"""Bind a workspace to a PostgreSQL server (kind = "postgres"), by hand.

The control plane does not offer this kind yet (control.opteryx's
/v1/catalog-kinds descriptor is the customer-facing allowlist, and "postgres"
is deliberately absent until the connector has had far more testing). So a
Postgres-bound TEST workspace is created with this script, which writes the
same `catalog` block on the workspace's `$properties` document that the
control plane would, through the same library function, with the password
KMS-envelope-encrypted the same way.

What the worker does with it (worker.opteryx/app/catalog_resolver.py):
  kind "postgres" -> opteryx.connectors.PostgresConnector, no metastore.
  `config` becomes the connector's constructor keywords.
  The stored credential is decrypted and injected at `inject-as` = "password".

Usage (the password is read from PGBIND_PASSWORD, never from argv, so it does
not land in shell history or process listings):

    PGBIND_PASSWORD='...' python scripts/bind_postgres_workspace.py \\
        --project my-gcp-project --database catalogs \\
        --workspace erp_test --updated-by justin \\
        --kms-key projects/P/locations/L/keyRings/R/cryptoKeys/K \\
        --host db.example.com --port 5432 --dbname app --user reader \\
        [--sslmode require|verify-full|disable] [--schema public] \\
        [--timeout-s 30] [--preserve-sql-case]

Or supply the server in one value, password included:

    python scripts/bind_postgres_workspace.py \\
        --project my-gcp-project --database catalogs \\
        --workspace erp_test --updated-by justin --kms-key projects/... \\
        --connection-url 'postgresql://reader:pw@db.example.com:5432/app?sslmode=require'

    python scripts/bind_postgres_workspace.py ... --clear   # revert to native

Requires the `kms` extra (opteryx-catalog[kms]) for the credential envelope.
"""

import argparse
import os
import sys

from google.cloud import firestore

from opteryx_catalog.binding import clear_catalog_binding
from opteryx_catalog.binding import read_catalog_binding
from opteryx_catalog.binding import write_catalog_binding

SSLMODES = ("disable", "require", "verify-full")


def _parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project", required=True, help="GCP project of the catalogs Firestore")
    parser.add_argument("--database", required=True, help="Firestore database holding the workspaces")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--updated-by", required=True, help="who is recorded as the binding's author")
    parser.add_argument("--clear", action="store_true", help="remove the binding (workspace reverts to native)")
    parser.add_argument("--kms-key", help="KMS key resource name that wraps the password")
    parser.add_argument(
        "--connection-url",
        help="postgresql://user:password@host:port/dbname?sslmode=... — supplies host, port, "
        "dbname, user, sslmode and the password in one value, instead of the flags below. "
        "PGBIND_PASSWORD still wins if it is set.",
    )
    parser.add_argument("--host")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname")
    parser.add_argument("--user")
    parser.add_argument("--sslmode", default="require", choices=SSLMODES)
    parser.add_argument("--schema", default="public", help="schema used for <workspace>.<table> names")
    parser.add_argument("--timeout-s", type=int, default=30)
    parser.add_argument("--preserve-sql-case", action="store_true")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv)
    client = firestore.Client(project=args.project, database=args.database)

    if args.clear:
        cleared = clear_catalog_binding(client, args.workspace)
        print(f"{args.workspace}: binding {'cleared' if cleared else 'was not set'}")
        return 0

    password = os.environ.get("PGBIND_PASSWORD")
    if args.connection_url:
        import urllib.parse

        url = urllib.parse.urlsplit(args.connection_url)
        if url.scheme not in ("postgresql", "postgres"):
            print(f"--connection-url must be a postgresql:// URL (got {url.scheme!r}://)", file=sys.stderr)
            return 2
        options = dict(urllib.parse.parse_qsl(url.query))
        args.host = args.host or url.hostname
        args.port = url.port or args.port
        args.dbname = args.dbname or url.path.lstrip("/")
        args.user = args.user or url.username
        if "sslmode" in options:
            if options["sslmode"] not in SSLMODES:
                print(f"unsupported sslmode {options['sslmode']!r}; expected one of {', '.join(SSLMODES)}", file=sys.stderr)
                return 2
            args.sslmode = options["sslmode"]
        # The URL may carry the password; the environment still takes precedence
        # so a URL kept in a file need not hold the live credential.
        if not password and url.password:
            password = urllib.parse.unquote(url.password)

    missing = [name for name in ("host", "dbname", "user", "kms_key") if not getattr(args, name)]
    if missing:
        print(f"missing required option(s): {', '.join('--' + m.replace('_', '-') for m in missing)}", file=sys.stderr)
        return 2
    if not password:
        print(
            "no password: set PGBIND_PASSWORD, or include one in --connection-url",
            file=sys.stderr,
        )
        return 2

    from opteryx_catalog.security.kms import encrypt_secret  # optional extra; import late

    existing = read_catalog_binding(client, args.workspace)
    if existing is not None and existing.kind != "postgres":
        print(
            f"{args.workspace} is already bound to kind '{existing.kind}'; a workspace's "
            "storage type is fixed for its lifetime — refusing to rebind",
            file=sys.stderr,
        )
        return 1

    config = {
        "host": args.host,
        "port": args.port,
        "dbname": args.dbname,
        "user": args.user,
        "sslmode": args.sslmode,
        "schema": args.schema,
        "timeout_s": args.timeout_s,
    }
    ciphertext = encrypt_secret(password, args.kms_key)
    version = write_catalog_binding(
        client,
        args.workspace,
        kind="postgres",
        config=config,
        auth_mode="stored",
        ciphertext=ciphertext,
        kms_key=args.kms_key,
        inject_as="password",
        preserve_sql_case=args.preserve_sql_case,
        updated_by=args.updated_by,
    )
    print(f"{args.workspace}: bound to postgres {args.user}@{args.host}:{args.port}/{args.dbname} (version {version})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
