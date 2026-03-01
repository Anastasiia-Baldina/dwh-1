#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2 import sql


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def with_timestamp_prefix(path: Path) -> Path:
    if path.name == "":
        return path
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return path.with_name(f"{ts}_{path.name}")


def main(argv: list[str]) -> int:
    # Usage:
    #   python scripts/csv_report.py public.v_cohort_mart ./out/cohort.csv
    view_name = argv[1] if len(argv) > 1 else env("VIEW_NAME", "public.v_cohort_mart")

    out_arg = argv[2] if len(argv) > 2 else env("OUT_CSV", "")
    if out_arg:
        out_path = with_timestamp_prefix(Path(out_arg))
    else:
        out_path = with_timestamp_prefix(Path("./report/view_export.csv"))

    host = env("PGHOST", "localhost")
    port = int(env("PGPORT", "5432"))
    user = env("PGUSER", "postgres")
    password = env("PGPASSWORD", "postgres")
    dbname = env("PGDATABASE", "order_service_db")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # schema.view
    if "." in view_name:
        schema, view = view_name.split(".", 1)
    else:
        schema, view = "public", view_name

    conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
    try:
        with conn.cursor() as cur, out_path.open("w", encoding="utf-8", newline="") as f:
            q = sql.SQL(
                "COPY (SELECT * FROM {}.{}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE)"
            ).format(sql.Identifier(schema), sql.Identifier(view))
            cur.copy_expert(q, f)

        print(f"Exported {schema}.{view} from db={dbname} to: {out_path}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))