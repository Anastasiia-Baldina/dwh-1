#!/usr/bin/env python3
from __future__ import annotations

import csv
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

import psycopg2
from psycopg2 import sql


@dataclass(frozen=True)
class ConnCfg:
    host: str
    port: int
    user: str
    password: str


@dataclass(frozen=True)
class LoadSpec:
    db: str
    table: str
    csv_path: Path
    exclude_cols: tuple[str, ...] = ()


SERIAL_ID_COLUMNS = {
    "user_id",
    "address_id",
    "history_id",
    "order_id",
    "order_item_id",
    "product_id",
    "warehouse_id",
    "pickup_point_id",
    "shipment_id",
    "movement_id",
}


def env_conn() -> ConnCfg:
    return ConnCfg(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"),
    )


def connect(cfg: ConnCfg, db: str):
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        dbname=db,
    )


def wait_connect(cfg: ConnCfg, db: str, retries: int = 60, delay_s: float = 1.0) -> None:
    for i in range(retries):
        try:
            with connect(cfg, db) as conn:
                with conn.cursor() as cur:
                    cur.execute("select 1;")
            return
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(delay_s)


def ensure_databases(cfg: ConnCfg) -> None:
    dbs = ["user_service_db", "order_service_db", "logistics_service_db"]
    with connect(cfg, "postgres") as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for db in dbs:
                cur.execute("select 1 from pg_database where datname=%s;", (db,))
                if cur.fetchone():
                    print(f"[OK] DB exists: {db}")
                else:
                    print(f"[RUN] Creating DB: {db}")
                    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db)))


def read_csv_header(csv_path: Path) -> list[str]:
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            raise ValueError(f"CSV has no header: {csv_path}")
        # trim spaces
        return [h.strip() for h in header if h and h.strip()]


def filter_columns(header_cols: Sequence[str], exclude: Sequence[str]) -> list[str]:
    excl = set(exclude) | SERIAL_ID_COLUMNS
    cols = [c for c in header_cols if c not in excl]
    if not cols:
        raise ValueError("After exclusions, no columns left to load")
    return cols


def copy_csv(cfg: ConnCfg, spec: LoadSpec) -> bool:
    if not spec.csv_path.exists():
        print(f"[SKIP] {spec.db}.{spec.table} (no file: {spec.csv_path})")
        return False

    try:
        header_cols = read_csv_header(spec.csv_path)
        cols = filter_columns(header_cols, spec.exclude_cols)

        schema, tbl = spec.table.split(".", 1)
        cols_sql = sql.SQL(", ").join(map(sql.Identifier, cols))
        copy_stmt = sql.SQL(
            "COPY {}.{} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)"
        ).format(sql.Identifier(schema), sql.Identifier(tbl), cols_sql)

        print(f"[LOAD] {spec.db}.{spec.table} <= {spec.csv_path.name} | columns={len(cols)}")

        with connect(cfg, spec.db) as conn:
            with conn.cursor() as cur:
                with spec.csv_path.open("r", encoding="utf-8") as f:
                    cur.copy_expert(copy_stmt, f)
            conn.commit()

        print(f"[OK]   {spec.db}.{spec.table}")
        return True

    except Exception as e:
        print(f"[ERR]  {spec.db}.{spec.table} failed: {type(e).__name__}: {e}")

        if hasattr(e, "diag") and e.diag is not None:
            diag = e.diag
            detail = getattr(diag, "message_detail", None)
            constraint = getattr(diag, "constraint_name", None)
            if constraint:
                print(f"       constraint: {constraint}")
            if detail:
                print(f"       detail: {detail}")

        return False


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def main(argv: list[str]) -> int:
    cfg = env_conn()
    root = project_root()
    csv_dir = Path(os.getenv("CSV_DIR", str(root / "mock_data")))

    print(f"==> Waiting DB at {cfg.host}:{cfg.port} as {cfg.user}")
    wait_connect(cfg, "postgres")

    ensure_databases(cfg)

    specs: list[LoadSpec] = [
        # user_service_db
        LoadSpec("user_service_db", "public.users", csv_dir / "user_service_users.csv"),
        LoadSpec("user_service_db", "public.user_addresses", csv_dir / "user_service_user_addresses.csv"),
        LoadSpec("user_service_db", "public.user_status_history", csv_dir / "user_service_user_status_history.csv"),

        # order_service_db
        LoadSpec("order_service_db", "public.products", csv_dir / "order_service_products.csv"),
        LoadSpec("order_service_db", "public.orders", csv_dir / "order_service_orders.csv"),
        LoadSpec("order_service_db", "public.order_items", csv_dir / "order_service_order_items.csv"),
        LoadSpec("order_service_db", "public.order_status_history", csv_dir / "order_service_order_status_history.csv"),

        # logistics_service_db
        LoadSpec("logistics_service_db", "public.warehouses", csv_dir / "logistics_service_warehouses.csv"),
        LoadSpec("logistics_service_db", "public.pickup_points", csv_dir / "logistics_service_pickup_points.csv"),
        LoadSpec("logistics_service_db", "public.shipments", csv_dir / "logistics_service_shipments.csv"),
        LoadSpec("logistics_service_db", "public.shipment_movements", csv_dir / "logistics_service_shipment_movements.csv"),
        LoadSpec("logistics_service_db", "public.shipment_status_history", csv_dir / "logistics_service_shipment_status_history.csv"),
    ]

    ok = failed = skipped = 0
    for spec in specs:
        if not spec.csv_path.exists():
            skipped += 1
            print(f"[SKIP] {spec.db}.{spec.table} (no file)")
            continue
        if copy_csv(cfg, spec):
            ok += 1
        else:
            failed += 1

    print("==> Summary")
    print(f"    ok:      {ok}")
    print(f"    failed:  {failed}")
    print(f"    skipped: {skipped}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))