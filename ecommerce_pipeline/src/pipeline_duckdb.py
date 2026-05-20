from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app_logging import configure_logging, get_logger

DATA_DIR = PROJECT_ROOT / "data" / "raw"
SQL_DIMS_FILE = PROJECT_ROOT / "sql" / "transform" / "dims.sql"
SQL_FACTS_FILE = PROJECT_ROOT / "sql" / "transform" / "facts.sql"
SQL_VIEWS_FILE = PROJECT_ROOT / "sql" / "transform" / "views.sql"
DB_PATH = PROJECT_ROOT / "ecommerce.duckdb"

CSV_FILES = {
    "users": "users.csv",
    "orders": "orders.csv",
    "order_items": "order_items.csv",
    "products": "products.csv",
    "distribution_centers": "distribution_centers.csv",
    "inventory_items": "inventory_items.csv",
    "events": "events.csv",
}

logger = get_logger("pipeline.duckdb")


def assert_inputs() -> None:
    missing = [name for name in CSV_FILES.values() if not (DATA_DIR / name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing CSV files in data/raw: {', '.join(missing)}")
    logger.info("input_files_verified", extra={"context": {"count": len(CSV_FILES)}})


def load_staging(con: duckdb.DuckDBPyConnection) -> None:
    logger.info("staging_schema_prepare_start")
    con.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    con.execute("CREATE SCHEMA IF NOT EXISTS mart;")

    for table_name, file_name in CSV_FILES.items():
        path = (DATA_DIR / file_name).as_posix()
        logger.info("staging_table_load_start", extra={"context": {"table": table_name, "file": file_name}})
        con.execute(
            f"""
            CREATE OR REPLACE TABLE staging.{table_name}_raw AS
            SELECT *
            FROM read_csv_auto('{path}', header=true, sample_size=-1);
            """
        )
        row_count = con.execute(f"SELECT COUNT(*) FROM staging.{table_name}_raw").fetchone()[0]
        logger.info("staging_table_load_done", extra={"context": {"table": table_name, "rows": int(row_count)}})


def load_single_table(con: duckdb.DuckDBPyConnection, table_name: str) -> None:
    if table_name not in CSV_FILES:
        raise ValueError(f"Unsupported table name: {table_name}")
    con.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    con.execute("CREATE SCHEMA IF NOT EXISTS mart;")
    file_name = CSV_FILES[table_name]
    path = (DATA_DIR / file_name).as_posix()
    logger.info("staging_table_load_start", extra={"context": {"table": table_name, "file": file_name}})
    con.execute(
        f"""
        CREATE OR REPLACE TABLE staging.{table_name}_raw AS
        SELECT *
        FROM read_csv_auto('{path}', header=true, sample_size=-1);
        """
    )
    row_count = con.execute(f"SELECT COUNT(*) FROM staging.{table_name}_raw").fetchone()[0]
    logger.info("staging_table_load_done", extra={"context": {"table": table_name, "rows": int(row_count)}})


def transform_dims(con: duckdb.DuckDBPyConnection) -> None:
    logger.info("transform_dims_start", extra={"context": {"sql_file": str(SQL_DIMS_FILE)}})
    con.execute(SQL_DIMS_FILE.read_text(encoding="utf-8"))
    logger.info("transform_dims_done")


def transform_facts(con: duckdb.DuckDBPyConnection) -> None:
    logger.info("transform_facts_start", extra={"context": {"sql_file": str(SQL_FACTS_FILE)}})
    con.execute(SQL_FACTS_FILE.read_text(encoding="utf-8"))
    logger.info("transform_facts_done")


def transform_views(con: duckdb.DuckDBPyConnection) -> None:
    logger.info("transform_views_start", extra={"context": {"sql_file": str(SQL_VIEWS_FILE)}})
    con.execute(SQL_VIEWS_FILE.read_text(encoding="utf-8"))
    logger.info("transform_views_done")


def run(db_path: Path, step: str = "all", table: str | None = None) -> None:
    logger.info("pipeline_run_start", extra={"context": {"db_path": str(db_path), "step": step, "table": table}})
    if step in ("all", "load", "load_table"):
        assert_inputs()
        if step == "load_table" and table is None:
            raise ValueError("--table is required when --step load_table")
    con = duckdb.connect(str(db_path))
    try:
        if step in ("all", "load"):
            load_staging(con)
        if step == "load_table":
            load_single_table(con, table_name=table)
        if step == "all":
            transform_dims(con)
            transform_facts(con)
            transform_views(con)
        if step == "transform":
            transform_dims(con)
            transform_facts(con)
            transform_views(con)
        if step == "transform_dims":
            transform_dims(con)
        if step == "transform_facts":
            transform_facts(con)
        if step == "transform_views":
            transform_views(con)
    finally:
        con.close()
    logger.info("pipeline_run_done", extra={"context": {"db_path": str(db_path), "step": step, "table": table}})


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description="E-Commerce DuckDB ELT Pipeline")
    parser.add_argument("--db-path", default=str(DB_PATH), help="DuckDB file path")
    parser.add_argument(
        "--step",
        choices=["all", "load", "load_table", "transform", "transform_dims", "transform_facts", "transform_views"],
        default="all",
        help="Step to run",
    )
    parser.add_argument("--table", choices=list(CSV_FILES.keys()), default=None, help="Table name for --step load_table")
    args = parser.parse_args()

    run(Path(args.db_path), args.step, args.table)


if __name__ == "__main__":
    main()
