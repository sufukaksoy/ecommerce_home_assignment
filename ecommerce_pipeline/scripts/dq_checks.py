from __future__ import annotations

import sys
from pathlib import Path

import duckdb

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app_logging import configure_logging, get_logger
DB_PATH = BASE_DIR / "ecommerce.duckdb"
logger = get_logger("dq.checks")


def main() -> None:
    configure_logging()
    logger.info("dq_checks_start", extra={"context": {"db_path": str(DB_PATH)}})

    con = duckdb.connect(str(DB_PATH))
    try:
        checks = {
            "fact_orders_not_empty": "SELECT COUNT(*) > 0 FROM mart.fact_orders",
            "fact_order_items_not_empty": "SELECT COUNT(*) > 0 FROM mart.fact_order_items",
            "daily_metrics_not_empty": "SELECT COUNT(*) > 0 FROM mart.daily_commerce_metrics",
            "daily_summary_not_empty": "SELECT COUNT(*) > 0 FROM mart.daily_summary_metrics",
            "order_date_not_null": "SELECT COUNT(*) = 0 FROM mart.fact_orders WHERE order_date IS NULL",
            "fact_items_orphaned": "SELECT COUNT(*) = 0 FROM mart.fact_order_items foi LEFT JOIN mart.fact_orders fo ON foi.order_id = fo.order_id WHERE fo.order_id IS NULL",
            "fact_orders_pk_unique": "SELECT COUNT(*) = 0 FROM (SELECT order_id, COUNT(*) FROM mart.fact_orders GROUP BY order_id HAVING COUNT(*) > 1)",
        }

        failed: list[str] = []
        for name, query in checks.items():
            ok = bool(con.execute(query).fetchone()[0])
            logger.info("dq_check_result", extra={"context": {"check": name, "ok": ok}})
            if not ok:
                failed.append(name)

        if failed:
            logger.error("dq_checks_failed", extra={"context": {"failed_checks": failed}})
            raise RuntimeError(f"DQ checks failed: {', '.join(failed)}")

        logger.info("dq_checks_passed")
    finally:
        con.close()
        logger.info("dq_checks_done")


if __name__ == "__main__":
    main()
