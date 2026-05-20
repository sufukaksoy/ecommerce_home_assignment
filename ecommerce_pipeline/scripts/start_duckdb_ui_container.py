from __future__ import annotations

import atexit
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import duckdb

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app_logging import configure_logging, get_logger

DB_PATH = os.getenv("DUCKDB_PATH", "/app/ecommerce.duckdb")
UI_INTERNAL_PORT = int(os.getenv("DUCKDB_UI_INTERNAL_PORT", "4214"))
UI_EXTERNAL_PORT = int(os.getenv("DUCKDB_UI_EXTERNAL_PORT", "4213"))
RUN_PIPELINE_CMD = os.getenv("RUN_PIPELINE_CMD", "python /app/src/pipeline_duckdb.py")

logger = get_logger("duckdb.ui")


def run_pipeline() -> None:
    logger.info("duckdb_pipeline_start", extra={"context": {"cmd": RUN_PIPELINE_CMD}})
    result = subprocess.run(RUN_PIPELINE_CMD, shell=True, check=False)
    if result.returncode != 0:
        logger.error("duckdb_pipeline_failed", extra={"context": {"return_code": result.returncode}})
        raise SystemExit(result.returncode)
    logger.info("duckdb_pipeline_done")


def is_port_open(port: int, timeout: float = 0.2) -> bool:
    for host in ("localhost", "127.0.0.1", "::1"):
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except Exception:
            continue
    return False


def detect_ui_port() -> int:
    candidates = [UI_INTERNAL_PORT, 4213]
    for _ in range(150):
        for port in candidates:
            if is_port_open(port):
                return port
        time.sleep(0.1)
    raise RuntimeError("DuckDB UI server port could not be detected")


def main() -> int:
    configure_logging()
    logger.info("duckdb_ui_container_start", extra={"context": {"db_path": DB_PATH}})

    run_pipeline()

    con = duckdb.connect(DB_PATH)
    logger.info("duckdb_ui_server_init")
    con.execute("INSTALL ui;")
    con.execute("LOAD ui;")
    try:
        con.execute(f"SET ui_local_port = {UI_INTERNAL_PORT};")
    except Exception:
        logger.warning("duckdb_ui_set_port_ignored", extra={"context": {"requested_port": UI_INTERNAL_PORT}})
    con.execute("CALL start_ui_server();")

    target_port = detect_ui_port()
    logger.info("duckdb_ui_server_detected", extra={"context": {"internal_port": target_port, "external_port": UI_EXTERNAL_PORT}})

    cmd = [
        "socat",
        f"TCP-LISTEN:{UI_EXTERNAL_PORT},fork,reuseaddr",
        f"TCP:localhost:{target_port}",
    ]
    proc = subprocess.Popen(cmd)
    logger.info("duckdb_ui_proxy_started", extra={"context": {"cmd": " ".join(cmd)}})

    def _cleanup(*_: object) -> None:
        logger.info("duckdb_ui_cleanup_start")
        try:
            proc.terminate()
        except Exception:
            pass
        try:
            con.execute("CALL stop_ui_server();")
        except Exception:
            pass
        try:
            con.close()
        except Exception:
            pass
        logger.info("duckdb_ui_cleanup_done")

    atexit.register(_cleanup)
    signal.signal(signal.SIGTERM, _cleanup)
    signal.signal(signal.SIGINT, _cleanup)

    while True:
        if proc.poll() is not None:
            logger.error("duckdb_ui_proxy_exited", extra={"context": {"return_code": proc.returncode}})
            return proc.returncode or 1
        time.sleep(1)


if __name__ == "__main__":
    sys.exit(main())
