from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = os.getenv("PROJECT_DIR", "/opt/airflow/project")

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="ecommerce_elt_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["assignment", "ecommerce", "elt"],
) as dag:
    validate_inputs = BashOperator(
        task_id="validate_inputs",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python - << 'PY'\n"
            "from pathlib import Path\n"
            "base=Path('data/raw')\n"
            "required=['users.csv','products.csv','orders.csv','order_items.csv','distribution_centers.csv','inventory_items.csv','events.csv']\n"
            "missing=[f for f in required if not (base/f).exists()]\n"
            "if missing: raise SystemExit('Missing CSV files: '+', '.join(missing))\n"
            "print('All input files exist.')\n"
            "PY"
        ),
    )

    load_users = BashOperator(
        task_id="load_users",
        bash_command=f"cd {PROJECT_DIR} && python src/pipeline_duckdb.py --step load_table --table users",
    )
    load_orders = BashOperator(
        task_id="load_orders",
        bash_command=f"cd {PROJECT_DIR} && python src/pipeline_duckdb.py --step load_table --table orders",
    )
    load_order_items = BashOperator(
        task_id="load_order_items",
        bash_command=f"cd {PROJECT_DIR} && python src/pipeline_duckdb.py --step load_table --table order_items",
    )
    load_products = BashOperator(
        task_id="load_products",
        bash_command=f"cd {PROJECT_DIR} && python src/pipeline_duckdb.py --step load_table --table products",
    )
    load_distribution_centers = BashOperator(
        task_id="load_distribution_centers",
        bash_command=f"cd {PROJECT_DIR} && python src/pipeline_duckdb.py --step load_table --table distribution_centers",
    )
    load_inventory_items = BashOperator(
        task_id="load_inventory_items",
        bash_command=f"cd {PROJECT_DIR} && python src/pipeline_duckdb.py --step load_table --table inventory_items",
    )
    load_events = BashOperator(
        task_id="load_events",
        bash_command=f"cd {PROJECT_DIR} && python src/pipeline_duckdb.py --step load_table --table events",
    )

    transform_dims = BashOperator(
        task_id="transform_dims",
        bash_command=f"cd {PROJECT_DIR} && python src/pipeline_duckdb.py --step transform_dims",
    )
    transform_facts = BashOperator(
        task_id="transform_facts",
        bash_command=f"cd {PROJECT_DIR} && python src/pipeline_duckdb.py --step transform_facts",
    )
    transform_views = BashOperator(
        task_id="transform_views",
        bash_command=f"cd {PROJECT_DIR} && python src/pipeline_duckdb.py --step transform_views",
    )

    run_dq_checks = BashOperator(
        task_id="run_dq_checks",
        bash_command=f"cd {PROJECT_DIR} && python scripts/dq_checks.py",
    )

    validate_inputs >> load_users >> load_orders >> load_order_items >> load_products >> load_distribution_centers >> load_inventory_items >> load_events >> transform_dims >> transform_facts >> transform_views >> run_dq_checks
