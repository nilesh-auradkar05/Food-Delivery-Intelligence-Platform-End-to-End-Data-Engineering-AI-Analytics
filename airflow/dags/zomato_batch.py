from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator

DBT = "/opt/airflow/dbt_venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/zomato"
DBT_PROFILES = "/opt/airflow/dbt/profiles"

# CSV columns in file order, and ON_ERROR per table (dimensions skip bad rows, facts abort)
RAW_TABLES = {
    "restaurants": (["_idx", "id", "name", "city", "rating", "rating_count", "cost", "cuisine", "lic_no", "link",
                     "address", "menu"], "CONTINUE"),
    "users":       (["_idx", "user_id", "name", "email", "password", "age", "gender", "marital_status",
                     "occupation", "monthly_income", "education", "family_size"], "CONTINUE"),
    "food":        (["_idx", "f_id", "item", "veg_or_non_veg"], "CONTINUE"),
    "menu":        (["_idx", "menu_id", "r_id", "f_id", "cuisine", "price"], "CONTINUE"),
    "orders":      (["order_id", "order_timestamp", "order_date", "user_id", "r_id", "restaurant_city", "cuisine",
                     "items_count", "sales_qty", "subtotal", "discount", "delivery_fee", "gst", "sales_amount",
                     "currency", "payment_method", "order_status", "customer_rating", "delivery_time_min"],
                    "ABORT_STATEMENT"),
    "order_items": (["order_item_id", "order_id", "r_id", "f_id", "price", "quantity", "line_amount"],
                    "ABORT_STATEMENT"),
    "reviews":     (["review_id", "order_id", "user_id", "restaurant_id", "rating", "comment", "review_date"],
                    "ABORT_STATEMENT"),
}

def copy_incremental(table, columns, on_error):
    """COPY skips files already loaded into this table, so only new files land. Adds lineage columns for dbt."""
    positions = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
    return (
        f"COPY INTO ZOMATO.RAW.{table} ({', '.join(columns)}, _source_file, _loaded_at) "
        f"FROM (SELECT {positions}, METADATA$FILENAME, METADATA$START_SCAN_TIME "
        f"FROM @ZOMATO.RAW.ZOMATO_RAW_STAGE/{table}/) "
        f"PATTERN = '.*[.]csv([.]gz)?' ON_ERROR = '{on_error}'"
    )

# Never CREATE OR REPLACE / TRUNCATE the RAW tables: that wipes load metadata and the next run reloads every file
COPY_RAW = ["USE WAREHOUSE ZOMATO_WH"] + [copy_incremental(t, cols, err) for t, (cols, err) in RAW_TABLES.items()]

with DAG(
    dag_id="zomato_batch",
    start_date=datetime(2024,1,1),
    schedule="@yearly",
    catchup=False,
    tags=["zomato","dbt","snowflake"],
    doc_md=__doc__,
) as dag:

    reload_raw = SQLExecuteQueryOperator(
        task_id="reload_raw",
        conn_id="snowflake_default",
        sql=COPY_RAW,
        split_statements=True,
        autocommit=True,
    )

    dbt_build_core = BashOperator(
        task_id="dbt_build_core",
        bash_command=f"{DBT} build --exclude tag:ai --project-dir {DBT_PROJECT} --profiles-dir {DBT_PROFILES}",
    )

    enrich_reviews = BashOperator(
        task_id="enrich_reviews",
        bash_command="uv run python3 /opt/airflow/ai/enrich_reviews.py",
    )

    dbt_build_ai = BashOperator(
        task_id="dbt_build_ai",
        bash_command=f"{DBT} build --select tag:ai --project-dir {DBT_PROJECT} --profiles-dir {DBT_PROFILES}"
    )

    reload_raw >> dbt_build_core >> enrich_reviews >> dbt_build_ai