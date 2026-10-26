"""Run: uv run pytest ai/test_sql_guardrails.py  (no LLM, no Snowflake)"""

import pytest

from sql_guardrails import MAX_ROWS, GuardrailError, check_question, validate_sql

BLOCKED = {
    "DELETE FROM FACT_ORDERS": "not_select",
    "SELECT 1; DROP TABLE FACT_ORDERS": "multiple_statements",
    "SHOW TABLES": "not_select",
    "CALL some_proc()": "not_select",
    "EXECUTE IMMEDIATE 'DROP TABLE FACT_ORDERS'": "not_select",
    "USE ROLE ACCOUNTADMIN": "not_select",
    "SELECT table_name FROM INFORMATION_SCHEMA.TABLES": "disallowed_table",
    "SELECT comment FROM ZOMATO.RAW.REVIEWS": "disallowed_table",
    "SELECT city FROM ZOMATO.STAGING.STG_ORDERS": "disallowed_table",
    "SELECT CURRENT_ROLE()": "disallowed_function",
    "SELECT CURRENT_USER, city FROM FACT_ORDERS": "disallowed_function",
    "SELECT GET_DDL('table', 'FACT_ORDERS')": "disallowed_function",
    "SELECT SYSTEM$ALLOWLIST()": "disallowed_function",
    "SELECT city FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))": "disallowed_source",
    "SELECT city FROM IDENTIFIER('ZOMATO.RAW.REVIEWS')": "disallowed_source",
    "SELECT $1 FROM @my_stage": "disallowed_source",
    "SELECT customer_name AS n FROM DIM_CUSTOMER": "pii_column",
    "SELECT email FROM DIM_CUSTOMER": "pii_column",
    "SELECT c.age AS email FROM DIM_CUSTOMER c": "pii_column",
    "SELECT * FROM DIM_CUSTOMER": "select_star",
    "SELECT c.* FROM DIM_CUSTOMER c": "select_star",
    "WITH t AS (SELECT table_name FROM INFORMATION_SCHEMA.TABLES) SELECT table_name FROM t": "disallowed_table",
    "WITH FACT_ORDERS AS (SELECT comment FROM ZOMATO.RAW.REVIEWS) SELECT comment FROM FACT_ORDERS": "disallowed_table",
    "SELECT secret_col FROM FACT_ORDERS": "disallowed_column",
    "not sql at all ((": "unparseable_sql",
}


@pytest.mark.parametrize("sql,code", BLOCKED.items())
def test_blocked(sql, code):
    with pytest.raises(GuardrailError) as err:
        validate_sql(sql)
    assert err.value.code == code


ALLOWED = [
    "SELECT city, SUM(gmv) AS total_gmv FROM MART_DAILY_CITY_REVENUE GROUP BY city ORDER BY total_gmv DESC LIMIT 10",
    "SELECT cuisine, COUNT(*) AS orders FROM FACT_ORDERS GROUP BY cuisine ORDER BY orders DESC",
    "SELECT payment_method, AVG(IFF(order_status = 'Cancelled', 1, 0)) AS cancel_rate FROM FACT_ORDERS GROUP BY 1",
    "SELECT city, SUM(gmv) FROM ZOMATO.MARTS.MART_DAILY_CITY_REVENUE "
    "WHERE order_date >= DATEADD(day, -30, CURRENT_DATE) GROUP BY city",
    "WITH c AS (SELECT city, AVG(delivery_time_min) AS avg_min FROM FACT_ORDERS GROUP BY city) "
    "SELECT city, avg_min FROM c ORDER BY avg_min DESC",
    "SELECT d.age_segment, COUNT(DISTINCT f.customer_id) AS customers "
    "FROM FACT_ORDERS f JOIN DIM_CUSTOMER d ON f.customer_id = d.customer_id GROUP BY d.age_segment",
]


@pytest.mark.parametrize("sql", ALLOWED)
def test_allowed_and_row_capped(sql):
    out = validate_sql(sql)
    assert "LIMIT" in out.upper()
    assert "ZOMATO." not in out.upper()


def test_limit_above_cap_is_lowered():
    out = validate_sql("SELECT city FROM FACT_ORDERS LIMIT 999999")
    assert out.endswith(f"LIMIT {MAX_ROWS}")


def test_small_limit_is_kept():
    assert validate_sql("SELECT city FROM FACT_ORDERS LIMIT 5").endswith("LIMIT 5")


def test_string_literals_untouched():
    out = validate_sql("SELECT city FROM FACT_ORDERS WHERE city = 'ZOMATO.MARTS.'")
    assert "'ZOMATO.MARTS.'" in out


@pytest.mark.parametrize("question,code", [("", "empty_question"), ("   \x00 ", "empty_question"),
                                           ("x" * 501, "question_too_long")])
def test_check_question_rejects(question, code):
    with pytest.raises(GuardrailError) as err:
        check_question(question)
    assert err.value.code == code
