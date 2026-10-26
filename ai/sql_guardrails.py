"""Guardrails for LLM-generated SQL. No LLM or Snowflake calls here, so every check is unit-testable.

Defense in depth, strongest first:
  1. Snowflake read-only role (configured in Snowflake, see README note in text_to_sql_bot.py)
  2. validate_sql: AST allowlist of statement type, tables, columns, functions, plus row cap
  3. Execution limits: statement timeout, query tag for audit
  4. LLM intent classifier (text_to_sql_bot.py), which can be talked around, so never the only gate
"""

import re

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

MAX_QUESTION_CHARS = 500
MAX_ROWS = 1000

# Single source of truth: the prompt's schema text and the validator both read this
ALLOWED_TABLES = {
    "FACT_ORDERS": ["order_id", "order_timestamp", "order_date", "customer_id", "restaurant_id", "city",
                    "cuisine", "payment_method", "order_status", "is_delivered", "items_count", "sales_qty",
                    "subtotal", "discount", "delivery_fee", "gst", "sales_amount", "customer_rating",
                    "delivery_time_min"],
    "DIM_RESTAURANTS": ["restaurant_id", "restaurant_name", "city", "cuisine", "rating", "rating_count",
                        "cost_for_two"],
    "DIM_CUSTOMER": ["customer_id", "age", "age_segment", "gender"],
    "MART_DAILY_CITY_REVENUE": ["order_date", "city", "orders", "delivered_orders", "cancel_rate", "gmv",
                                "avg_order_value"],
    "MART_RESTAURANT_PERFORMANCE": ["restaurant_id", "restaurant_name", "city", "cuisine", "orders", "revenue",
                                    "avg_customer_rating", "avg_delivery_min"],
    "MART_DELIVERY_SLA": ["city", "order_hour", "delivered_orders", "p50", "p90"],
}
ALLOWED_COLUMNS = {col for cols in ALLOWED_TABLES.values() for col in cols}

# DIM_CUSTOMER columns that exist in Snowflake but must never be selected, aliased, or filtered on
PII_COLUMNS = {"customer_name", "email", "marital_status", "occupation", "income_band", "education", "family_size"}

# Metadata / session / stage functions that leak account info or read outside the allowlist
BLOCKED_FUNCTIONS = {"GET_DDL", "LAST_QUERY_ID", "RESULT_SCAN", "GENERATOR", "DYNAMIC_IDENTIFIER", "IDENTIFIER",
                     "INVOKER_ROLE", "INVOKER_SHARE", "IS_ROLE_IN_SESSION", "IS_DATABASE_ROLE_IN_SESSION",
                     "GET_STAGE_LOCATION", "GET_PRESIGNED_URL", "BUILD_STAGE_FILE_URL",
                     "BUILD_SCOPED_FILE_URL", "GET_ABSOLUTE_PATH", "GET_RELATIVE_PATH"}
ALLOWED_CURRENT_FUNCTIONS = {"CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP"}

WRITE_NODES = (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Create, exp.Drop, exp.Alter, exp.Command,
               exp.Into, exp.Use, exp.Show, exp.Set, exp.Transaction, exp.Commit, exp.Rollback)

CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")


class GuardrailError(Exception):
    """Raised by any guardrail. `code` is safe to show users and to log; details go to the audit log only."""

    def __init__(self, code, detail=""):
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


def render_schema():
    lines = [f"{table}({', '.join(cols)})" for table, cols in ALLOWED_TABLES.items()]
    return "\n".join(lines)


def check_question(question):
    """Pre-generation hook: reject empty, oversized, or control-character input before it reaches the LLM."""
    question = CONTROL_CHARS.sub("", question or "").strip()
    if not question:
        raise GuardrailError("empty_question")
    if len(question) > MAX_QUESTION_CHARS:
        raise GuardrailError("question_too_long", f"{len(question)} > {MAX_QUESTION_CHARS} chars")
    return question


def _function_name(func):
    return (func.name if isinstance(func, exp.Anonymous) else func.sql_name()).upper()


def _check_statement(ast):
    if not isinstance(ast, (exp.Select, exp.SetOperation)):
        raise GuardrailError("not_select", type(ast).__name__)
    for node in ast.find_all(*WRITE_NODES):
        raise GuardrailError("write_or_command", type(node).__name__)


def _check_sources(ast):
    """Every FROM/JOIN source must be a plain table or a subquery: no TABLE(...), stages, IDENTIFIER(), LATERAL."""
    for clause in ast.find_all(exp.From, exp.Join):
        source = clause.this
        if isinstance(source, exp.Subquery):
            continue
        if not (isinstance(source, exp.Table) and isinstance(source.this, exp.Identifier)):
            raise GuardrailError("disallowed_source", source.sql(dialect="snowflake"))
    if any(ast.find_all(exp.Lateral)):
        raise GuardrailError("disallowed_source", "LATERAL")


def _check_tables(ast):
    cte_names = {cte.alias_or_name.upper() for cte in ast.find_all(exp.CTE)}
    for table in ast.find_all(exp.Table):
        name = table.name.upper()
        catalog, db = table.catalog.upper(), table.db.upper()
        if (catalog, db) not in {("", ""), ("", "MARTS"), ("ZOMATO", "MARTS")}:
            raise GuardrailError("disallowed_table", table.sql(dialect="snowflake"))
        if name not in ALLOWED_TABLES and not (name in cte_names and not db):
            raise GuardrailError("disallowed_table", name)
        # Strip ZOMATO.MARTS. in the AST so string literals are never touched
        table.set("catalog", None)
        table.set("db", None)


def _check_columns(ast):
    aliases = {a.alias.lower() for a in ast.find_all(exp.Alias)}
    aliases |= {c.name.lower() for ta in ast.find_all(exp.TableAlias) for c in ta.columns}
    for name in aliases & PII_COLUMNS:
        raise GuardrailError("pii_column", name)

    for star in ast.find_all(exp.Star):
        if not isinstance(star.parent, exp.Count):
            raise GuardrailError("select_star", "list columns explicitly")

    for column in ast.find_all(exp.Column):
        name = column.name.lower()
        if name in PII_COLUMNS:
            raise GuardrailError("pii_column", name)
        if name not in ALLOWED_COLUMNS and name not in aliases:
            raise GuardrailError("disallowed_column", name)


def _check_functions(ast):
    for func in ast.find_all(exp.Func):
        name = _function_name(func)
        if (name in BLOCKED_FUNCTIONS or name.startswith("SYSTEM$")
                or (name.startswith("CURRENT_") and name not in ALLOWED_CURRENT_FUNCTIONS)):
            raise GuardrailError("disallowed_function", name)


def _cap_rows(ast):
    limit = ast.args.get("limit")
    value = limit.expression if limit is not None else None
    if not (isinstance(value, exp.Literal) and value.is_int and int(value.this) <= MAX_ROWS):
        ast = ast.limit(MAX_ROWS)
    return ast


def validate_sql(sql):
    """Post-generation hook: parse, allowlist-check, cap rows, and return the SQL to execute.

    Always execute the returned string, never the model's raw output: it is re-rendered from the checked AST.
    """
    try:
        statements = [s for s in sqlglot.parse(sql, read="snowflake") if s is not None]
    except ParseError as e:
        raise GuardrailError("unparseable_sql", str(e)[:200]) from e
    if len(statements) != 1:
        raise GuardrailError("multiple_statements", str(len(statements)))

    ast = statements[0]
    _check_statement(ast)
    _check_sources(ast)
    _check_tables(ast)
    _check_columns(ast)
    _check_functions(ast)
    return _cap_rows(ast).sql(dialect="snowflake")
