"""Build GBNF grammar for constrained IQL generation.

The grammar guarantees the LLM output is syntactically valid IQL.
Column names are derived from the live catalog so hallucinated columns are impossible.
"""

from __future__ import annotations

# Default catalog — in production, load from the catalog endpoint.
DEFAULT_TABLES: dict[str, list[str]] = {
    "logs": [
        "timestamp_ns", "severity_number", "severity_text", "body",
        "trace_id", "span_id", "service_name", "attributes",
    ],
    "metrics": [
        "timestamp_ns", "metric_name", "metric_type", "value",
        "labels", "service_name",
    ],
    "traces": [
        "timestamp_ns", "trace_id", "span_id", "parent_span_id",
        "name", "service_name", "duration_ns", "status_code", "attributes",
    ],
}

TEMPORAL_FUNCTIONS = [
    "time_bucket", "rate", "delta", "histogram_quantile", "now",
]

AGGREGATE_FUNCTIONS = ["count", "sum", "avg", "min", "max"]

ALL_FUNCTIONS = TEMPORAL_FUNCTIONS + AGGREGATE_FUNCTIONS


def _quote(values: list[str]) -> str:
    """Produce a GBNF alternation: '"a" | "b" | ...'"""
    return " | ".join(f'"{v}"' for v in values)


def build_gbnf_grammar(
    tables: dict[str, list[str]] | None = None,
    *,
    selected_table: str | None = None,
) -> str:
    """Return a GBNF grammar string for IQL.

    If `selected_table` is provided, column names are restricted to that table.
    Otherwise all columns across all tables are allowed (less precise).
    """
    catalog = tables or DEFAULT_TABLES

    table_names = list(catalog.keys())

    if selected_table and selected_table in catalog:
        col_names = catalog[selected_table]
    else:
        # Union of all columns
        seen: dict[str, None] = {}
        for cols in catalog.values():
            for c in cols:
                seen[c] = None
        col_names = list(seen.keys())

    func_names = ALL_FUNCTIONS

    lines = [
        'root         ::= select-stmt',
        'ws           ::= " "+',
        'comma-ws     ::= ", "',
        '',
        # Table names
        f'table-name   ::= {_quote(table_names)}',
        '',
        # Column names
        f'column-name  ::= {_quote(col_names)}',
        '',
        # Function names
        f'func-name    ::= {_quote(func_names)}',
        '',
        # Literals
        'integer      ::= [0-9]+',
        'float-lit    ::= [0-9]+ "." [0-9]+',
        'string-lit   ::= "\''" [^"]* "\''\"',
        'literal      ::= integer | float-lit | string-lit | "TRUE" | "FALSE" | "NULL"',
        '',
        # Intervals
        'interval-unit ::= "minutes" | "hours" | "seconds" | "days" | "m" | "h" | "s" | "d"',
        'interval-expr ::= "interval \'" integer ws interval-unit "\'"',
        '',
        # Operators
        'binop        ::= " = " | " != " | " < " | " <= " | " > " | " >= "',
        '',
        # Expressions
        'func-call    ::= func-name "(" (expr-list)? ")"',
        'primary-expr ::= column-name | literal | func-call | "now()" | interval-expr | "(" expr ")"',
        'mult-expr    ::= primary-expr ((" * " | " / ") primary-expr)*',
        'add-expr     ::= mult-expr ((" + " | " - ") mult-expr)*',
        'cmp-expr     ::= add-expr (binop add-expr)?',
        'and-expr     ::= cmp-expr (" AND " cmp-expr)*',
        'expr         ::= and-expr (" OR " and-expr)*',
        'expr-list    ::= expr (comma-ws expr)*',
        '',
        # Projections
        'alias        ::= " AS " column-name',
        'projection   ::= expr (alias)?',
        'proj-star    ::= "*"',
        'projections  ::= proj-star | projection (comma-ws projection)*',
        '',
        # ORDER BY
        'order-dir    ::= " ASC" | " DESC"',
        'order-item   ::= expr (order-dir)?',
        'order-list   ::= order-item (comma-ws order-item)*',
        '',
        # LIMIT
        'limit-clause  ::= " LIMIT " integer',
        'offset-clause ::= " OFFSET " integer',
        '',
        # GROUP BY / HAVING
        'group-clause  ::= " GROUP BY " expr-list',
        'having-clause ::= " HAVING " expr',
        '',
        # WHERE
        'where-clause  ::= " WHERE " expr',
        '',
        # Full SELECT
        'select-stmt  ::= "SELECT " projections " FROM " table-name',
        '               (where-clause)?',
        '               (group-clause)?',
        '               (having-clause)?',
        '               (" ORDER BY " order-list)?',
        '               (limit-clause)?',
        '               (offset-clause)?',
        '               ";"',
    ]

    return "\n".join(lines)


if __name__ == "__main__":
    print(build_gbnf_grammar())
