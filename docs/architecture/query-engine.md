# Query Engine

**Crate:** `crates/infralens-query`

IQL (InfraLens Query Language) is a SQL dialect tailored for observability data. The
query engine is a classic pipeline: Lexer → Parser → AST → Planner → Optimizer →
Executor.

---

## Pipeline Overview

```
IQL string
    │
    ▼
Lexer               Hand-written, zero-allocation tokeniser
    │ Vec<Token>
    ▼
Parser              Recursive-descent, full precedence chain
    │ Statement (AST)
    ▼
Planner             AST → LogicalPlan tree
    │ LogicalPlan
    ▼
Optimizer           Applies rewrite rules (4 rules)
    │ LogicalPlan (optimised)
    ▼
Executor            Pull-based, 8192-row Arrow batches
    │ Vec<RecordBatch>
    ▼
Serialiser          Arrow → NDJSON rows
```

---

## Lexer

**File:** `crates/infralens-query/src/lexer.rs`

The lexer converts an IQL string into a flat `Vec<Token>` in a single pass. It is
hand-written (no external parser combinator library) to keep compile times fast.

Token types:

| Category | Examples |
|----------|---------|
| Keywords | `SELECT`, `FROM`, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`, `INTERVAL` |
| Identifiers | `service_name`, `timestamp_ns`, `time_bucket` |
| Literals | `'hello'` (string), `42` (integer), `3.14` (float) |
| Operators | `>=`, `<=`, `!=`, `AND`, `OR`, `NOT`, `LIKE`, `IN`, `BETWEEN`, `IS NULL` |
| Punctuation | `(`, `)`, `,`, `.`, `;` |

No heap allocation is performed for tokens that reference the original string —
identifiers and string literals are stored as `&str` slices into the input.

---

## Parser

**File:** `crates/infralens-query/src/parser.rs`

Recursive-descent parser producing a strongly-typed AST. The precedence chain (low to
high):

```
OR
  AND
    NOT
      comparison  (=, !=, <, <=, >, >=, LIKE, IN, BETWEEN, IS NULL)
        addition / subtraction
          multiplication / division
            unary (-)
              primary (literal, identifier, function call, subexpression)
```

The parser handles:
- `SELECT` with aliased projections (`expr AS alias`)
- `FROM` with a single table name
- `WHERE` with the full boolean expression grammar above
- `GROUP BY` with an expression list
- `ORDER BY` with `ASC`/`DESC`
- `LIMIT` with an integer constant
- `INTERVAL` literals (`INTERVAL '5 minutes'`, `INTERVAL '1 hour'`)
- Aggregate functions: `count(*)`, `count(expr)`, `sum`, `avg`, `min`, `max`
- Window functions: `count(*) FILTER (WHERE expr)`

### AST types

**File:** `crates/infralens-query/src/ast.rs`

```
Statement
  SelectStatement
    projections: Vec<Projection>       SELECT list
    table:       String                FROM clause
    predicate:   Option<Expr>          WHERE clause
    group_by:    Vec<Expr>             GROUP BY clause
    order_by:    Vec<(Expr, SortDir)>  ORDER BY clause
    limit:       Option<u64>           LIMIT clause

Expr
  Literal(Literal)          string, integer, float, bool, null
  Identifier(String)        column name
  BinOp(Box<Expr>, BinOp, Box<Expr>)
  UnaryOp(UnaryOp, Box<Expr>)
  FunctionCall(String, Vec<Expr>)
  IsNull(Box<Expr>)
  Between(Box<Expr>, Box<Expr>, Box<Expr>)
  In(Box<Expr>, Vec<Expr>)
  Like(Box<Expr>, String)
  Interval(u64)             pre-converted to nanoseconds

Literal
  String(String)
  Integer(i64)
  Float(f64)
  Bool(bool)
  Null
```

---

## Planner

**File:** `crates/infralens-query/src/planner.rs`

The planner converts a `SelectStatement` into a `LogicalPlan` tree. A `LogicalPlan` is
a recursive enum where each node wraps its input:

```
LogicalPlan
  Scan       { table, predicate, projections }
  Filter     { input, predicate }
  Project    { input, projections }
  Aggregate  { input, group_by, aggregates }
  Sort       { input, order_by }
  Limit      { input, n }
```

The default plan shape for `SELECT a, avg(b) FROM t WHERE c > 1 GROUP BY a ORDER BY a LIMIT 10`:

```
Limit(10)
  Sort(a ASC)
    Aggregate(group_by=[a], aggs=[avg(b)])
      Filter(c > 1)
        Scan(t, projections=[a, b, c])
```

---

## Optimizer

**File:** `crates/infralens-query/src/optimizer/`

The optimizer applies four rewrite rules in order. Each rule is a function
`fn rewrite(plan: LogicalPlan) -> LogicalPlan` that pattern-matches and returns a
potentially different plan.

### Rule 1 — Constant Folding

**File:** `crates/infralens-query/src/optimizer/rules.rs`

Evaluates `now()` at plan time (once, at query start) and replaces it with a literal
nanosecond timestamp. This ensures consistent results within a query and allows the
subsequent rules to see concrete timestamp values.

```sql
-- Before constant folding
WHERE timestamp_ns >= now() - INTERVAL '1 hour'

-- After constant folding (now() = 1733059200000000000)
WHERE timestamp_ns >= 1733055600000000000
```

### Rule 2 — Predicate Pushdown

**File:** `crates/infralens-query/src/optimizer/rules.rs`

Moves filter predicates as close to the `Scan` node as possible so the storage engine
can evaluate them at read time. When the predicate is a conjunction (`AND`), each
sub-predicate is independently pushed:

```
Before:   Filter(c > 1) → Scan(t)
After:    Scan(t, predicate = c > 1)
```

This avoids materialising rows that will be filtered out.

### Rule 3 — Projection Pushdown

**File:** `crates/infralens-query/src/optimizer/rules.rs`

Collects only the column names actually needed by the query and passes them to the
`Scan` node. The Parquet reader then uses column projection to read only those column
chunks from disk.

A query on `timestamp_ns, body, service_name` in a table with 12 columns reads roughly
3/12 = 25% of the raw bytes.

### Rule 4 — Partition Pruning

**File:** `crates/infralens-query/src/optimizer/cost.rs`

Uses the `timestamp_ns` predicate bounds (known after constant folding) to compute the
set of partition directories that can possibly contain relevant data.

```
partition_min = floor(predicate_low  / partition_ns)
partition_max = floor(predicate_high / partition_ns)
```

Only partitions in `[partition_min, partition_max]` are opened. This is the coarsest
pruning level — zone-map pruning within each partition is the second level.

---

## Executor

**Files:** `crates/infralens-query/src/executor/`

The executor is pull-based: each operator implements `fn next(&mut self) -> Option<RecordBatch>`.
Batches are 8192 rows (chosen to fit L1 cache while keeping operator overhead per row low).

### Operators

| Operator | File | Behaviour |
|----------|------|-----------|
| `ScanOperator` | `scan.rs` | Opens Parquet files, checks zone maps and blooms, reads column batches |
| `FilterOperator` | `filter.rs` | Evaluates a predicate Arrow `BooleanArray`, filters rows |
| `ProjectOperator` | (in `mod.rs`) | Selects and renames columns |
| `AggregateOperator` | `aggregate.rs` | Hash-aggregate with group keys, supports `count`, `sum`, `avg`, `min`, `max` |
| `SortOperator` | `sort.rs` | Collects all batches, sort by key columns |
| `LimitOperator` | `limit.rs` | Passes through until `n` rows emitted, then `None` |

### Expression evaluation

Expressions (`Expr`) from the AST are evaluated against Arrow `RecordBatch`es using
the `arrow-arith` and `arrow-ord` crates. This vectorises comparisons, arithmetic, and
boolean logic across 8192 rows at once using SIMD where available.

---

## Temporal Functions

**File:** `crates/infralens-query/src/functions/temporal.rs`

| Function | Signature | Description |
|----------|-----------|-------------|
| `now()` | `() → Int64` | Current time as Unix nanoseconds |
| `time_bucket(width, ts)` | `(Interval, Int64) → Int64` | Floors `ts` to the nearest `width` boundary |
| `rate(value, window)` | `(Float64, Interval) → Float64` | Per-second rate over a time window |
| `delta(value, window)` | `(Float64, Interval) → Float64` | Absolute change over a time window |
| `histogram_quantile(q, col)` | `(Float64, Float64) → Float64` | Estimated quantile from histogram data |

`time_bucket` is implemented as:

```rust
fn time_bucket(width_ns: i64, ts: i64) -> i64 {
    (ts / width_ns) * width_ns
}
```

All temporal functions are registered in `functions/mod.rs` and resolved by name
during planning.

---

## Supported IQL Syntax

```sql
SELECT
    <expr> [AS <alias>] [, ...]
    | *
FROM <table>               -- logs | metrics | traces
[WHERE <predicate>]
[GROUP BY <expr> [, ...]]
[ORDER BY <expr> [ASC|DESC] [, ...]]
[LIMIT <integer>]

<predicate>
  ::= <expr> AND <predicate>
    | <expr> OR  <predicate>
    | NOT <predicate>
    | <expr> {= | != | < | <= | > | >=} <expr>
    | <expr> LIKE '<pattern>'
    | <expr> IN  (<expr> [, ...])
    | <expr> BETWEEN <expr> AND <expr>
    | <expr> IS [NOT] NULL
    | <expr> FILTER (WHERE <predicate>)   -- aggregate filter

<interval>
  ::= INTERVAL '<integer> <unit>'
  unit ::= seconds | minutes | hours | days | ms

<function>
  ::= now()
    | time_bucket(<interval>, <expr>)
    | rate(<expr>, <interval>)
    | delta(<expr>, <interval>)
    | histogram_quantile(<float>, <expr>)
    | count(*) | count(<expr>)
    | sum(<expr>) | avg(<expr>) | min(<expr>) | max(<expr>)
```
