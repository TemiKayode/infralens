//! Abstract Syntax Tree for IQL (InfraLens Query Language).

use std::fmt;

// ── Statements ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub projections: Vec<Projection>,
    pub from:        TableRef,
    pub joins:       Vec<JoinClause>,
    pub filter:      Option<Expr>,
    pub group_by:    Vec<Expr>,
    pub having:      Option<Expr>,
    pub order_by:    Vec<OrderByItem>,
    pub limit:       Option<u64>,
    pub offset:      Option<u64>,
}

// ── Table references ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    pub name:  String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub kind:      JoinKind,
    pub table:     TableRef,
    pub condition: Expr,
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum JoinKind { Inner, Left, Right }

// ── Projections ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    Star,
    Expr { expr: Expr, alias: Option<String> },
}

// ── Ordering ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expr: Expr,
    pub asc:  bool,
}

// ── Expressions ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    // Leaf nodes
    Column { table: Option<String>, name: String },
    Literal(Literal),
    Star,

    // Operators
    BinOp { op: BinOp, left: Box<Expr>, right: Box<Expr> },
    UnaryOp { op: UnaryOp, operand: Box<Expr> },

    // Predicates
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    Between { expr: Box<Expr>, low: Box<Expr>, high: Box<Expr> },
    In      { expr: Box<Expr>, list: Vec<Expr> },
    Like    { expr: Box<Expr>, pattern: String, negated: bool },

    // Function calls (including temporal)
    Function { name: String, args: Vec<Expr> },

    // Temporal built-ins (desugared from Function during binding)
    TimeBucket  { width_ns: i64, ts_expr: Box<Expr> },
    Rate        { value_expr: Box<Expr>, window_ns: i64 },
    Delta       { value_expr: Box<Expr>, window_ns: i64 },
    HistogramQuantile { q: f64, value_expr: Box<Expr>, buckets: u32 },
    Now,
    Interval(i64),  // nanoseconds
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum BinOp {
    Add, Sub, Mul, Div, Mod,
    Eq, NotEq, Lt, LtEq, Gt, GtEq,
    And, Or,
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum UnaryOp { Neg, Not }

// ── Literals ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    Null,
    IntervalNs(i64), // already-parsed interval as nanoseconds
}

// ── Display impls (for error messages) ───────────────────────────────────────

impl fmt::Display for BinOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            BinOp::Add => "+", BinOp::Sub => "-", BinOp::Mul => "*", BinOp::Div => "/",
            BinOp::Mod => "%", BinOp::Eq => "=", BinOp::NotEq => "!=",
            BinOp::Lt => "<", BinOp::LtEq => "<=", BinOp::Gt => ">", BinOp::GtEq => ">=",
            BinOp::And => "AND", BinOp::Or => "OR",
        };
        write!(f, "{s}")
    }
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.alias {
            Some(a) => write!(f, "{} AS {a}", self.name),
            None    => write!(f, "{}", self.name),
        }
    }
}
