//! Rule-based optimizer rewrites.

use std::sync::Arc;

use crate::{
    ast::{Expr, Literal},
    catalog::Catalog,
    planner::{LogicalPlan, ScanOp},
};

// ── Constant folding ──────────────────────────────────────────────────────────

/// Evaluate `now()`, `interval(...)`, and constant arithmetic at plan time.
pub fn fold_constants(plan: LogicalPlan) -> LogicalPlan {
    transform_plan(plan, &fold_expr)
}

fn fold_expr(expr: Expr) -> Expr {
    match expr {
        Expr::Function { ref name, ref args } if name == "now" && args.is_empty() => {
            let ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64;
            Expr::Literal(Literal::Int(ns))
        }
        Expr::BinOp { op, left, right } => {
            let l = fold_expr(*left);
            let r = fold_expr(*right);
            match (&l, &r) {
                (Expr::Literal(Literal::Int(a)), Expr::Literal(Literal::Int(b))) => {
                    use crate::ast::BinOp::*;
                    if let Some(v) = match op {
                        Add => a.checked_add(*b),
                        Sub => a.checked_sub(*b),
                        Mul => a.checked_mul(*b),
                        Div if *b != 0 => a.checked_div(*b),
                        Mod if *b != 0 => a.checked_rem(*b),
                        _ => None,
                    } { return Expr::Literal(Literal::Int(v)); }
                }
                _ => {}
            }
            Expr::BinOp { op, left: Box::new(l), right: Box::new(r) }
        }
        other => other,
    }
}

// ── Predicate pushdown ────────────────────────────────────────────────────────

/// Push WHERE predicates into Scan nodes where possible.
pub fn predicate_pushdown(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { predicate, input } => {
            let input = predicate_pushdown(*input);
            match input {
                LogicalPlan::Scan(mut scan) => {
                    scan.predicate = Some(predicate);
                    LogicalPlan::Scan(scan)
                }
                other => LogicalPlan::Filter { predicate, input: Box::new(other) },
            }
        }
        other => map_children(other, predicate_pushdown),
    }
}

// ── Projection pushdown ───────────────────────────────────────────────────────

/// Restrict scans to only columns actually referenced.
pub fn projection_pushdown(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Project { columns, input } => {
            let input = projection_pushdown(*input);
            match input {
                LogicalPlan::Scan(mut scan) => {
                    if scan.projection.is_none() {
                        scan.projection = Some(columns.clone());
                    }
                    LogicalPlan::Project { columns, input: Box::new(LogicalPlan::Scan(scan)) }
                }
                other => LogicalPlan::Project { columns, input: Box::new(other) },
            }
        }
        other => map_children(other, projection_pushdown),
    }
}

// ── Partition pruning ─────────────────────────────────────────────────────────

/// Use zone-map stats to restrict scan to time-overlapping partitions.
pub fn partition_prune(plan: LogicalPlan, catalog: &Arc<Catalog>) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan(mut scan) => {
            if let Some(ref pred) = scan.predicate.clone() {
                if let Some((start, end)) = extract_time_range(pred) {
                    scan.partition_filter =
                        catalog.overlapping_partitions(&scan.table, start, end);
                }
            }
            LogicalPlan::Scan(scan)
        }
        other => map_children(other, |p| partition_prune(p, catalog)),
    }
}

/// Extract (start_ns, end_ns) from a predicate tree if it contains a simple
/// `timestamp_ns >= X AND timestamp_ns <= Y` pattern (post constant-folding).
fn extract_time_range(expr: &Expr) -> Option<(u64, u64)> {
    use crate::ast::BinOp;

    fn is_ts(e: &Expr) -> bool {
        matches!(e, Expr::Column { name, .. } if name == "timestamp_ns")
    }
    fn as_u64(e: &Expr) -> Option<u64> {
        match e { Expr::Literal(crate::ast::Literal::Int(n)) => Some(*n as u64), _ => None }
    }

    match expr {
        Expr::BinOp { op: BinOp::And, left, right } => {
            let l = extract_time_range(left);
            let r = extract_time_range(right);
            match (l, r) {
                (Some((ls, _)), Some((_, re))) => Some((ls, re)),
                (Some(v), None) | (None, Some(v)) => Some(v),
                _ => None,
            }
        }
        Expr::BinOp { op: BinOp::GtEq, left, right } if is_ts(left) => {
            as_u64(right).map(|v| (v, u64::MAX))
        }
        Expr::BinOp { op: BinOp::LtEq, left, right } if is_ts(left) => {
            as_u64(right).map(|v| (0, v))
        }
        Expr::Between { expr, low, high } if is_ts(expr) => {
            match (as_u64(low), as_u64(high)) {
                (Some(lo), Some(hi)) => Some((lo, hi)),
                _ => None,
            }
        }
        _ => None,
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn transform_plan(plan: LogicalPlan, f: &dyn Fn(Expr) -> Expr) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { predicate, input } =>
            LogicalPlan::Filter { predicate: f(predicate), input: Box::new(transform_plan(*input, f)) },
        LogicalPlan::Scan(mut scan) => {
            scan.predicate = scan.predicate.map(f);
            LogicalPlan::Scan(scan)
        }
        other => map_children(other, |p| transform_plan(p, f)),
    }
}

fn map_children(plan: LogicalPlan, f: impl Fn(LogicalPlan) -> LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter    { predicate, input }   => LogicalPlan::Filter    { predicate, input: Box::new(f(*input)) },
        LogicalPlan::Project   { columns, input }     => LogicalPlan::Project   { columns, input: Box::new(f(*input)) },
        LogicalPlan::Aggregate { keys, aggregates, input } =>
            LogicalPlan::Aggregate { keys, aggregates, input: Box::new(f(*input)) },
        LogicalPlan::Sort      { keys, input }        => LogicalPlan::Sort      { keys, input: Box::new(f(*input)) },
        LogicalPlan::Limit     { n, offset, input }   => LogicalPlan::Limit     { n, offset, input: Box::new(f(*input)) },
        LogicalPlan::Join      { kind, left, right, condition } =>
            LogicalPlan::Join { kind, left: Box::new(f(*left)), right: Box::new(f(*right)), condition },
        other => other,
    }
}
