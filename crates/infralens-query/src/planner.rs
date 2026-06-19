//! Converts a typed AST into a logical plan, then into a physical plan.

use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::{
    ast::{BinOp, Expr, JoinKind, Literal, Projection, SelectStatement, Statement},
    catalog::Catalog,
    error::{QueryError, Result},
    optimizer::cost::choose_join_strategy,
};

// ── Logical plan ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ScanOp {
    pub table:            String,
    pub predicate:        Option<Expr>,
    pub projection:       Option<Vec<String>>,
    /// Partition keys to scan; empty = all.
    pub partition_filter: Vec<u64>,
    pub limit:            Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinStrategy { NestedLoop, Hash, SortMerge }

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan(ScanOp),
    Filter    { predicate: Expr, input: Box<LogicalPlan> },
    Project   { columns: Vec<String>, input: Box<LogicalPlan> },
    Aggregate { keys: Vec<Expr>, aggregates: Vec<AggExpr>, input: Box<LogicalPlan> },
    Sort      { keys: Vec<(Expr, bool)>, input: Box<LogicalPlan> },
    Limit     { n: u64, offset: u64, input: Box<LogicalPlan> },
    Join      { kind: JoinKind, left: Box<LogicalPlan>, right: Box<LogicalPlan>, condition: Expr },
}

#[derive(Debug, Clone)]
pub struct AggExpr {
    pub func:  String,
    pub arg:   Option<Expr>,
    pub alias: String,
}

// ── Physical plan ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    pub logical: LogicalPlan,
    pub join_strategy: Option<JoinStrategy>,
    pub batch_size:    usize,
}

// ── Planner ───────────────────────────────────────────────────────────────────

pub struct Planner {
    catalog: Arc<Catalog>,
}

impl Planner {
    pub fn new(catalog: Arc<Catalog>) -> Self { Self { catalog } }

    pub fn plan_statement(&self, stmt: Statement) -> Result<LogicalPlan> {
        match stmt {
            Statement::Select(sel) => self.plan_select(sel),
        }
    }

    fn plan_select(&self, sel: SelectStatement) -> Result<LogicalPlan> {
        // Base scan
        let mut plan = LogicalPlan::Scan(ScanOp {
            table:            sel.from.name.clone(),
            predicate:        None,
            projection:       None,
            partition_filter: vec![],
            limit:            sel.limit,
        });

        // Joins
        for join in sel.joins {
            let right_table = join.table.name.clone();
            let right_scan  = LogicalPlan::Scan(ScanOp {
                table: right_table, predicate: None, projection: None,
                partition_filter: vec![], limit: None,
            });
            plan = LogicalPlan::Join {
                kind:      join.kind,
                left:      Box::new(plan),
                right:     Box::new(right_scan),
                condition: join.condition,
            };
        }

        // Filter
        if let Some(pred) = sel.filter {
            plan = LogicalPlan::Filter { predicate: pred, input: Box::new(plan) };
        }

        // Aggregation
        let (agg_exprs, non_agg_cols) = extract_aggregates(&sel.projections);
        if !agg_exprs.is_empty() || !sel.group_by.is_empty() {
            plan = LogicalPlan::Aggregate {
                keys:       sel.group_by.clone(),
                aggregates: agg_exprs,
                input:      Box::new(plan),
            };
        }

        // Having
        if let Some(having) = sel.having {
            plan = LogicalPlan::Filter { predicate: having, input: Box::new(plan) };
        }

        // Projection (non-aggregate columns)
        if !sel.projections.is_empty() {
            let cols: Vec<String> = sel.projections.iter().filter_map(|p| match p {
                Projection::Expr { expr: Expr::Column { name, .. }, alias } =>
                    Some(alias.as_deref().unwrap_or(name).to_string()),
                Projection::Expr { alias: Some(a), .. } => Some(a.clone()),
                _ => None,
            }).collect();
            if !cols.is_empty() {
                plan = LogicalPlan::Project { columns: cols, input: Box::new(plan) };
            }
        }

        // Sort
        if !sel.order_by.is_empty() {
            plan = LogicalPlan::Sort {
                keys:  sel.order_by.iter().map(|o| (o.expr.clone(), o.asc)).collect(),
                input: Box::new(plan),
            };
        }

        // Limit / offset
        if let Some(n) = sel.limit {
            plan = LogicalPlan::Limit {
                n,
                offset: sel.offset.unwrap_or(0),
                input:  Box::new(plan),
            };
        }

        Ok(plan)
    }

    pub fn to_physical(&self, logical: LogicalPlan) -> Result<PhysicalPlan> {
        let join_strategy = self.pick_join_strategy(&logical);
        Ok(PhysicalPlan { logical, join_strategy, batch_size: 8192 })
    }

    fn pick_join_strategy(&self, plan: &LogicalPlan) -> Option<JoinStrategy> {
        if let LogicalPlan::Join { ref left, ref right, .. } = plan {
            let left_table  = extract_scan_table(left);
            let right_table = extract_scan_table(right);
            let left_rows   = left_table.and_then(|t| self.catalog.get_table(t).ok())
                .map(|d| d.stats.iter().map(|s| s.row_count).sum::<u64>())
                .unwrap_or(u64::MAX);
            let right_rows  = right_table.and_then(|t| self.catalog.get_table(t).ok())
                .map(|d| d.stats.iter().map(|s| s.row_count).sum::<u64>())
                .unwrap_or(u64::MAX);
            return Some(choose_join_strategy(left_rows, right_rows));
        }
        None
    }
}

fn extract_scan_table(plan: &LogicalPlan) -> Option<&str> {
    match plan {
        LogicalPlan::Scan(s)                 => Some(&s.table),
        LogicalPlan::Filter { input, .. }    => extract_scan_table(input),
        LogicalPlan::Project { input, .. }   => extract_scan_table(input),
        _ => None,
    }
}

fn extract_aggregates(projections: &[Projection]) -> (Vec<AggExpr>, Vec<String>) {
    let agg_fns = ["count", "sum", "avg", "min", "max"];
    let mut aggs  = Vec::new();
    let mut plain = Vec::new();
    for p in projections {
        match p {
            Projection::Expr { expr: Expr::Function { name, args }, alias } => {
                let lname = name.to_ascii_lowercase();
                if agg_fns.contains(&lname.as_str()) {
                    let out_alias = alias.clone().unwrap_or_else(|| name.clone());
                    aggs.push(AggExpr {
                        func:  lname,
                        arg:   args.first().cloned(),
                        alias: out_alias,
                    });
                }
            }
            Projection::Expr { expr: Expr::Column { name, .. }, alias: None } => {
                plain.push(name.clone());
            }
            _ => {}
        }
    }
    (aggs, plain)
}
