//! Query optimizer — rule-based rewrites followed by lightweight cost estimates.

pub mod cost;
pub mod rules;

use std::sync::Arc;

use crate::{
    catalog::Catalog,
    error::Result,
    planner::LogicalPlan,
};

pub struct Optimizer {
    catalog: Arc<Catalog>,
}

impl Optimizer {
    pub fn new(catalog: Arc<Catalog>) -> Self { Self { catalog } }

    pub fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let plan = rules::fold_constants(plan);
        let plan = rules::predicate_pushdown(plan);
        let plan = rules::projection_pushdown(plan);
        let plan = rules::partition_prune(plan, &self.catalog);
        Ok(plan)
    }
}
