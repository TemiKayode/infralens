//! InfraLens Query Engine — IQL parser, optimizer, and vectorised executor.

pub mod ast;
pub mod catalog;
pub mod error;
pub mod executor;
pub mod functions;
pub mod lexer;
pub mod optimizer;
pub mod parser;
pub mod planner;

use std::sync::Arc;

use infralens_storage::engine::StorageEngine;

pub use error::{QueryError, Result};

/// End-to-end query execution: text → result batches.
pub async fn execute(
    sql:     &str,
    storage: Arc<StorageEngine>,
    catalog: Arc<catalog::Catalog>,
) -> Result<Vec<arrow::record_batch::RecordBatch>> {
    // 1. Lex
    let tokens = lexer::Lexer::new(sql).tokenise()?;

    // 2. Parse
    let ast = parser::Parser::new(tokens).parse_statement()?;

    // 3. Plan
    let planner  = planner::Planner::new(Arc::clone(&catalog));
    let logical  = planner.plan_statement(ast)?;

    // 4. Optimize
    let optimizer = optimizer::Optimizer::new(Arc::clone(&catalog));
    let logical   = optimizer.optimize(logical)?;

    // 5. Build physical plan
    let physical  = planner.to_physical(logical)?;

    // 6. Execute
    let mut op    = executor::build(physical, storage)?;
    let mut batches = Vec::new();
    while let Some(batch) = op.poll_next()? {
        batches.push(batch);
    }

    Ok(batches)
}
