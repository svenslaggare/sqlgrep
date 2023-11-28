pub mod execution_engine;

mod expression_execution;
mod select_execution;
mod aggregate_execution;
mod join;
mod helpers;
mod column_providers;

#[cfg(test)]
mod aggregate_execution_tests;

use std::hash::{Hash, Hasher};
use fnv::FnvHasher;

use crate::data_model::{Row, TableDefinition};
use crate::execution::expression_execution::EvaluationError;
use crate::model::{ExpressionTree, Value};

#[derive(Debug, PartialEq)]
pub enum ExecutionError {
    Expression(EvaluationError),
    InternalError,
    TableNotFound(String),
    ColumnNotFound(String),
    GroupKeyNotAvailable(Option<String>),
    ExpectedNumericValue,
    NotSupportedOperation,
    JoinNotSupported,
    FailOpenFile(String),
    CannotCreateArrayOfNullType,
    DistinctRequiresColumn
}

impl From<EvaluationError> for ExecutionError {
    fn from(error: EvaluationError) -> Self {
        ExecutionError::Expression(error)
    }
}

impl std::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionError::Expression(expression) => { write!(f, "{}", expression) }
            ExecutionError::InternalError => { write!(f, "Internal error") }
            ExecutionError::TableNotFound(name) => { write!(f, "Table '{}' not found", name) }
            ExecutionError::ColumnNotFound(name) => { write!(f, "Column '{}' not found", name) }
            ExecutionError::GroupKeyNotAvailable(name) => {
                match name {
                    None => {
                        write!(f, "Column names can only be used with group by clause")
                    }
                    Some(name) => {
                        write!(f, "The expression '{}' is not used in group by clause", name)
                    }
                }
            }
            ExecutionError::ExpectedNumericValue => { write!(f, "Expected a numeric value") }
            ExecutionError::NotSupportedOperation => { write!(f, "Not a supported operation") }
            ExecutionError::JoinNotSupported => { write!(f, "Join clause not supported") },
            ExecutionError::FailOpenFile(err) => { write!(f, "Failed open file due to: {}", err) },
            ExecutionError::CannotCreateArrayOfNullType => { write!(f, "Cannot create array of null type") },
            ExecutionError::DistinctRequiresColumn => { write!(f, "COUNT(DISTINCT) requires a column") },
        }
    }
}

pub type ExecutionResult<T> = Result<T, ExecutionError>;

pub trait ColumnProvider {
    fn get(&self, name: &str) -> Option<&Value>;
    fn exist(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    fn add_key(&mut self, key: &str);
    fn keys(&self) -> &Vec<String>;

    fn add_keys_for_table(&mut self, table: &TableDefinition) {
        for column in &table.columns {
            self.add_key(&column.name);
        }
    }
}

#[derive(Debug)]
pub struct ResultRow {
    pub data: Vec<Row>,
    pub columns: Vec<String>
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ExpressionTreeHash(u64);

impl ExpressionTreeHash {
    pub fn new(expression_tree: &ExpressionTree) -> ExpressionTreeHash {
        let mut hasher = FnvHasher::default();
        expression_tree.hash(&mut hasher);
        let hash = hasher.finish();
        ExpressionTreeHash(hash)
    }
}

impl std::fmt::Display for ExpressionTreeHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}