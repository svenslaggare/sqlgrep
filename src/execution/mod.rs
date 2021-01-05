pub mod expression_execution;
pub mod select_execution;
pub mod aggregate_execution;
pub mod execution_engine;

use std::collections::HashMap;

use crate::data_model::Row;
use crate::execution::expression_execution::EvaluationError;
use crate::model::Value;

pub trait ColumnProvider {
    fn get(&self, name: &str) -> Option<&Value>;
}

pub struct HashMapColumnProvider<'a> {
    columns: HashMap<&'a str, &'a Value>
}

impl<'a> HashMapColumnProvider<'a> {
    pub fn new(columns: HashMap<&'a str, &'a Value>) -> HashMapColumnProvider<'a> {
        HashMapColumnProvider {
            columns
        }
    }
}

impl<'a> ColumnProvider for HashMapColumnProvider<'a> {
    fn get(&self, name: &str) -> Option<&Value> {
        self.columns.get(name).map(|x| *x)
    }
}

pub struct HashMapOwnedKeyColumnProvider<'a> {
    columns: HashMap<String, &'a Value>
}

impl<'a> HashMapOwnedKeyColumnProvider<'a> {
    pub fn new(columns: HashMap<String, &'a Value>) -> HashMapOwnedKeyColumnProvider<'a> {
        HashMapOwnedKeyColumnProvider {
            columns
        }
    }
}

impl<'a> ColumnProvider for HashMapOwnedKeyColumnProvider<'a> {
    fn get(&self, name: &str) -> Option<&Value> {
        self.columns.get(name).map(|x| *x)
    }
}

#[derive(Debug, PartialEq)]
pub enum ExecutionError {
    Expression(EvaluationError),
    TableNotFound(String),
    ColumnNotFound(String),
    GroupKeyNotAvailable(Option<String>),
    ExpectedNumericValue,
    NotSupportedOperation
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
            ExecutionError::TableNotFound(name) => { write!(f, "Table '{}' not found", name) }
            ExecutionError::ColumnNotFound(name) => { write!(f, "Column '{}' not found", name) }
            ExecutionError::GroupKeyNotAvailable(name) => {
                match name {
                    None => {
                        write!(f, "Column names can only be used with group by clause")
                    }
                    Some(name) => {
                        write!(f, "The column '{}' is not used in group by", name)
                    }
                }
            }
            ExecutionError::ExpectedNumericValue => { write!(f, "Expected a numeric value") }
            ExecutionError::NotSupportedOperation => { write!(f, "Not a supported operation") }
        }
    }
}

pub type ExecutionResult<T> = Result<T, ExecutionError>;

pub struct ResultRow {
    pub data: Vec<Row>,
    pub columns: Vec<String>
}
