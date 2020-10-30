use std::collections::HashMap;

use crate::model::Value;
use crate::expression_execution::EvaluationError;
use crate::data_model::Row;

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

#[derive(Debug, PartialEq)]
pub enum ExecutionError {
    Expression(EvaluationError),
    TableNotFound,
    ColumnNotFound,
    GroupKeyNotAvailable,
    ExpectedNumericValue
}

impl From<EvaluationError> for ExecutionError {
    fn from(error: EvaluationError) -> Self {
        ExecutionError::Expression(error)
    }
}

impl std::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub type ExecutionResult<T> = Result<T, ExecutionError>;

pub struct ResultRow {
    pub data: Vec<Row>,
    pub columns: Vec<String>
}