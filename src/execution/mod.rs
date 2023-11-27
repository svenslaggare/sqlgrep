pub mod expression_execution;
pub mod select_execution;
pub mod aggregate_execution;
pub mod join;
pub mod execution_engine;

#[cfg(test)]
pub mod aggregate_execution_tests;

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use fnv::FnvHasher;

use crate::data_model::{Row, TableDefinition};
use crate::execution::expression_execution::EvaluationError;
use crate::model::{ExpressionTree, Value};

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

pub struct HashMapColumnProvider<'a> {
    columns: HashMap<&'a str, &'a Value>,
    keys: Vec<String>
}

impl<'a> HashMapColumnProvider<'a> {
    pub fn new(columns: HashMap<&'a str, &'a Value>) -> HashMapColumnProvider<'a> {
        HashMapColumnProvider {
            columns,
            keys: Vec::new()
        }
    }

    pub fn with_table_keys(columns: HashMap<&'a str, &'a Value>,
                           table: &TableDefinition) -> HashMapColumnProvider<'a> {
        let mut provider = HashMapColumnProvider {
            columns,
            keys: Vec::new()
        };

        provider.add_keys_for_table(table);
        provider
    }
}

impl<'a> ColumnProvider for HashMapColumnProvider<'a> {
    fn get(&self, name: &str) -> Option<&Value> {
        self.columns.get(name).map(|x| *x)
    }

    fn add_key(&mut self, key: &str) {
        self.keys.push(key.to_owned());
    }

    fn keys(&self) -> &Vec<String> {
        &self.keys
    }
}

pub struct HashMapOwnedKeyColumnProvider<'a> {
    columns: HashMap<String, &'a Value>,
    keys: Vec<String>
}

impl<'a> HashMapOwnedKeyColumnProvider<'a> {
    pub fn new(columns: HashMap<String, &'a Value>) -> HashMapOwnedKeyColumnProvider<'a> {
        HashMapOwnedKeyColumnProvider {
            columns,
            keys: Vec::new()
        }
    }
}

impl<'a> ColumnProvider for HashMapOwnedKeyColumnProvider<'a> {
    fn get(&self, name: &str) -> Option<&Value> {
        self.columns.get(name).map(|x| *x)
    }

    fn add_key(&mut self, key: &str) {
        self.keys.push(key.to_owned());
    }

    fn keys(&self) -> &Vec<String> {
        &self.keys
    }
}

pub struct SingleColumnProvider<'a> {
    empty_keys: Vec<String>,
    key: &'a str,
    value: &'a Value
}

impl<'a> SingleColumnProvider<'a> {
    pub fn new(key: &'a str, value: &'a Value) -> SingleColumnProvider<'a> {
        SingleColumnProvider {
            empty_keys: Vec::new(),
            key,
            value
        }
    }
}

impl<'a> ColumnProvider for SingleColumnProvider<'a> {
    fn get(&self, name: &str) -> Option<&Value> {
        if name == self.key {
            Some(self.value)
        } else {
            None
        }
    }

    fn add_key(&mut self, _: &str) {

    }

    fn keys(&self) -> &Vec<String> {
        &self.empty_keys
    }
}

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

#[derive(Debug)]
pub struct ResultRow {
    pub data: Vec<Row>,
    pub columns: Vec<String>
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExpressionTreeHash(u64);

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