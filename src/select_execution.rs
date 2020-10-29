use std::collections::HashMap;
use std::iter::FromIterator;
use std::marker::PhantomData;

use crate::model::{SelectStatement, Value, ExpressionTree, ArithmeticOperator, CompareOperator};
use crate::execution_model::{ColumnProvider, RowsProvider, HashMapRowsProvider, HashMapRefColumnProvider};
use crate::expression_execution::{ExpressionExecutionEngine, EvaluationError};
use crate::data_model::Row;

#[derive(Debug, PartialEq)]
pub enum SelectExecutionError {
    Expression(EvaluationError),
    TableNotFound,
    Generic
}

impl From<EvaluationError> for SelectExecutionError {
    fn from(error: EvaluationError) -> Self {
        SelectExecutionError::Expression(error)
    }
}

pub struct RowsResult {
    column_name_mapping: HashMap<String, usize>,
    rows: Vec<Row>
}

pub type SelectExecutionResult<T> = Result<T, SelectExecutionError>;

pub struct SelectExecutionEngine<TColumnProvider> where TColumnProvider: ColumnProvider {
    _phantom: PhantomData<TColumnProvider>
}

impl<TColumnProvider> SelectExecutionEngine<TColumnProvider> where TColumnProvider: ColumnProvider {
    pub fn new() -> SelectExecutionEngine<TColumnProvider> {
        SelectExecutionEngine {
            _phantom: PhantomData::default()
        }
    }

    pub fn execute(&self, select_statement: &SelectStatement, row: TColumnProvider) -> SelectExecutionResult<Option<Row>> {
        let expression_engine = ExpressionExecutionEngine::new(row);

        let valid = if let Some(filter) = select_statement.filter.as_ref() {
            expression_engine.evaluate(filter)?.bool()
        } else {
            true
        };

        if valid {
            let mut result_columns = Vec::new();

            for (_, projection) in &select_statement.projection {
                result_columns.push(expression_engine.evaluate(projection)?);
            }

            Ok(Some(Row { columns: result_columns }))
        } else {
            Ok(None)
        }
    }
}

#[test]
fn test_project1() {
    let select_execution_engine = SelectExecutionEngine::new();

    let column_values = vec![
        Value::Int(1337)
    ];

    let mut columns = HashMap::new();
    columns.insert("x", &column_values[0]);

    let result = select_execution_engine.execute(
        &SelectStatement {
            projection: vec![("p0".to_owned(), ExpressionTree::ColumnAccess("x".to_owned()))],
            from: "test".to_owned(),
            filter: None
        },
        HashMapRefColumnProvider::new(columns)
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(Value::Int(1337), result.columns[0]);
}

#[test]
fn test_project2() {
    let select_execution_engine = SelectExecutionEngine::new();

    let column_values = vec![
        Value::Int(1000)
    ];

    let mut columns = HashMap::new();
    columns.insert("x", &column_values[0]);

    let result = select_execution_engine.execute(
        &SelectStatement {
            projection: vec![
                (
                    "p0".to_owned(),
                    ExpressionTree::Arithmetic {
                        left: Box::new(ExpressionTree::ColumnAccess("x".to_owned())),
                        right: Box::new(ExpressionTree::Value(Value::Int(2))),
                        operator: ArithmeticOperator::Multiply
                    }
                )
            ],
            from: "test".to_owned(),
            filter: None
        },
        HashMapRefColumnProvider::new(columns)
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(Value::Int(2000), result.columns[0]);
}

#[test]
fn test_project3() {
    let select_execution_engine = SelectExecutionEngine::new();

    let column_values = vec![
        Value::Int(1000)
    ];

    let mut columns = HashMap::new();
    columns.insert("x", &column_values[0]);

    let result = select_execution_engine.execute(
        &SelectStatement {
            projection: vec![
                (
                    "p0".to_owned(),
                    ExpressionTree::ColumnAccess("x".to_owned())
                ),
                (
                    "p1".to_owned(),
                    ExpressionTree::Arithmetic {
                        left: Box::new(ExpressionTree::ColumnAccess("x".to_owned())),
                        right: Box::new(ExpressionTree::Value(Value::Int(2))),
                        operator: ArithmeticOperator::Multiply
                    }
                )
            ],
            from: "test".to_owned(),
            filter: None
        },
        HashMapRefColumnProvider::new(columns)
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(Value::Int(1000), result.columns[0]);
    assert_eq!(Value::Int(2000), result.columns[1]);
}

#[test]
fn test_filter1() {
    let select_execution_engine = SelectExecutionEngine::new();

    let column_values = vec![
        Value::Int(1337)
    ];

    let mut columns = HashMap::new();
    columns.insert("x", &column_values[0]);

    let result = select_execution_engine.execute(
        &SelectStatement {
            projection: vec![("p0".to_owned(), ExpressionTree::ColumnAccess("x".to_owned()))],
            from: "test".to_owned(),
            filter: Some(
                ExpressionTree::Compare {
                    left: Box::new(ExpressionTree::ColumnAccess("x".to_owned())),
                    right: Box::new(ExpressionTree::Value(Value::Int(2000))),
                    operator: CompareOperator::GreaterThan
                }
            )
        },
        HashMapRefColumnProvider::new(columns)
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_none());
}

#[test]
fn test_filter2() {
    let select_execution_engine = SelectExecutionEngine::new();

    let column_values = vec![
        Value::Int(1337)
    ];

    let mut columns = HashMap::new();
    columns.insert("x", &column_values[0]);

    let result = select_execution_engine.execute(
        &SelectStatement {
            projection: vec![("p0".to_owned(), ExpressionTree::ColumnAccess("x".to_owned()))],
            from: "test".to_owned(),
            filter: Some(
                ExpressionTree::Compare {
                    left: Box::new(ExpressionTree::ColumnAccess("x".to_owned())),
                    right: Box::new(ExpressionTree::Value(Value::Int(1000))),
                    operator: CompareOperator::GreaterThan
                }
            )
        },
        HashMapRefColumnProvider::new(columns)
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(Value::Int(1337), result.columns[0]);
}