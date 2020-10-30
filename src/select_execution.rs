use std::collections::HashMap;
use std::marker::PhantomData;

use crate::model::{SelectStatement, Value, ExpressionTree, ArithmeticOperator, CompareOperator};
use crate::execution_model::{ColumnProvider, HashMapColumnProvider, ExecutionResult, ResultRow};
use crate::expression_execution::{ExpressionExecutionEngine};
use crate::data_model::Row;

pub struct SelectExecutionEngine<TColumnProvider> where TColumnProvider: ColumnProvider {
    _phantom: PhantomData<TColumnProvider>
}

impl<TColumnProvider> SelectExecutionEngine<TColumnProvider> where TColumnProvider: ColumnProvider {
    pub fn new() -> SelectExecutionEngine<TColumnProvider> {
        SelectExecutionEngine {
            _phantom: PhantomData::default()
        }
    }

    pub fn execute(&self, select_statement: &SelectStatement, row: TColumnProvider) -> ExecutionResult<Option<ResultRow>> {
        let expression_execution_engine = ExpressionExecutionEngine::new(&row);

        let valid = if let Some(filter) = select_statement.filter.as_ref() {
            expression_execution_engine.evaluate(filter)?.bool()
        } else {
            true
        };

        if valid {
            let mut result_columns = Vec::new();

            for (_, projection) in &select_statement.projection {
                result_columns.push(expression_execution_engine.evaluate(projection)?);
            }

            let result_row = ResultRow {
                data: vec![Row { columns: result_columns }],
                columns: select_statement.projection.iter().map(|projection| projection.0.clone()).collect()
            };

            Ok(Some(result_row))
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
            filter: None,
        },
        HashMapColumnProvider::new(columns)
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(Value::Int(1337), result.data[0].columns[0]);
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
            filter: None,
        },
        HashMapColumnProvider::new(columns)
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(Value::Int(2000), result.data[0].columns[0]);
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
            filter: None,
        },
        HashMapColumnProvider::new(columns)
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(Value::Int(1000), result.data[0].columns[0]);
    assert_eq!(Value::Int(2000), result.data[0].columns[1]);
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
            ),
        },
        HashMapColumnProvider::new(columns)
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
            ),
        },
        HashMapColumnProvider::new(columns)
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(Value::Int(1337), result.data[0].columns[0]);
}