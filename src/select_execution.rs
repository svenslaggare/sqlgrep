use std::collections::HashMap;

use crate::model::{SelectStatement, ExpressionTree, ArithmeticOperator, Value, ValueType, CompareOperator};
use crate::execution_model::{ColumnProvider, ExecutionResult, ResultRow, HashMapColumnProvider, ExecutionError};
use crate::expression_execution::{ExpressionExecutionEngine};
use crate::data_model::{Row, TableDefinition, ColumnDefinition, Tables};

pub struct SelectExecutionEngine<'a>{
    tables: &'a Tables
}

impl<'a> SelectExecutionEngine<'a>  {
    pub fn new(tables: &'a Tables) -> SelectExecutionEngine<'a> {
        SelectExecutionEngine {
            tables
        }
    }

    pub fn execute<TColumnProvider: ColumnProvider>(&self, select_statement: &SelectStatement, row: TColumnProvider) -> ExecutionResult<Option<ResultRow>> {
        let expression_execution_engine = ExpressionExecutionEngine::new(&row);

        let valid = if let Some(filter) = select_statement.filter.as_ref() {
            expression_execution_engine.evaluate(filter)?.bool()
        } else {
            true
        };

        if valid {
            let mut column_names = Vec::new();
            let mut result_columns = Vec::new();

            if select_statement.is_wildcard_projection() {
                let table_definition = self.tables.get(&select_statement.from)
                    .ok_or_else(|| ExecutionError::TableNotFound(select_statement.from.clone()))?;

                for column in &table_definition.columns {
                    column_names.push(column.name.clone());
                    result_columns.push(expression_execution_engine.evaluate(&ExpressionTree::ColumnAccess(column.name.clone()))?);
                }
            } else {
                column_names = select_statement.projections.iter().map(|projection| projection.0.clone()).collect();

                for (_, projection) in &select_statement.projections {
                    result_columns.push(expression_execution_engine.evaluate(projection)?);
                }
            }

            Ok(Some(
                ResultRow {
                    data: vec![Row::new(result_columns)],
                    columns: column_names
                }
            ))
        } else {
            Ok(None)
        }
    }
}

#[test]
fn test_project1() {
    let table_definition = TableDefinition::new("test", Vec::new(), Vec::new()).unwrap();
    let tables = Tables::with_tables(vec![table_definition]);
    let select_execution_engine = SelectExecutionEngine::new(&tables);

    let column_values = vec![
        Value::Int(1337)
    ];

    let mut columns = HashMap::new();
    columns.insert("x", &column_values[0]);

    let result = select_execution_engine.execute(
        &SelectStatement {
            projections: vec![("p0".to_owned(), ExpressionTree::ColumnAccess("x".to_owned()))],
            from: "test".to_owned(),
            filename: None,
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
    let table_definition = TableDefinition::new("test", Vec::new(), Vec::new()).unwrap();
    let tables = Tables::with_tables(vec![table_definition]);
    let select_execution_engine = SelectExecutionEngine::new(&tables);

    let column_values = vec![
        Value::Int(1000)
    ];

    let mut columns = HashMap::new();
    columns.insert("x", &column_values[0]);

    let result = select_execution_engine.execute(
        &SelectStatement {
            projections: vec![
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
            filename: None,
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
    let table_definition = TableDefinition::new("test", Vec::new(), Vec::new()).unwrap();
    let tables = Tables::with_tables(vec![table_definition]);
    let select_execution_engine = SelectExecutionEngine::new(&tables);

    let column_values = vec![
        Value::Int(1000)
    ];

    let mut columns = HashMap::new();
    columns.insert("x", &column_values[0]);

    let result = select_execution_engine.execute(
        &SelectStatement {
            projections: vec![
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
            filename: None,
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
fn test_project4() {
    let table_definition = TableDefinition::new(
        "test",
        Vec::new(),
        vec![
            ColumnDefinition::with_regex("", 0, "x", ValueType::Int),
            ColumnDefinition::with_regex("", 0, "y", ValueType::Bool),
        ]
    ).unwrap();

    let tables = Tables::with_tables(vec![table_definition]);
    let select_execution_engine = SelectExecutionEngine::new(&tables);

    let column_values = vec![
        Value::Int(1000),
        Value::Bool(false),
    ];

    let mut columns = HashMap::new();
    columns.insert("x", &column_values[0]);
    columns.insert("y", &column_values[1]);

    let result = select_execution_engine.execute(
        &SelectStatement {
            projections: vec![("p0".to_owned(), ExpressionTree::Wildcard)],
            from: "test".to_owned(),
            filename: None,
            filter: None,
        },
        HashMapColumnProvider::new(columns)
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(Value::Int(1000), result.data[0].columns[0]);
    assert_eq!("x".to_owned(), result.columns[0]);

    assert_eq!(Value::Bool(false), result.data[0].columns[1]);
    assert_eq!("y".to_owned(), result.columns[1]);
}

#[test]
fn test_filter1() {
    let table_definition = TableDefinition::new("test", Vec::new(), Vec::new()).unwrap();
    let tables = Tables::with_tables(vec![table_definition]);
    let select_execution_engine = SelectExecutionEngine::new(&tables);

    let column_values = vec![
        Value::Int(1337)
    ];

    let mut columns = HashMap::new();
    columns.insert("x", &column_values[0]);

    let result = select_execution_engine.execute(
        &SelectStatement {
            projections: vec![("p0".to_owned(), ExpressionTree::ColumnAccess("x".to_owned()))],
            from: "test".to_owned(),
            filename: None,
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
    let table_definition = TableDefinition::new("test", Vec::new(), Vec::new()).unwrap();
    let tables = Tables::with_tables(vec![table_definition]);
    let select_execution_engine = SelectExecutionEngine::new(&tables);

    let column_values = vec![
        Value::Int(1337)
    ];

    let mut columns = HashMap::new();
    columns.insert("x", &column_values[0]);

    let result = select_execution_engine.execute(
        &SelectStatement {
            projections: vec![("p0".to_owned(), ExpressionTree::ColumnAccess("x".to_owned()))],
            from: "test".to_owned(),
            filename: None,
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