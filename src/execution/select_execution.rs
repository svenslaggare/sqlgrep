use crate::execution::{ColumnProvider, ExecutionResult, ResultRow};
use crate::execution::expression_execution::ExpressionExecutionEngine;
use crate::model::{ExpressionTree, SelectStatement};
use crate::data_model::{Row};

pub struct SelectExecutionEngine {

}

impl SelectExecutionEngine  {
    pub fn new() -> SelectExecutionEngine {
        SelectExecutionEngine {

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
                for name in row.keys() {
                    column_names.push(name.clone());
                    result_columns.push(expression_execution_engine.evaluate(&ExpressionTree::ColumnAccess(name.clone()))?);
                }
            } else {
                column_names = select_statement.projections.iter().map(|projection| projection.0.clone()).collect();

                for (_, projection) in &select_statement.projections {
                    result_columns.push(expression_execution_engine.evaluate(projection)?);
                }
            }

            Ok(
                Some(
                    ResultRow {
                        data: vec![Row::new(result_columns)],
                        columns: column_names
                    }
                )
            )
        } else {
            Ok(None)
        }
    }
}

#[test]
fn test_project1() {
    use std::collections::HashMap;
    use crate::execution::HashMapColumnProvider;
    use crate::model::{ExpressionTree, SelectStatement, Value};

    let select_execution_engine = SelectExecutionEngine::new();

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
            join: None,
            limit: None
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
    use std::collections::HashMap;
    use crate::execution::HashMapColumnProvider;
    use crate::model::{ArithmeticOperator, ExpressionTree, SelectStatement, Value};

    let select_execution_engine = SelectExecutionEngine::new();

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
            join: None,
            limit: None
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
    use std::collections::HashMap;
    use crate::execution::HashMapColumnProvider;
    use crate::model::{ArithmeticOperator, ExpressionTree, SelectStatement, Value};

    let select_execution_engine = SelectExecutionEngine::new();

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
            join: None,
            limit: None
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
    use std::collections::HashMap;
    use crate::execution::HashMapColumnProvider;
    use crate::model::{ExpressionTree, SelectStatement, Value, ValueType};
    use crate::data_model::{TableDefinition, Tables, ColumnDefinition};

    let table_definition = TableDefinition::new(
        "test",
        Vec::new(),
        vec![
            ColumnDefinition::with_regex("", 0, "x", ValueType::Int),
            ColumnDefinition::with_regex("", 0, "y", ValueType::Bool),
        ]
    ).unwrap();

    let tables = Tables::with_tables(vec![table_definition]);
    let select_execution_engine = SelectExecutionEngine::new();

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
            join: None,
            limit: None
        },
        HashMapColumnProvider::with_table_keys(columns, tables.get("test").unwrap())
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
    use std::collections::HashMap;
    use crate::execution::HashMapColumnProvider;
    use crate::model::{CompareOperator, ExpressionTree, SelectStatement, Value};

    let select_execution_engine = SelectExecutionEngine::new();

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
            join: None,
            limit: None
        },
        HashMapColumnProvider::new(columns)
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_none());
}

#[test]
fn test_filter2() {
    use std::collections::HashMap;
    use crate::execution::HashMapColumnProvider;
    use crate::model::{CompareOperator, ExpressionTree, SelectStatement, Value};

    let select_execution_engine = SelectExecutionEngine::new();

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
            join: None,
            limit: None
        },
        HashMapColumnProvider::new(columns)
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(Value::Int(1337), result.data[0].columns[0]);
}