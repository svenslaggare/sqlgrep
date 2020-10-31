use std::collections::{HashMap, BTreeMap};

use crate::model::{Value, Aggregate, AggregateStatement, ExpressionTree, CompareOperator};
use crate::data_model::{Row};
use crate::execution_model::{ColumnProvider, ExecutionResult, ExecutionError, ResultRow, HashMapColumnProvider};
use crate::expression_execution::ExpressionExecutionEngine;

pub struct AggregateExecutionEngine {
    groups: BTreeMap<Value, HashMap<usize, i64>>,
    summary_statistics: BTreeMap<Value, HashMap<usize, (i64, i64)>>
}

impl AggregateExecutionEngine {
    pub fn new() -> AggregateExecutionEngine {
        AggregateExecutionEngine {
            groups: BTreeMap::new(),
            summary_statistics: BTreeMap::new()
        }
    }

    pub fn execute<TColumnProvider: ColumnProvider>(&mut self,
                                                    aggregate_statement: &AggregateStatement,
                                                    row: TColumnProvider) -> ExecutionResult<Option<ResultRow>> {
        let expression_execution_engine = ExpressionExecutionEngine::new(&row);

        let valid = if let Some(filter) = aggregate_statement.filter.as_ref() {
            expression_execution_engine.evaluate(filter)?.bool()
        } else {
            true
        };

        if !valid {
            return Ok(None);
        }

        self.update_aggregates(aggregate_statement, &row, &expression_execution_engine)?;
        self.execute_result_only(aggregate_statement).map(|result| Some(result))
    }

    fn update_aggregates<TColumnProvider: ColumnProvider>(&mut self,
                                                          aggregate_statement: &AggregateStatement,
                                                          row: &TColumnProvider,
                                                          expression_execution_engine: &ExpressionExecutionEngine<TColumnProvider>) -> ExecutionResult<()> {
        let group = if let Some(group_by) = aggregate_statement.group_by.as_ref() {
            row.get(group_by).map(|x| x.clone()).ok_or(ExecutionError::ColumnNotFound)?
        } else {
            Value::Null
        };

        for (aggregate_index, aggregate) in aggregate_statement.aggregates.iter().enumerate() {
            match aggregate.1 {
                Aggregate::GroupKey => {
                    if aggregate_statement.group_by.is_none() {
                        return Err(ExecutionError::GroupKeyNotAvailable);
                    }
                }
                Aggregate::Count => {
                    *self.groups.entry(group.clone()).or_insert_with(|| HashMap::new()).entry(aggregate_index).or_insert(0) += 1;
                }
                Aggregate::Min(ref expression) => {
                    let column_value = expression_execution_engine.evaluate(expression)?.int().ok_or(ExecutionError::ExpectedNumericValue)?;
                    let group_value = self.get_group(group.clone(), aggregate_index, column_value);
                    *group_value = (*group_value).min(column_value);
                }
                Aggregate::Max(ref expression) => {
                    let column_value = expression_execution_engine.evaluate(expression)?.int().ok_or(ExecutionError::ExpectedNumericValue)?;
                    let group_value = self.get_group(group.clone(), aggregate_index, column_value);
                    *group_value = (*group_value).max(column_value);
                }
                Aggregate::Average(ref expression) => {
                    let column_value = expression_execution_engine.evaluate(expression)?.int().ok_or(ExecutionError::ExpectedNumericValue)?;
                    let average_entry = self.get_summary_group(group.clone(), aggregate_index);
                    average_entry.0 += column_value;
                    average_entry.1 += 1;

                    let average = average_entry.0 / average_entry.1;

                    let group_value = self.get_group(group.clone(), aggregate_index, average);
                    *group_value = average;
                }
                Aggregate::Sum(ref expression) => {
                    let column_value = expression_execution_engine.evaluate(expression)?.int().ok_or(ExecutionError::ExpectedNumericValue)?;
                    let sum_entry = self.get_summary_group(group.clone(), aggregate_index);
                    sum_entry.0 += column_value;
                    sum_entry.1 += 1;

                    let sum = sum_entry.0;

                    let group_value = self.get_group(group.clone(), aggregate_index, sum);
                    *group_value = sum;
                }
            }
        }

        Ok(())
    }

    fn get_group(&mut self, group: Value, aggregate_index: usize, default_value: i64) -> &mut i64 {
        self.groups.entry(group).or_insert_with(|| HashMap::new()).entry(aggregate_index).or_insert(default_value)
    }

    fn get_summary_group(&mut self, group: Value, aggregate_index: usize) -> &mut (i64, i64) {
        self.summary_statistics.entry(group.clone()).or_insert_with(|| HashMap::new()).entry(aggregate_index).or_insert((0, 0))
    }

    pub fn execute_result_only(&self, aggregate_statement: &AggregateStatement) -> ExecutionResult<ResultRow> {
        let mut result_rows_by_column = Vec::new();
        let mut columns = Vec::new();

        for (aggregate_index, aggregate) in aggregate_statement.aggregates.iter().enumerate() {
            columns.push(aggregate.0.clone());
            result_rows_by_column.push(Vec::new());

            match aggregate.1 {
                Aggregate::GroupKey => {
                    for group_key in self.groups.keys() {
                        result_rows_by_column.last_mut().unwrap().push(group_key.clone());
                    }
                }
                Aggregate::Count | Aggregate::Max(_) | Aggregate::Min(_) | Aggregate::Average(_) | Aggregate::Sum(_) => {
                    for subgroups in self.groups.values() {
                        if let Some(group_value) = subgroups.get(&aggregate_index) {
                            result_rows_by_column.last_mut().unwrap().push(Value::Int(*group_value));
                        }
                    }
                }
            }
        }

        let num_columns = result_rows_by_column.len();
        let num_rows = result_rows_by_column[0].len();

        let mut result_rows = Vec::new();
        for row_index in 0..num_rows {
            let mut result_columns = Vec::new();
            for column_index in 0..num_columns {
                result_columns.push(result_rows_by_column[column_index][row_index].clone());
            }

            result_rows.push(Row { columns: result_columns });
        }

        Ok(
            ResultRow {
                data: result_rows,
                columns
            }
        )
    }
}

fn create_test_columns<'a>(names: Vec<&'a str>, values: &'a Vec<Value>) -> HashMap<&'a str, &'a Value> {
    let mut columns = HashMap::new();
    for (name, value) in names.into_iter().zip(values.iter()) {
        columns.insert(name, value);
    }

    columns
}

#[test]
fn test_group_by_and_count() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            ("x".to_owned(), Aggregate::GroupKey),
            ("count".to_owned(), Aggregate::Count)
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some("x".to_string())
    };

    let column_values = vec![Value::Int(1000)];
    let columns = create_test_columns(vec!["x"], &column_values);

    for _ in 0..5 {
        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone())
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(columns.clone())
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(1, result.data.len());
    assert_eq!(Value::Int(1000), result.data[0].columns[0]);
    assert_eq!(Value::Int(6), result.data[0].columns[1]);

    // Add another group
    let column_values = vec![Value::Int(2000)];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::Int(1000), result.data[0].columns[0]);
    assert_eq!(Value::Int(6), result.data[0].columns[1]);

    assert_eq!(Value::Int(2000), result.data[1].columns[0]);
    assert_eq!(Value::Int(1), result.data[1].columns[1]);
}

#[test]
fn test_group_by_and_count_and_filter() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            ("x".to_owned(), Aggregate::GroupKey),
            ("count".to_owned(), Aggregate::Count)
        ],
        from: "test".to_owned(),
        filename: None,
        filter: Some(
            ExpressionTree::Compare {
                left: Box::new(ExpressionTree::ColumnAccess("x".to_owned())),
                right: Box::new(ExpressionTree::Value(Value::Int(1500))),
                operator: CompareOperator::GreaterThan
            }
        ),
        group_by: Some("x".to_string())
    };

    let column_values = vec![Value::Int(1000)];
    let columns = create_test_columns(vec!["x"], &column_values);

    for _ in 0..5 {
        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone())
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_none());
    }

    // Add another group
    let column_values = vec![Value::Int(2000)];
    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(1, result.data.len());
    assert_eq!(Value::Int(2000), result.data[0].columns[0]);
    assert_eq!(Value::Int(1), result.data[0].columns[1]);
}

#[test]
fn test_group_by_and_max() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            ("name".to_owned(), Aggregate::GroupKey),
            ("max".to_owned(), Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())))
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some("name".to_string())
    };

    for i in 1..6 {
        let column_values = vec![
            Value::Int(i * 1000),
            Value::String("test".to_owned())
        ];

        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![
        Value::Int(0),
        Value::String("test2".to_owned())
    ];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::String("test".to_owned()), result.data[0].columns[0]);
    assert_eq!(Value::Int(5000), result.data[0].columns[1]);

    assert_eq!(Value::String("test2".to_owned()), result.data[1].columns[0]);
    assert_eq!(Value::Int(0), result.data[1].columns[1]);
}

#[test]
fn test_group_by_and_count_and_max() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            ("name".to_owned(), Aggregate::GroupKey),
            ("count".to_owned(), Aggregate::Count),
            ("max".to_owned(), Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())))
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some("name".to_string())
    };

    for i in 1..6 {
        let column_values = vec![
            Value::Int(i * 1000),
            Value::String("test".to_owned())
        ];

        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![
        Value::Int(0),
        Value::String("test2".to_owned())
    ];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::String("test".to_owned()), result.data[0].columns[0]);
    assert_eq!(Value::Int(5), result.data[0].columns[1]);
    assert_eq!(Value::Int(5000), result.data[0].columns[2]);

    assert_eq!(Value::String("test2".to_owned()), result.data[1].columns[0]);
    assert_eq!(Value::Int(1), result.data[1].columns[1]);
    assert_eq!(Value::Int(0), result.data[1].columns[2]);
}

#[test]
fn test_group_by_and_average() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            ("name".to_owned(), Aggregate::GroupKey),
            ("max".to_owned(), Aggregate::Average(ExpressionTree::ColumnAccess("x".to_owned())))
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some("name".to_string())
    };

    for i in 1..6 {
        let column_values = vec![
            Value::Int(i * 1000),
            Value::String("test".to_owned())
        ];

        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![
        Value::Int(1000),
        Value::String("test2".to_owned())
    ];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::String("test".to_owned()), result.data[0].columns[0]);
    assert_eq!(Value::Int(3000), result.data[0].columns[1]);

    assert_eq!(Value::String("test2".to_owned()), result.data[1].columns[0]);
    assert_eq!(Value::Int(1000), result.data[1].columns[1]);
}

#[test]
fn test_group_by_and_sum() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            ("name".to_owned(), Aggregate::GroupKey),
            ("max".to_owned(), Aggregate::Sum(ExpressionTree::ColumnAccess("x".to_owned())))
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some("name".to_string())
    };

    for i in 1..6 {
        let column_values = vec![
            Value::Int(i * 1000),
            Value::String("test".to_owned())
        ];

        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![
        Value::Int(1000),
        Value::String("test2".to_owned())
    ];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::String("test".to_owned()), result.data[0].columns[0]);
    assert_eq!(Value::Int(15000), result.data[0].columns[1]);

    assert_eq!(Value::String("test2".to_owned()), result.data[1].columns[0]);
    assert_eq!(Value::Int(1000), result.data[1].columns[1]);
}

#[test]
fn test_count() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            ("count".to_owned(), Aggregate::Count)
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: None
    };

    let column_values = vec![Value::Int(1000)];
    let columns = create_test_columns(vec!["x"], &column_values);

    for _ in 0..5 {
        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone())
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(columns.clone())
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(1, result.data.len());
    assert_eq!(Value::Int(6), result.data[0].columns[0]);
}
