use std::collections::{HashMap, BTreeMap};

use crate::model::{Value, Aggregate, AggregateStatement, ValueType, ExpressionTree, CompareOperator};
use crate::data_model::{Row};
use crate::execution_model::{ColumnProvider, ExecutionResult, ExecutionError, ResultRow, HashMapColumnProvider};
use crate::expression_execution::ExpressionExecutionEngine;

type GroupKey = Vec<Value>;
type Groups<T> = BTreeMap<GroupKey, HashMap<usize, T>>;

pub struct AggregateExecutionEngine {
    groups: Groups<Value>,
    summary_statistics: Groups<(Value, i64)>
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
        if !self.execute_update(aggregate_statement, row)? {
            return Ok(None);
        }

        self.execute_result(aggregate_statement).map(|result| Some(result))
    }

    pub fn execute_update<TColumnProvider: ColumnProvider>(&mut self,
                                                           aggregate_statement: &AggregateStatement,
                                                           row: TColumnProvider) -> ExecutionResult<bool> {
        let expression_execution_engine = ExpressionExecutionEngine::new(&row);

        let valid = if let Some(filter) = aggregate_statement.filter.as_ref() {
            expression_execution_engine.evaluate(filter)?.bool()
        } else {
            true
        };

        if !valid {
            return Ok(false);
        }

        self.update_aggregates(aggregate_statement, &row, &expression_execution_engine)?;

        Ok(true)
    }

    fn update_aggregates<TColumnProvider: ColumnProvider>(&mut self,
                                                          aggregate_statement: &AggregateStatement,
                                                          row: &TColumnProvider,
                                                          expression_execution_engine: &ExpressionExecutionEngine<TColumnProvider>) -> ExecutionResult<()> {
        let group_key = if let Some(group_by) = aggregate_statement.group_by.as_ref() {
            let mut group_key = Vec::new();
            for column in group_by {
                group_key.push(row.get(column).map(|x| x.clone()).ok_or(ExecutionError::ColumnNotFound)?);
            }

            group_key
        } else {
            vec![Value::Null]
        };

        for (aggregate_index, aggregate) in aggregate_statement.aggregates.iter().enumerate() {
            match &aggregate.1 {
                Aggregate::GroupKey(group_column) => {
                    match aggregate_statement.group_by.as_ref() {
                        None => { return Err(ExecutionError::GroupKeyNotAvailable); }
                        Some(group_by) => {
                            if !group_by.iter().any(|column| column == group_column) {
                                return Err(ExecutionError::GroupKeyNotAvailable);
                            }
                        }
                    }
                }
                Aggregate::Count => {
                    self.get_group(group_key.clone(), aggregate_index, Value::Int(0)).modify(
                        |group_value| { *group_value += 1; },
                        |_| {},
                        |_| {},
                        |_| {}
                    );
                }
                Aggregate::Min(ref expression) => {
                    let column_value = expression_execution_engine.evaluate(expression)?;
                    let group_value = self.get_group(group_key.clone(), aggregate_index, column_value.clone());
                    group_value.modify_same_type(
                        &column_value,
                        |x, y| { *x = (*x).min(y) },
                        |x, y| { *x = (*x).min(y) },
                        |_, _| {},
                        |_, _| {}
                    );
                }
                Aggregate::Max(ref expression) => {
                    let column_value = expression_execution_engine.evaluate(expression)?;
                    let group_value = self.get_group(group_key.clone(), aggregate_index, column_value.clone());
                    group_value.modify_same_type(
                        &column_value,
                        |x, y| { *x = (*x).max(y) },
                        |x, y| { *x = (*x).max(y) },
                        |_, _| {},
                        |_, _| {}
                    );
                }
                Aggregate::Average(ref expression) => {
                    let column_value = expression_execution_engine.evaluate(expression)?;
                    let value_type = column_value.value_type().ok_or(ExecutionError::ExpectedNumericValue)?;

                    let average_entry = self.get_summary_group(group_key.clone(), aggregate_index, value_type);
                    average_entry.0.modify_same_type(
                        &column_value,
                        |x, y| { *x += y },
                        |x, y| { *x += y },
                        |_, _| {},
                        |_, _| {}
                    );
                    average_entry.1 += 1;

                    let average = average_entry.0.map(
                        || None,
                        |x| Some(x / average_entry.1),
                        |x| Some(x / average_entry.1 as f64),
                        |_| None,
                        |_| None
                    );

                    if let Some(average) = average {
                        *self.get_group(group_key.clone(), aggregate_index, average.clone()) = average.clone();
                    }
                }
                Aggregate::Sum(ref expression) => {
                    let column_value = expression_execution_engine.evaluate(expression)?;
                    let value_type = column_value.value_type().ok_or(ExecutionError::ExpectedNumericValue)?;

                    let sum_entry = self.get_summary_group(group_key.clone(), aggregate_index, value_type);
                    sum_entry.0.modify_same_type(
                        &column_value,
                        |x, y| { *x += y },
                        |x, y| { *x += y },
                        |_, _| {},
                        |_, _| {}
                    );
                    sum_entry.1 += 1;

                    let sum = sum_entry.0.clone();
                    *self.get_group(group_key.clone(), aggregate_index, sum.clone()) = sum.clone();
                }
            }
        }

        Ok(())
    }

    fn get_group(&mut self, group_key: GroupKey, aggregate_index: usize, default_value: Value) -> &mut Value {
        AggregateExecutionEngine::get_generic_group(
            &mut self.groups,
            group_key,
            aggregate_index,
            default_value
        )
    }

    fn get_summary_group(&mut self, group_key: GroupKey, aggregate_index: usize, value_type: ValueType) -> &mut (Value, i64) {
        let default_value = value_type.default_value();
        AggregateExecutionEngine::get_generic_group(
            &mut self.summary_statistics,
            group_key,
            aggregate_index,
            (default_value, 0)
        )
    }

    fn get_generic_group<T>(groups: &mut BTreeMap<GroupKey, HashMap<usize, T>>,
                            group_key: GroupKey,
                            aggregate_index: usize,
                            default_value: T) -> &mut T {
        groups.entry(group_key).or_insert_with(|| HashMap::new()).entry(aggregate_index).or_insert(default_value)
    }

    pub fn execute_result(&self, aggregate_statement: &AggregateStatement) -> ExecutionResult<ResultRow> {
        let mut result_rows_by_column = Vec::new();
        let mut columns = Vec::new();

        let mut group_key_mapping = HashMap::new();
        if let Some(group_key) = aggregate_statement.group_by.as_ref() {
            for (index, column) in group_key.iter().enumerate() {
                group_key_mapping.insert(column, index);
            }
        }

        for (aggregate_index, aggregate) in aggregate_statement.aggregates.iter().enumerate() {
            columns.push(aggregate.0.clone());
            result_rows_by_column.push(Vec::new());

            match aggregate.1 {
                Aggregate::GroupKey(ref column) => {
                    for group_key in self.groups.keys() {
                        result_rows_by_column.last_mut().unwrap().push(group_key[group_key_mapping[&column]].clone());
                    }
                }
                Aggregate::Count | Aggregate::Max(_) | Aggregate::Min(_) | Aggregate::Average(_) | Aggregate::Sum(_) => {
                    for subgroups in self.groups.values() {
                        if let Some(group_value) = subgroups.get(&aggregate_index) {
                            result_rows_by_column.last_mut().unwrap().push(group_value.clone());
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

            result_rows.push(Row::new(result_columns));
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
            ("x".to_owned(), Aggregate::GroupKey("x".to_owned())),
            ("count".to_owned(), Aggregate::Count)
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec!["x".to_string()])
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
            ("x".to_owned(), Aggregate::GroupKey("x".to_owned())),
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
        group_by: Some(vec!["x".to_string()])
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
            ("name".to_owned(), Aggregate::GroupKey("name".to_owned())),
            ("max".to_owned(), Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())))
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec!["name".to_string()])
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
fn test_group_by_and_min() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            ("name".to_owned(), Aggregate::GroupKey("name".to_owned())),
            ("max".to_owned(), Aggregate::Min(ExpressionTree::ColumnAccess("x".to_owned())))
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec!["name".to_string()])
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
    assert_eq!(Value::Int(1000), result.data[0].columns[1]);

    assert_eq!(Value::String("test2".to_owned()), result.data[1].columns[0]);
    assert_eq!(Value::Int(0), result.data[1].columns[1]);
}

#[test]
fn test_group_by_and_count_and_max() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            ("name".to_owned(), Aggregate::GroupKey("name".to_owned())),
            ("count".to_owned(), Aggregate::Count),
            ("max".to_owned(), Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())))
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec!["name".to_string()])
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
            ("name".to_owned(), Aggregate::GroupKey("name".to_owned())),
            ("max".to_owned(), Aggregate::Average(ExpressionTree::ColumnAccess("x".to_owned())))
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec!["name".to_string()])
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
            ("name".to_owned(), Aggregate::GroupKey("name".to_owned())),
            ("max".to_owned(), Aggregate::Sum(ExpressionTree::ColumnAccess("x".to_owned())))
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec!["name".to_string()])
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
