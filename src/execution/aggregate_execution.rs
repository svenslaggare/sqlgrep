use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

use fnv::FnvHasher;

use crate::data_model::Row;
use crate::execution::{ColumnProvider, ExecutionError, ExecutionResult, HashMapColumnProvider, HashMapOwnedKeyColumnProvider, ResultRow};
use crate::execution::expression_execution::{ExpressionExecutionEngine, unique_values};
use crate::model::{Aggregate, AggregateStatement, CompareOperator, ExpressionTree, Value, ValueType};

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

    pub fn clear(&mut self) {
        self.groups.clear();
        self.summary_statistics.clear();
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
                group_key.push(row.get(column).map(|x| x.clone()).ok_or_else(|| ExecutionError::ColumnNotFound(column.clone()))?);
            }

            group_key
        } else {
            vec![Value::Null]
        };

        let validate_group_key = |group_column: &String| -> ExecutionResult<()> {
            match aggregate_statement.group_by.as_ref() {
                None => { return Err(ExecutionError::GroupKeyNotAvailable(None)); }
                Some(group_by) => {
                    if !group_by.iter().any(|column| column == group_column) {
                        return Err(ExecutionError::GroupKeyNotAvailable(Some(group_column.clone())));
                    }
                }
            }

            Ok(())
        };

        let mut update_aggregate = |aggregate_index: usize, aggregate: &Aggregate| -> ExecutionResult<()> {
            match aggregate {
                Aggregate::GroupKey(group_column) => {
                    validate_group_key(group_column)?;
                }
                Aggregate::Count(column) => {
                    let valid = if let Some(column) = column{
                        row.get(column).ok_or_else(|| ExecutionError::ColumnNotFound(column.clone()))?.is_not_null()
                    } else {
                        true
                    };

                    if valid {
                        self.get_group(group_key.clone(), aggregate_index, || Ok(Value::Int(0)))?.modify(
                            |group_value| { *group_value += 1; },
                            |_| {},
                            |_| {},
                            |_| {},
                            |_| {},
                            |_| {},
                        );
                    }
                }
                Aggregate::Min(ref expression) => {
                    let column_value = expression_execution_engine.evaluate(expression)?;
                    let group_value = self.get_group(group_key.clone(), aggregate_index, || Ok(column_value.clone()))?;
                    group_value.modify_same_type(
                        &column_value,
                        |x, y| { *x = (*x).min(y) },
                        |x, y| { *x = (*x).min(y) },
                        |_, _| {},
                        |_, _| {},
                        |_, _| {},
                        |_, _| {},
                    );
                }
                Aggregate::Max(ref expression) => {
                    let column_value = expression_execution_engine.evaluate(expression)?;
                    let group_value = self.get_group(group_key.clone(), aggregate_index, || Ok(column_value.clone()))?;
                    group_value.modify_same_type(
                        &column_value,
                        |x, y| { *x = (*x).max(y) },
                        |x, y| { *x = (*x).max(y) },
                        |_, _| {},
                        |_, _| {},
                        |_, _| {},
                        |_, _| {},
                    );
                }
                Aggregate::Average(ref expression) => {
                    let column_value = expression_execution_engine.evaluate(expression)?;
                    let value_type = column_value.value_type().ok_or(ExecutionError::ExpectedNumericValue)?;

                    let average_entry = self.get_summary_group(group_key.clone(), aggregate_index, value_type)?;
                    average_entry.0.modify_same_type(
                        &column_value,
                        |x, y| { *x += y },
                        |x, y| { *x += y },
                        |_, _| {},
                        |_, _| {},
                        |_, _| {},
                        |_, _| {},
                    );
                    average_entry.1 += 1;

                    let average = average_entry.0.map(
                        || None,
                        |x| Some(x / average_entry.1),
                        |x| Some(x / average_entry.1 as f64),
                        |_| None,
                        |_| None,
                        |_| None,
                        |_| None,
                    );

                    if let Some(average) = average {
                        *self.get_group(group_key.clone(), aggregate_index, || Ok(average.clone()))? = average.clone();
                    }
                }
                Aggregate::Sum(ref expression) => {
                    let column_value = expression_execution_engine.evaluate(expression)?;
                    let value_type = column_value.value_type().ok_or(ExecutionError::ExpectedNumericValue)?;

                    let sum_entry = self.get_summary_group(group_key.clone(), aggregate_index, value_type)?;
                    sum_entry.0.modify_same_type(
                        &column_value,
                        |x, y| { *x += y },
                        |x, y| { *x += y },
                        |_, _| {},
                        |_, _| {},
                        |_, _| {},
                        |_, _| {},
                    );
                    sum_entry.1 += 1;

                    let sum = sum_entry.0.clone();
                    *self.get_group(group_key.clone(), aggregate_index, || Ok(sum.clone()))? = sum.clone();
                }
                Aggregate::CollectArray(ref expression, _) => {
                    let column_value = expression_execution_engine.evaluate(expression)?;
                    let group_value = self.get_group(
                        group_key.clone(),
                        aggregate_index,
                        || {
                            let element_type = column_value.value_type().ok_or(ExecutionError::CannotCreateArrayOfNullType)?;
                            Ok(ValueType::Array(Box::new(element_type)).default_value())
                        }
                    )?;

                    group_value.modify(
                        |_| {},
                        |_| {},
                        |_| {},
                        |_| {},
                        |array| { array.push(column_value.clone()) },
                        |_| {},
                    );
                }
            }

            Ok(())
        };

        for (aggregate_index, aggregate) in aggregate_statement.aggregates.iter().enumerate() {
            update_aggregate(aggregate_index, &aggregate.1)?;
        }

        if let Some(having) = aggregate_statement.having.as_ref() {
            let mut having_aggregate_index = 0;
            having.visit(&mut |tree| {
                match tree {
                    ExpressionTree::Aggregate(_, aggregate) => {
                        match aggregate.as_ref() {
                            Aggregate::GroupKey(group_column) => {
                                validate_group_key(group_column)?;
                            }
                            aggregate => {
                                update_aggregate(aggregate_statement.aggregates.len() + having_aggregate_index, aggregate)?;
                                having_aggregate_index += 1;
                            }
                        }
                    },
                    _ => {}
                }

                Result::<(), ExecutionError>::Ok(())
            })?;
        }

        Ok(())
    }

    fn get_group<F: Fn() -> ExecutionResult<Value>>(&mut self, group_key: GroupKey, aggregate_index: usize, default_value_fn: F) -> ExecutionResult<&mut Value> {
        AggregateExecutionEngine::get_generic_group(
            &mut self.groups,
            group_key,
            aggregate_index,
            default_value_fn
        )
    }

    fn get_summary_group(&mut self, group_key: GroupKey, aggregate_index: usize, value_type: ValueType) -> ExecutionResult<&mut (Value, i64)> {
        AggregateExecutionEngine::get_generic_group(
            &mut self.summary_statistics,
            group_key,
            aggregate_index,
            || Ok((value_type.default_value(), 0))
        )
    }

    fn get_generic_group<T, F: Fn() -> ExecutionResult<T>>(groups: &mut BTreeMap<GroupKey, HashMap<usize, T>>,
                                                           group_key: GroupKey,
                                                           aggregate_index: usize,
                                                           default_value_fn: F) -> ExecutionResult<&mut T> {
        let group_map = groups.entry(group_key).or_insert_with(|| HashMap::new());
        if !group_map.contains_key(&aggregate_index) {
            group_map.insert(aggregate_index, default_value_fn()?);
        }

        group_map.get_mut(&aggregate_index).ok_or(ExecutionError::InternalError)
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
            let mut result_column = Vec::new();

            match aggregate.1 {
                Aggregate::GroupKey(ref column) => {
                    for group_key in self.groups.keys() {
                        result_column.push(group_key[group_key_mapping[&column]].clone());
                    }
                }
                Aggregate::Count(_) | Aggregate::Max(_) | Aggregate::Min(_) | Aggregate::Average(_) | Aggregate::Sum(_) => {
                    for subgroups in self.groups.values() {
                        if let Some(group_value) = subgroups.get(&aggregate_index) {
                            result_column.push(group_value.clone());
                        }
                    }
                }
                Aggregate::CollectArray(_, unique) => {
                    for subgroups in self.groups.values() {
                        if let Some(group_value) = subgroups.get(&aggregate_index) {
                            if unique {
                                let mut group_value = group_value.clone();
                                group_value.modify(
                                    |_| {},
                                    |_| {},
                                    |_| {},
                                    |_| {},
                                    |array| { unique_values(array); },
                                    |_| {},
                                );

                                result_column.push(group_value);
                            } else {
                                result_column.push(group_value.clone());
                            }
                        }
                    }
                }
            }

            result_rows_by_column.push(result_column);
        }

        let num_columns = result_rows_by_column.len();
        let num_rows = result_rows_by_column[0].len();

        let mut result_rows = Vec::new();
        let mut group_key_iterator = self.groups.keys();
        let mut group_value_iterator = self.groups.values();

        let having_aggregates = extract_having_aggregates(aggregate_statement)?;

        for row_index in 0..num_rows {
            let mut result_columns = Vec::new();
            for column_index in 0..num_columns {
                result_columns.push(result_rows_by_column[column_index][row_index].clone());
            }

            if let Some(having) = aggregate_statement.having.as_ref() {
                let group_key_value = group_key_iterator.next().unwrap();
                let group_value = group_value_iterator.next().unwrap();

                if !accept_group(&group_key_mapping,
                                 &having_aggregates,
                                 &group_key_value,
                                 &group_value,
                                 aggregate_statement,
                                 having)? {
                    continue;
                }
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

fn extract_having_aggregates<'a>(aggregate_statement: &'a AggregateStatement) -> ExecutionResult<Vec<(usize, &'a Aggregate)>> {
    let mut having_aggregates = Vec::new();
    if let Some(having) = aggregate_statement.having.as_ref() {
        having.visit(&mut |tree| {
            match tree {
                ExpressionTree::Aggregate(id, aggregate) => {
                    match aggregate.as_ref() {
                        Aggregate::GroupKey(_) => {}
                        aggregate => { having_aggregates.push((*id, aggregate)); }
                    }
                },
                _ => {}
            }

            Result::<(), ExecutionError>::Ok(())
        })?;
    }

    Ok(having_aggregates)
}

fn accept_group<'a>(group_key_mapping: &HashMap<&String, usize>,
                    having_aggregates: &Vec<(usize, &'a Aggregate)>,
                    group_key_value: &GroupKey,
                    group_value: &HashMap<usize, Value>,
                    aggregate_statement: &AggregateStatement,
                    having: &ExpressionTree) -> ExecutionResult<bool> {
    let mut columns = HashMap::new();
    for (group_key_part, group_key_part_index) in group_key_mapping {
        columns.insert(
            format!("$group_key_{}", group_key_part),
            &group_key_value[*group_key_part_index]
        );
    }

    for (having_aggregate_index, &(aggregate_id, aggregate)) in having_aggregates.iter().enumerate() {
        let mut hasher = FnvHasher::default();
        aggregate.hash(&mut hasher);
        let hash = hasher.finish();

        columns.insert(
            format!("$group_value_{}_{}", aggregate_id, hash),
            &group_value[&(aggregate_statement.aggregates.len() + having_aggregate_index)]
        );
    }

    let row = HashMapOwnedKeyColumnProvider::new(columns);
    let expression_execution_engine = ExpressionExecutionEngine::new(&row);
    if !expression_execution_engine.evaluate(having)?.bool() {
        return Ok(false);
    }

    return Ok(true)
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
            ("count".to_owned(), Aggregate::Count(None))
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec!["x".to_string()]),
        having: None,
        join: None
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
fn test_group_by_and_count2() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            ("x".to_owned(), Aggregate::GroupKey("x".to_owned())),
            ("count".to_owned(), Aggregate::Count(Some("x".to_owned())))
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec!["x".to_string()]),
        having: None,
        join: None
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

    let column_values = vec![Value::Null];
    let columns = create_test_columns(vec!["x"], &column_values);

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
    assert_eq!(Value::Int(5), result.data[0].columns[1]);
}

#[test]
fn test_group_by_and_count3() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            ("x".to_owned(), Aggregate::GroupKey("x".to_owned())),
            ("count".to_owned(), Aggregate::Count(None))
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec!["x".to_string()]),
        having: None,
        join: None
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

    let column_values = vec![Value::Null];
    let columns = create_test_columns(vec!["x"], &column_values);

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(columns.clone())
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::Null, result.data[0].columns[0]);
    assert_eq!(Value::Int(1), result.data[0].columns[1]);

    assert_eq!(Value::Int(1000), result.data[1].columns[0]);
    assert_eq!(Value::Int(5), result.data[1].columns[1]);
}


#[test]
fn test_group_by_and_count_and_filter() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            ("x".to_owned(), Aggregate::GroupKey("x".to_owned())),
            ("count".to_owned(), Aggregate::Count(None))
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
        group_by: Some(vec!["x".to_string()]),
        having: None,
        join: None
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
        group_by: Some(vec!["name".to_string()]),
        having: None,
        join: None
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
        group_by: Some(vec!["name".to_string()]),
        having: None,
        join: None
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
            ("count".to_owned(), Aggregate::Count(None)),
            ("max".to_owned(), Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())))
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec!["name".to_string()]),
        having: None,
        join: None
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
        group_by: Some(vec!["name".to_string()]),
        having: None,
        join: None
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
        group_by: Some(vec!["name".to_string()]),
        having: None,
        join: None
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
            ("count".to_owned(), Aggregate::Count(None))
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: None,
        having: None,
        join: None
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

#[test]
fn test_group_by_and_count_and_having1() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            ("x".to_owned(), Aggregate::GroupKey("x".to_owned())),
            ("count".to_owned(), Aggregate::Count(None))
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec!["x".to_string()]),
        having: Some(
            ExpressionTree::Compare {
                operator: CompareOperator::Equal,
                left: Box::new(ExpressionTree::Aggregate(0, Box::new(Aggregate::GroupKey("x".to_owned())))),
                right: Box::new(ExpressionTree::Value(Value::Int(2000)))
            }
        ),
        join: None
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
fn test_group_by_and_count_and_having2() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            ("x".to_owned(), Aggregate::GroupKey("x".to_owned())),
            ("count".to_owned(), Aggregate::Count(None))
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec!["x".to_string()]),
        having: Some(
            ExpressionTree::Compare {
                operator: CompareOperator::NotEqual,
                left: Box::new(ExpressionTree::Aggregate(0, Box::new(Aggregate::GroupKey("x".to_owned())))),
                right: Box::new(ExpressionTree::Value(Value::Int(2000)))
            }
        ),
        join: None
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
    assert_eq!(Value::Int(1000), result.data[0].columns[0]);
    assert_eq!(Value::Int(6), result.data[0].columns[1]);
}

#[test]
fn test_group_by_and_count_and_having3() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            ("x".to_owned(), Aggregate::GroupKey("x".to_owned())),
            ("count".to_owned(), Aggregate::Count(None))
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec!["x".to_string()]),
        having: Some(
            ExpressionTree::Compare {
                operator: CompareOperator::GreaterThan,
                left: Box::new(ExpressionTree::Aggregate(0, Box::new(Aggregate::Count(None)))),
                right: Box::new(ExpressionTree::Value(Value::Int(1)))
            }
        ),
        join: None
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

    // Add another group
    let column_values = vec![Value::Int(3000)];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();
    assert!(result.is_some());

    // Add another group
    let column_values = vec![Value::Int(2000)];
    let columns = create_test_columns(vec!["x"], &column_values);

    for _ in 0..3 {
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

    assert_eq!(2, result.data.len());
    assert_eq!(Value::Int(1000), result.data[0].columns[0]);
    assert_eq!(Value::Int(6), result.data[0].columns[1]);

    assert_eq!(Value::Int(2000), result.data[1].columns[0]);
    assert_eq!(Value::Int(4), result.data[1].columns[1]);
}

#[test]
fn test_group_by_and_count_and_having4() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            ("x".to_owned(), Aggregate::GroupKey("x".to_owned())),
            ("count".to_owned(), Aggregate::Count(None))
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec!["x".to_string()]),
        having: Some(
            ExpressionTree::And {
                left: Box::new(ExpressionTree::Compare {
                    operator: CompareOperator::GreaterThan,
                    left: Box::new(ExpressionTree::Aggregate(0, Box::new(Aggregate::Count(None)))),
                    right: Box::new(ExpressionTree::Value(Value::Int(1)))
                }),
                right: Box::new(ExpressionTree::Compare {
                    operator: CompareOperator::Equal,
                    left: Box::new(ExpressionTree::Aggregate(1, Box::new(Aggregate::GroupKey("x".to_owned())))),
                    right: Box::new(ExpressionTree::Value(Value::Int(1000)))
                })
            }
        ),
        join: None
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

    // Add another group
    let column_values = vec![Value::Int(3000)];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();
    assert!(result.is_some());

    // Add another group
    let column_values = vec![Value::Int(2000)];
    let columns = create_test_columns(vec!["x"], &column_values);

    for _ in 0..3 {
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
}

#[test]
fn test_group_by_array_agg1() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            ("x".to_owned(), Aggregate::GroupKey("x".to_owned())),
            ("ys".to_owned(), Aggregate::CollectArray(ExpressionTree::ColumnAccess("y".to_owned()), false))
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec!["x".to_string()]),
        having: None,
        join: None
    };

    // Add first group
    for i in 0..5 {
        let column_values = vec![Value::Int(1000), Value::Int(100 + i)];
        let columns = create_test_columns(vec!["x", "y"], &column_values);
        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone())
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    // Add second group
    for i in 0..4 {
        let column_values = vec![Value::Int(2000), Value::Int(300 + i)];
        let columns = create_test_columns(vec!["x", "y"], &column_values);
        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone())
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![Value::Int(2000), Value::Int(300 + 4)];
    let columns = create_test_columns(vec!["x", "y"], &column_values);
    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(columns.clone())
    );

    assert!(result.is_ok());
    let result = result.unwrap();
    assert!(result.is_some());

    println!("{:?}", result);

    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::Int(1000), result.data[0].columns[0]);
    assert_eq!(
        Value::Array(ValueType::Int, vec![Value::Int(100), Value::Int(101), Value::Int(102), Value::Int(103), Value::Int(104)]),
        result.data[0].columns[1]
    );

    assert_eq!(Value::Int(2000), result.data[1].columns[0]);
    assert_eq!(
        Value::Array(ValueType::Int, vec![Value::Int(300), Value::Int(301), Value::Int(302), Value::Int(303), Value::Int(304)]),
        result.data[1].columns[1]
    );
}