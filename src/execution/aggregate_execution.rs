use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};

use fnv::FnvHasher;

use crate::data_model::Row;
use crate::execution::{ColumnProvider, ExecutionError, ExecutionResult, HashMapOwnedKeyColumnProvider, ResultRow, SingleColumnProvider};
use crate::execution::expression_execution::{ExpressionExecutionEngine};
use crate::model::{Aggregate, AggregateStatement, ExpressionTree, Float, Value, ValueType};

type GroupKey = Vec<Value>;
type Groups<T> = BTreeMap<GroupKey, HashMap<usize, T>>;

enum SummaryGroupValue {
    Sum(Value),
    Average(Value, i64),
    StandardDeviation { sum: Value, sum_square: Value, count: i64 },
    CountDistinct(HashSet<Value>)
}

pub struct AggregateExecutionEngine {
    group_values: Groups<Value>,
    groups: Groups<SummaryGroupValue>
}

impl AggregateExecutionEngine {
    pub fn new() -> AggregateExecutionEngine {
        AggregateExecutionEngine {
            group_values: BTreeMap::new(),
            groups: BTreeMap::new()
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
                Aggregate::Count(column, distinct) => {
                    if column.is_none() && *distinct {
                        return Err(ExecutionError::DistinctRequiresColumn);
                    }

                    let (mut valid, column_value) = if let Some(column) = column {
                        let column_value = row.get(column).ok_or_else(|| ExecutionError::ColumnNotFound(column.clone()))?;
                        (column_value.is_not_null(), Some(column_value))
                    } else {
                        (true, None)
                    };

                    if valid && *distinct {
                        let count_distinct_entry = self.get_group(
                            group_key.clone(),
                            aggregate_index,
                            || SummaryGroupValue::CountDistinct(HashSet::new())
                        )?;

                        if let SummaryGroupValue::CountDistinct(values) = count_distinct_entry {
                            valid = values.insert(column_value.cloned().unwrap());
                        }
                    }

                    if valid {
                        if let Value::Int(group_value) = self.get_group_value(group_key.clone(), aggregate_index, || Ok(Value::Int(0)))? {
                            *group_value += 1;
                        }
                    }
                }
                Aggregate::Min(ref expression) => {
                    let column_value = expression_execution_engine.evaluate(expression)?;
                    if column_value.is_not_null() {
                        let group_value = self.get_group_value(group_key.clone(), aggregate_index, || Ok(column_value.clone()))?;
                        group_value.modify_same_type_numeric_nullable(
                            &column_value,
                            |x, y| { *x = (*x).min(y) },
                            |x, y| { *x = (*x).min(y) }
                        );
                    } else {
                        self.get_group_value(group_key.clone(), aggregate_index, || Ok(Value::Null))?;
                    }
                }
                Aggregate::Max(ref expression) => {
                    let column_value = expression_execution_engine.evaluate(expression)?;
                    if column_value.is_not_null() {
                        let group_value = self.get_group_value(group_key.clone(), aggregate_index, || Ok(column_value.clone()))?;
                        group_value.modify_same_type_numeric_nullable(
                            &column_value,
                            |x, y| { *x = (*x).max(y) },
                            |x, y| { *x = (*x).max(y) }
                        );
                    } else {
                        self.get_group_value(group_key.clone(), aggregate_index, || Ok(Value::Null))?;
                    }
                }
                Aggregate::Sum(ref expression) => {
                    let column_value = expression_execution_engine.evaluate(expression)?;
                    if column_value.is_not_null() {
                        let value_type = column_value.value_type().ok_or(ExecutionError::ExpectedNumericValue)?;

                        let sum_entry = self.get_group(
                            group_key.clone(),
                            aggregate_index,
                            || SummaryGroupValue::Sum(value_type.default_value())
                        )?;

                        if let SummaryGroupValue::Sum(sum) = sum_entry {
                            sum.modify_same_type_numeric_nullable(
                                &column_value,
                                |x, y| { *x += y },
                                |x, y| { *x += y }
                            );

                            let sum = sum.clone();
                            *self.get_group_value(group_key.clone(), aggregate_index, || Ok(sum.clone()))? = sum.clone();
                        }
                    } else {
                        let sum_entry = self.get_group(
                            group_key.clone(),
                            aggregate_index,
                            || SummaryGroupValue::Sum(Value::Null)
                        )?;

                        if let SummaryGroupValue::Sum(sum) = sum_entry {
                            if sum.is_null() {
                                *self.get_group_value(group_key.clone(), aggregate_index, || Ok(Value::Null))? = Value::Null;
                            }
                        }
                    }
                }
                Aggregate::Average(ref expression) => {
                    let column_value = expression_execution_engine.evaluate(expression)?;
                    if column_value.is_not_null() {
                        let value_type = column_value.value_type().ok_or(ExecutionError::ExpectedNumericValue)?;

                        let average_entry = self.get_group(
                            group_key.clone(),
                            aggregate_index,
                            || SummaryGroupValue::Average(value_type.default_value(), 0)
                        )?;

                        if let SummaryGroupValue::Average(sum, count) = average_entry {
                            sum.modify_same_type_numeric_nullable(
                                &column_value,
                                |x, y| { *x += y },
                                |x, y| { *x += y }
                            );
                            *count += 1;

                            let average = sum.map_numeric(
                                |x| Some(x / *count),
                                |x| Some(x / *count as f64)
                            );

                            if let Some(average) = average {
                                *self.get_group_value(group_key.clone(), aggregate_index, || Ok(average.clone()))? = average.clone();
                            }
                        }
                    } else {
                        let average_entry = self.get_group(
                            group_key.clone(),
                            aggregate_index,
                            || SummaryGroupValue::Average(Value::Null, 0)
                        )?;

                        if let SummaryGroupValue::Average(sum, _) = average_entry {
                            if sum.is_null() {
                                *self.get_group_value(group_key.clone(), aggregate_index, || Ok(Value::Null))? = Value::Null;
                            }
                        }
                    }
                }
                Aggregate::StandardDeviation(ref expression) => {
                    let column_value = expression_execution_engine.evaluate(expression)?;
                    if column_value.is_not_null() {
                        let value_type = column_value.value_type().ok_or(ExecutionError::ExpectedNumericValue)?;

                        let std_entry = self.get_group(
                            group_key.clone(),
                            aggregate_index,
                            || SummaryGroupValue::StandardDeviation {
                                sum: value_type.default_value(),
                                sum_square: value_type.default_value(),
                                count: 0
                            }
                        )?;

                        if let SummaryGroupValue::StandardDeviation { sum, sum_square, count } = std_entry {
                            let squared_column_value = column_value.map_numeric(
                                |x| Some(x * x),
                                |x| Some(x * x)
                            ).unwrap_or(Value::Null);

                            sum.modify_same_type_numeric_nullable(
                                &column_value,
                                |x, y| { *x += y },
                                |x, y| { *x += y }
                            );

                            sum_square.modify_same_type_numeric_nullable(
                                &squared_column_value,
                                |x, y| { *x += y },
                                |x, y| { *x += y }
                            );

                            *count += 1;

                            let calculate_std = |sum: f64, sum_square: f64, n: f64| {
                                ((sum_square - (sum * sum) / n) / n).sqrt()
                            };

                            let std = match (sum, sum_square) {
                                (Value::Int(sum), Value::Int(sum_square)) => {
                                    Some(Value::Float(Float(calculate_std(*sum as f64, *sum_square as f64, *count as f64))))
                                }
                                (Value::Float(sum), Value::Float(sum_square)) => {
                                    Some(Value::Float(Float(calculate_std(sum.0, sum_square.0, *count as f64))))
                                }
                                _ => None
                            };

                            if let Some(std) = std {
                                *self.get_group_value(group_key.clone(), aggregate_index, || Ok(std.clone()))? = std.clone();
                            }
                        }
                    } else {
                        let std_entry = self.get_group(
                            group_key.clone(),
                            aggregate_index,
                            || SummaryGroupValue::StandardDeviation {
                                sum: Value::Null,
                                sum_square: Value::Null,
                                count: 0
                            }
                        )?;

                        if let SummaryGroupValue::StandardDeviation { sum, sum_square, .. } = std_entry {
                            if sum.is_null() || sum_square.is_null() {
                                *self.get_group_value(group_key.clone(), aggregate_index, || Ok(Value::Null))? = Value::Null;
                            }
                        }
                    }
                }
                Aggregate::CollectArray(ref expression) => {
                    let column_value = expression_execution_engine.evaluate(expression)?;
                    let group_value = self.get_group_value(
                        group_key.clone(),
                        aggregate_index,
                        || {
                            let element_type = column_value.value_type().ok_or(ExecutionError::CannotCreateArrayOfNullType)?;
                            Ok(ValueType::Array(Box::new(element_type)).default_value())
                        }
                    )?;

                    if let Value::Array(_, array) = group_value {
                        array.push(column_value.clone());
                    }
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

    fn get_group_value<F: Fn() -> ExecutionResult<Value>>(&mut self, group_key: GroupKey, aggregate_index: usize, default_value_fn: F) -> ExecutionResult<&mut Value> {
        AggregateExecutionEngine::get_generic_group(
            &mut self.group_values,
            group_key,
            aggregate_index,
            default_value_fn
        )
    }

    fn get_group<F: Fn() -> SummaryGroupValue>(&mut self,
                                               group_key: GroupKey,
                                               aggregate_index: usize,
                                               default_value_fn: F) -> ExecutionResult<&mut SummaryGroupValue> {
        AggregateExecutionEngine::get_generic_group(
            &mut self.groups,
            group_key,
            aggregate_index,
            || Ok(default_value_fn())
        )
    }

    fn get_generic_group<T, F: FnOnce() -> ExecutionResult<T>>(groups: &mut BTreeMap<GroupKey, HashMap<usize, T>>,
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

            let transform_value = |value: Value| {
                if let Some(transform) = &aggregate.2 {
                    let columns = SingleColumnProvider::new("$agg", &value);
                    ExpressionExecutionEngine::new(&columns).evaluate(transform)
                } else {
                    Ok(value)
                }
            };

            match aggregate.1 {
                Aggregate::GroupKey(ref column) => {
                    for group_key in self.group_values.keys() {
                        result_column.push(group_key[group_key_mapping[&column]].clone());
                    }
                }
                _ => {
                    for subgroups in self.group_values.values() {
                        if let Some(group_value) = subgroups.get(&aggregate_index) {
                            result_column.push(transform_value(group_value.clone())?);
                        }
                    }
                }
            }

            result_rows_by_column.push(result_column);
        }

        let num_columns = result_rows_by_column.len();
        let num_rows = result_rows_by_column[0].len();

        let mut result_rows = Vec::new();
        let mut group_key_iterator = self.group_values.keys();
        let mut group_value_iterator = self.group_values.values();

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