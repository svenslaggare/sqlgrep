use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::ops::Add;

use fnv::FnvHasher;

use crate::data_model::Row;
use crate::execution::{ColumnProvider, ColumnScope, ExecutionError, ExecutionResult, ExpressionTreeHash, ResultRow};
use crate::execution::column_providers::{HashMapOwnedKeyColumnProvider, SingleColumnProvider};
use crate::execution::expression_execution::{ExpressionExecutionEngine};
use crate::execution::helpers::DistinctValues;
use crate::helpers::IterExt;
use crate::model::{Aggregate, AggregateStatement, ExpressionTree, Float, IntervalType, Value, ValueType};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct GroupKey(Vec<Value>);
type Groups<T> = BTreeMap<GroupKey, HashMap<usize, T>>;

pub struct AggregateExecutionEngine {
    group_aggregators: Groups<GroupAggregator>,
    group_values: Groups<Value>,
    distinct_values: DistinctValues
}

impl AggregateExecutionEngine {
    pub fn new() -> AggregateExecutionEngine {
        AggregateExecutionEngine {
            group_aggregators: Groups::new(),
            group_values: Groups::new(),
            distinct_values: DistinctValues::new()
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
            GroupKey(
                group_by
                    .map_result_vec(|part| expression_execution_engine.evaluate(part))?
            )
        } else {
            GroupKey(vec![Value::Null])
        };

        for (aggregate_index, aggregate) in aggregate_statement.aggregates.iter().enumerate() {
            self.update_aggregate(
                aggregate_statement,
                row,
                expression_execution_engine,
                &group_key,
                aggregate_index,
                &aggregate.aggregate
            )?;
        }

        if let Some(having) = aggregate_statement.having.as_ref() {
            let mut having_aggregate_index = 0;
            having.visit(&mut |tree| {
                match tree {
                    ExpressionTree::Aggregate(_, aggregate) => {
                        match aggregate.as_ref() {
                            Aggregate::GroupKey(group_column) => {
                                AggregateExecutionEngine::validate_group_key(aggregate_statement, group_column)?;
                            }
                            aggregate => {
                                self.update_aggregate(
                                    aggregate_statement,
                                    row,
                                    expression_execution_engine,
                                    &group_key,
                                    aggregate_statement.aggregates.len() + having_aggregate_index,
                                    &aggregate
                                )?;
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

    fn update_aggregate<TColumnProvider: ColumnProvider>(&mut self,
                                                         aggregate_statement: &AggregateStatement,
                                                         row: &TColumnProvider,
                                                         expression_execution_engine: &ExpressionExecutionEngine<TColumnProvider>,
                                                         group_key: &GroupKey,
                                                         aggregate_index: usize,
                                                         aggregate: &Aggregate) -> ExecutionResult<()> {
        match aggregate {
            Aggregate::GroupKey(group_column) => {
                AggregateExecutionEngine::validate_group_key(aggregate_statement, group_column)?;
            }
            Aggregate::Count(column, distinct) => {
                if column.is_none() && *distinct {
                    return Err(ExecutionError::DistinctRequiresColumn);
                }

                let (mut valid, column_value) = if let Some(column) = column {
                    let column_value = row.get(ColumnScope::Table, column).ok_or_else(|| ExecutionError::ColumnNotFound(column.clone()))?;
                    (column_value.is_not_null(), Some(column_value))
                } else {
                    (true, None)
                };

                if valid && *distinct {
                    let count_distinct_aggregator = self.get_group_aggregator(
                        group_key.clone(),
                        aggregate_index,
                        || GroupAggregator::default(aggregate, &Value::Null)
                    )?;

                    if let Some(this_valid) = count_distinct_aggregator.update(column_value.cloned().unwrap())? {
                        valid = this_valid.bool();
                    }
                }

                if valid {
                    if let Value::Int(group_value) = self.get_group_value(group_key.clone(), aggregate_index, || Ok(Value::Int(0)))? {
                        *group_value += 1;
                    }
                }
            }
            Aggregate::Min(ref expression) | Aggregate::Max(ref expression) => {
                let column_value = expression_execution_engine.evaluate(expression)?;
                if column_value.is_not_null() {
                    let group_value = self.get_group_value(group_key.clone(), aggregate_index, || Ok(column_value.clone()))?;

                    match aggregate {
                        Aggregate::Min(_) => {
                            group_value.modify_same_type_numeric_nullable(
                                &column_value,
                                |x, y| { *x = (*x).min(y) },
                                |x, y| { *x = (*x).min(y) },
                                |x, y| { *x = (*x).min(y) }
                            );
                        }
                        Aggregate::Max(_) => {
                            group_value.modify_same_type_numeric_nullable(
                                &column_value,
                                |x, y| { *x = (*x).max(y) },
                                |x, y| { *x = (*x).max(y) },
                                |x, y| { *x = (*x).max(y) }
                            );
                        }
                        _ => { unimplemented!(); }
                    };
                } else {
                    self.get_group_value(group_key.clone(), aggregate_index, || Ok(Value::Null))?;
                }
            }
            Aggregate::Sum(ref expression) | Aggregate::Average(ref expression) | Aggregate::StandardDeviation(ref expression, _) | Aggregate::Percentile(ref expression, _) => {
                let column_value = expression_execution_engine.evaluate(expression)?;

                let aggregator = self.get_group_aggregator(
                    group_key.clone(),
                    aggregate_index,
                    || GroupAggregator::default(aggregate, &column_value)
                )?;

                if column_value.is_not_null() {
                    if let Some(value) = aggregator.update(column_value)? {
                        *self.get_group_value(group_key.clone(), aggregate_index, || Ok(value.clone()))? = value.clone();
                    }
                } else if aggregator.is_null() {
                    *self.get_group_value(group_key.clone(), aggregate_index, || Ok(Value::Null))? = Value::Null;
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
    }

    pub fn execute_result(&mut self, aggregate_statement: &AggregateStatement) -> ExecutionResult<ResultRow> {
        for (group_key, subgroups) in self.group_aggregators.iter_mut() {
            for (aggregate_index, group_aggregator) in subgroups.iter_mut() {
                if let Some(value) = group_aggregator.update_value()? {
                    let entry = AggregateExecutionEngine::get_group(
                        &mut self.group_values,
                        group_key.clone(),
                        *aggregate_index,
                        || Ok(value.clone())
                    )?;
                    *entry = value.clone();
                }
            }
        }

        let mut group_key_mapping = HashMap::new();
        if let Some(group_key) = aggregate_statement.group_by.as_ref() {
            for (index, part) in group_key.iter().enumerate() {
                group_key_mapping.insert(ExpressionTreeHash::new(part), index);
            }
        }

        let (column_names, result_rows_by_column) = self.extract_result_rows_by_column(aggregate_statement, &group_key_mapping)?;

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

                if aggregate_statement.distinct {
                    if !self.distinct_values.add(&result_columns) {
                        continue;
                    }
                }
            }

            result_rows.push(Row::new(result_columns));
        }

        Ok(
            ResultRow {
                data: result_rows,
                columns: column_names
            }
        )
    }

    fn extract_result_rows_by_column(&self,
                                     aggregate_statement: &AggregateStatement,
                                     group_key_mapping: &HashMap<ExpressionTreeHash, usize>) -> ExecutionResult<(Vec<String>, Vec<Vec<Value>>)> {
        let mut column_names = Vec::new();
        let mut result_rows_by_column = Vec::new();
        for (aggregate_index, aggregate) in aggregate_statement.aggregates.iter().enumerate() {
            column_names.push(aggregate.name.clone());
            let mut result_column = Vec::new();

            let transform_value = |value: Value| {
                if let Some(transform) = &aggregate.transform {
                    let columns = SingleColumnProvider::new(ColumnScope::AggregationValue, "$value", &value);
                    ExpressionExecutionEngine::new(&columns).evaluate(transform)
                } else {
                    Ok(value)
                }
            };

            match aggregate.aggregate {
                Aggregate::GroupKey(ref column) => {
                    let hash = ExpressionTreeHash::new(column);
                    for group_key in self.group_values.keys() {
                        result_column.push(group_key.0[group_key_mapping[&hash]].clone());
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

        Ok((column_names, result_rows_by_column))
    }

    fn validate_group_key(aggregate_statement: &AggregateStatement,
                          group_part: &ExpressionTree) -> ExecutionResult<()> {
        match aggregate_statement.group_by.as_ref() {
            None => {
                return Err(ExecutionError::GroupKeyNotAvailable(None));
            }
            Some(group_by) => {
                if !group_by.iter().any(|part| part == group_part) {
                    return Err(ExecutionError::GroupKeyNotAvailable(Some(format!("{}", group_part))));
                }
            }
        }

        Ok(())
    }

    fn get_group_value<F: Fn() -> ExecutionResult<Value>>(&mut self,
                                                          group_key: GroupKey,
                                                          aggregate_index: usize,
                                                          default_value_fn: F) -> ExecutionResult<&mut Value> {
        AggregateExecutionEngine::get_group(
            &mut self.group_values,
            group_key,
            aggregate_index,
            default_value_fn
        )
    }

    fn get_group_aggregator<F: Fn() -> GroupAggregator>(&mut self,
                                                        group_key: GroupKey,
                                                        aggregate_index: usize,
                                                        default_value_fn: F) -> ExecutionResult<&mut GroupAggregator> {
        AggregateExecutionEngine::get_group(
            &mut self.group_aggregators,
            group_key,
            aggregate_index,
            || Ok(default_value_fn())
        )
    }

    fn get_group<T, F: FnOnce() -> ExecutionResult<T>>(groups: &mut BTreeMap<GroupKey, HashMap<usize, T>>,
                                                       group_key: GroupKey,
                                                       aggregate_index: usize,
                                                       default_value_fn: F) -> ExecutionResult<&mut T> {
        let group_map = groups.entry(group_key).or_insert_with(|| HashMap::new());
        if !group_map.contains_key(&aggregate_index) {
            group_map.insert(aggregate_index, default_value_fn()?);
        }

        group_map.get_mut(&aggregate_index).ok_or(ExecutionError::InternalError)
    }
}

enum GroupAggregator {
    Sum(Value),
    Average { sum: Value, count: i64 },
    StandardDeviation { sum: Value, sum_square: Value, count: i64, is_variance: bool },
    Percentile { values: Vec<Value>, percentile: f64 },
    CountDistinct(HashSet<Value>)
}

impl GroupAggregator {
    pub fn default(aggregate: &Aggregate, column_value: &Value) -> GroupAggregator {
        match aggregate {
            Aggregate::GroupKey(_) => { unimplemented!(); }
            Aggregate::Count(_, true) => GroupAggregator::CountDistinct(HashSet::new()),
            Aggregate::Count(_, false) => { unimplemented!(); }
            Aggregate::Min(_) => { unimplemented!(); }
            Aggregate::Max(_) => { unimplemented!(); }
            Aggregate::Average(_) => GroupAggregator::Average {
                sum: column_value.default_value(),
                count: 0,
            },
            Aggregate::Sum(_) => GroupAggregator::Sum(column_value.default_value()),
            Aggregate::StandardDeviation(_, is_variance) => GroupAggregator::StandardDeviation {
                sum: column_value.default_value(),
                sum_square: column_value.default_value(),
                count: 0,
                is_variance: *is_variance
            },
            Aggregate::Percentile(_, percentile) => GroupAggregator::Percentile {
                values: Vec::new(),
                percentile: percentile.0,
            },
            Aggregate::CollectArray(_) => { unimplemented!(); }
        }
    }

    pub fn update(&mut self, column_value: Value) -> ExecutionResult<Option<Value>> {
        match self {
            GroupAggregator::Sum(sum) => {
                sum.modify_same_type_numeric_nullable(
                    &column_value,
                    |x, y| { *x += y },
                    |x, y| { *x += y },
                    |x, y| { *x = x.add(y) }
                );

                let sum = sum.clone();
                Ok(Some(sum))
            }
            GroupAggregator::Average { sum, count } => {
                sum.modify_same_type_numeric_nullable(
                    &column_value,
                    |x, y| { *x += y },
                    |x, y| { *x += y },
                    |x, y| { *x = x.add(y) }
                );
                *count += 1;

                let average = sum.map_numeric(
                    |x| Some(x / *count),
                    |x| Some(x / *count as f64),
                    |x| Some(x / *count as i32)
                );

                Ok(average)
            }
            GroupAggregator::StandardDeviation { sum, sum_square, count, is_variance } => {
                let squared_column_value = column_value.map_numeric(
                    |x| Some(x * x),
                    |x| Some(x * x),
                    |x| {
                        if let Some(microseconds) = x.num_microseconds() {
                            Some(IntervalType::microseconds(microseconds * microseconds))
                        } else {
                            Some(IntervalType::milliseconds(x.num_milliseconds() * x.num_milliseconds()))
                        }
                    }
                ).unwrap_or(Value::Null);

                sum.modify_same_type_numeric_nullable(
                    &column_value,
                    |x, y| { *x += y },
                    |x, y| { *x += y },
                    |x, y| { *x = x.add(y) }
                );

                sum_square.modify_same_type_numeric_nullable(
                    &squared_column_value,
                    |x, y| { *x += y },
                    |x, y| { *x += y },
                    |x, y| { *x = x.add(y) }
                );

                *count += 1;

                let calculate = |sum: f64, sum_square: f64, n: f64| {
                    let variance = (sum_square - (sum * sum) / n) / n;
                    if *is_variance {
                        variance
                    } else {
                        variance.sqrt()
                    }
                };

                let value = match (sum, sum_square) {
                    (Value::Int(sum), Value::Int(sum_square)) => {
                        Some(Value::Float(Float(calculate(*sum as f64, *sum_square as f64, *count as f64))))
                    }
                    (Value::Float(sum), Value::Float(sum_square)) => {
                        Some(Value::Float(Float(calculate(sum.0, sum_square.0, *count as f64))))
                    }
                    _ => None
                };

                Ok(value)
            }
            GroupAggregator::Percentile { values, .. } => {
                values.push(column_value);
                Ok(None)
            }
            GroupAggregator::CountDistinct(values) => {
                Ok(Some(Value::Bool(values.insert(column_value))))
            }
        }
    }

    pub fn update_value(&mut self) -> ExecutionResult<Option<Value>> {
        match self {
            GroupAggregator::Sum(_) => Ok(None),
            GroupAggregator::Average { .. } => Ok(None),
            GroupAggregator::StandardDeviation { .. } => Ok(None),
            GroupAggregator::Percentile { values, percentile } => {
                values.sort();
                Ok(values.get((*percentile * values.len() as f64) as usize).cloned())
            }
            GroupAggregator::CountDistinct(_) => Ok(None)
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            GroupAggregator::Sum(sum) => sum.is_null(),
            GroupAggregator::Average { sum, .. } => sum.is_null(),
            GroupAggregator::StandardDeviation { sum, sum_square, .. } => sum.is_null() || sum_square.is_null(),
            GroupAggregator::Percentile { .. } => false,
            GroupAggregator::CountDistinct(_) => false
        }
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

fn accept_group<'a>(group_key_mapping: &HashMap<ExpressionTreeHash, usize>,
                    having_aggregates: &Vec<(usize, &'a Aggregate)>,
                    group_key_value: &GroupKey,
                    group_value: &HashMap<usize, Value>,
                    aggregate_statement: &AggregateStatement,
                    having: &ExpressionTree) -> ExecutionResult<bool> {
    let mut group_key_columns = HashMap::new();
    let mut group_value_columns = HashMap::new();

    for (group_key_part, group_key_part_index) in group_key_mapping {
        group_key_columns.insert(
            group_key_part.to_string(),
            &group_key_value.0[*group_key_part_index]
        );
    }

    for (having_aggregate_index, &(aggregate_id, aggregate)) in having_aggregates.iter().enumerate() {
        let mut hasher = FnvHasher::default();
        aggregate.hash(&mut hasher);
        let hash = hasher.finish();

        group_value_columns.insert(
            format!("{}_{}", aggregate_id, hash),
            &group_value[&(aggregate_statement.aggregates.len() + having_aggregate_index)]
        );
    }

    let mut columns = HashMap::new();
    columns.insert(ColumnScope::GroupKey, group_key_columns);
    columns.insert(ColumnScope::GroupValue, group_value_columns);

    let row = HashMapOwnedKeyColumnProvider::new(columns);
    let expression_execution_engine = ExpressionExecutionEngine::new(&row);
    if !expression_execution_engine.evaluate(having)?.bool() {
        return Ok(false);
    }

    Ok(true)
}