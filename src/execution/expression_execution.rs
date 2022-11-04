use std::hash::{Hasher, Hash};
use std::collections::{HashMap, BTreeSet};
use std::iter::FromIterator;
use std::ops::Add;

use regex::Regex;

use fnv::FnvHasher;

use chrono::{Local, Datelike, Timelike, DurationRound, Duration};

use itertools::Itertools;

use crate::model::{ExpressionTree, Value, CompareOperator, ArithmeticOperator, UnaryArithmeticOperator, Function, Aggregate, ValueType, value_type_to_string, Float, create_timestamp, NullableCompareOperator, BooleanOperator, IntervalType};
use crate::execution::{ColumnProvider};


#[derive(Debug, PartialEq)]
pub enum EvaluationError {
    ColumnNotFound(String),
    ExpectedNonNull,
    TypeError(ValueType, ValueType),
    GroupKeyNotFound,
    GroupValueNotFound,
    UndefinedOperation,
    UndefinedFunction(Function, Vec<Option<ValueType>>),
    InvalidRegex(String),
    ExpectedArray(Option<ValueType>),
    ExpectedArrayIndexingToBeInt(Option<ValueType>),
    ExpectedArrayElementType,
    FailedToTruncate,
    InvalidTruncatePart,
    FailedToParseTimestamp,
    FailedToConvert
}

impl std::fmt::Display for EvaluationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvaluationError::ColumnNotFound(name) => { write!(f, "Column '{}' not found", name) }
            EvaluationError::ExpectedNonNull => { write!(f, "Expected non-null value") }
            EvaluationError::TypeError(expected, actual) => { write!(f, "Expected type '{}' but got type {}", expected, actual) }
            EvaluationError::GroupKeyNotFound => { write!(f, "Group key not found") }
            EvaluationError::GroupValueNotFound => { write!(f, "Group value not found") }
            EvaluationError::UndefinedOperation => { write!(f, "Undefined operation") }
            EvaluationError::UndefinedFunction(function, arguments) => {
                write!(
                    f,
                    "Undefined function {:?}({})",
                    function,
                    arguments.iter().map(|arg| arg.as_ref().map(|x| x.to_string()).unwrap_or("NULL".to_owned())).join(", ")
                )
            },
            EvaluationError::InvalidRegex(error) => { write!(f, "Invalid regex: {}", error) },
            EvaluationError::ExpectedArray(other_type) => { write!(f, "Expected value to be of array type but got {}", value_type_to_string(other_type)) },
            EvaluationError::ExpectedArrayIndexingToBeInt(other_type) => { write!(f, "Expected array indexing to be of type 'int' but got {}", value_type_to_string(other_type)) },
            EvaluationError::ExpectedArrayElementType => { write!(f, "Expected an element with type") }
            EvaluationError::FailedToTruncate => { write!(f, "Failed to truncate timestamp") }
            EvaluationError::InvalidTruncatePart => { write!(f, "Not a valid part to truncate") },
            EvaluationError::FailedToParseTimestamp => { write!(f, "Failed to parse timestamp") },
            EvaluationError::FailedToConvert => { write!(f, "Failed to convert to type") }
        }
    }
}

pub type EvaluationResult = Result<Value, EvaluationError>;

pub struct ExpressionExecutionEngine<'a, T: ColumnProvider> {
    column_access: &'a T
}

impl<'a, T: ColumnProvider> ExpressionExecutionEngine<'a, T> {
    pub fn new(column_access: &T) -> ExpressionExecutionEngine<T> {
        ExpressionExecutionEngine {
            column_access
        }
    }

    pub fn evaluate(&self, expression: &ExpressionTree) -> EvaluationResult {
        match expression {
            ExpressionTree::Value(value) => Ok(value.clone()),
            ExpressionTree::ColumnAccess(name) => {
                self.column_access
                    .get(name.as_str())
                    .map(|value| value.clone())
                    .ok_or_else(|| EvaluationError::ColumnNotFound(name.to_owned()))
            }
            ExpressionTree::Wildcard => Err(EvaluationError::UndefinedOperation),
            ExpressionTree::Compare { left, right, operator } => {
                let mut left_value = self.evaluate(left)?;
                let mut right_value = self.evaluate(right)?;

                match (&left_value, &right_value) {
                    (Value::Timestamp(_), Value::String(value)) => {
                        right_value = ValueType::Timestamp.parse(&value).ok_or(EvaluationError::FailedToParseTimestamp)?;
                    }
                    (Value::String(value), Value::Timestamp(_)) => {
                        left_value = ValueType::Timestamp.parse(&value).ok_or(EvaluationError::FailedToParseTimestamp)?;
                    }
                    _ => {}
                }

                if !left_value.is_null() && !right_value.is_null() {
                    match operator {
                        CompareOperator::Equal => Ok(Value::Bool(left_value == right_value)),
                        CompareOperator::NotEqual => Ok(Value::Bool(left_value != right_value)),
                        CompareOperator::GreaterThan => Ok(Value::Bool(left_value > right_value)),
                        CompareOperator::GreaterThanOrEqual => Ok(Value::Bool(left_value >= right_value)),
                        CompareOperator::LessThan => Ok(Value::Bool(left_value < right_value)),
                        CompareOperator::LessThanOrEqual => Ok(Value::Bool(left_value <= right_value))
                    }
                } else {
                    Ok(Value::Bool(false))
                }
            }
            ExpressionTree::NullableCompare { left, right, operator } => {
                let left_value = self.evaluate(left)?;
                let right_value = self.evaluate(right)?;

                match operator {
                    NullableCompareOperator::Equal => Ok(Value::Bool(left_value == right_value)),
                    NullableCompareOperator::NotEqual => Ok(Value::Bool(left_value != right_value))
                }
            }
            ExpressionTree::Arithmetic { left, right, operator } => {
                let left_value = self.evaluate(left)?;
                let right_value = self.evaluate(right)?;

                match (&left_value, &right_value) {
                    (Value::Timestamp(left), Value::Interval(right)) => {
                        return Ok(Value::Timestamp(left.add(right.clone())));
                    }
                    (Value::Interval(left), Value::Timestamp(right)) => {
                        return Ok(Value::Timestamp(right.add(left.clone())));
                    }
                    _ => {}
                }

                left_value.map_same_type(
                    &right_value,
                    || Some(Value::Null),
                    |x, y| {
                        Some(
                            Value::Int(
                                match operator {
                                    ArithmeticOperator::Add => x + y,
                                    ArithmeticOperator::Subtract => x - y,
                                    ArithmeticOperator::Multiply => x * y,
                                    ArithmeticOperator::Divide => x / y
                                }
                            )
                        )
                    },
                    |x, y| {
                        Some(
                            Value::Float(
                                Float(
                                    match operator {
                                        ArithmeticOperator::Add => x + y,
                                        ArithmeticOperator::Subtract => x - y,
                                        ArithmeticOperator::Multiply => x * y,
                                        ArithmeticOperator::Divide => x / y
                                    }
                                )
                            )
                        )
                    },
                    |_, _| None,
                    |_, _| None,
                    |_, _| None,
                    |x, y| {
                        match operator {
                            ArithmeticOperator::Subtract => { Some(Value::Interval(x - y)) },
                            _ => { None }
                        }
                    },
                    |x, y| {
                        match operator {
                            ArithmeticOperator::Add => { Some(Value::Interval(x + y)) }
                            ArithmeticOperator::Subtract => { Some(Value::Interval(x - y)) }
                            ArithmeticOperator::Multiply => { None }
                            ArithmeticOperator::Divide => { None }
                        }
                    }
                ).ok_or(EvaluationError::UndefinedOperation)
            }
            ExpressionTree::UnaryArithmetic { operand, operator } => {
                let operand_value = self.evaluate(operand)?;

                operand_value.map(
                    || Some(Value::Null),
                    |x| {
                        match operator {
                            UnaryArithmeticOperator::Negative => Some(-x),
                            UnaryArithmeticOperator::Invert => None
                        }
                    },
                    |x| {
                        match operator {
                            UnaryArithmeticOperator::Negative => Some(-x),
                            UnaryArithmeticOperator::Invert => None
                        }
                    },
                    |x| {
                        match operator {
                            UnaryArithmeticOperator::Negative => None,
                            UnaryArithmeticOperator::Invert => Some(!x)
                        }
                    },
                    |_| None,
                    |_| None,
                    |_| None,
                    |_| None,
                ).ok_or(EvaluationError::UndefinedOperation)
            }
            ExpressionTree::BooleanOperation { operator, left, right } => {
                match operator {
                    BooleanOperator::And => Ok(Value::Bool(self.evaluate(left)?.bool() && self.evaluate(right)?.bool())),
                    BooleanOperator::Or => Ok(Value::Bool(self.evaluate(left)?.bool() || self.evaluate(right)?.bool()))
                }
            }
            ExpressionTree::Function { function, arguments } => {
                let mut executed_arguments = Vec::new();
                for argument in arguments {
                    executed_arguments.push(self.evaluate(argument)?);
                }
                let executed_arguments_types = executed_arguments.iter().map(|arg| arg.value_type()).collect::<Vec<_>>();

                match function {
                    Function::Greatest if arguments.len() == 2 => {
                        let arg0 = executed_arguments.remove(0);
                        let arg1 = executed_arguments.remove(0);

                        arg0.map_same_type(
                            &arg1,
                            || Some(Value::Null),
                            |x, y| Some(Value::Int(x.max(y))),
                            |x, y| Some(Value::Float(Float(x.max(y)))),
                            |_, _| None,
                            |_, _| None,
                            |_, _| None,
                            |_, _| None,
                            |_, _| None,
                        ).ok_or(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                    }
                    Function::Least if arguments.len() == 2 => {
                        let arg0 = executed_arguments.remove(0);
                        let arg1 = executed_arguments.remove(0);

                        arg0.map_same_type(
                            &arg1,
                            || Some(Value::Null),
                            |x, y| Some(Value::Int(x.min(y))),
                            |x, y| Some(Value::Float(Float(x.min(y)))),
                            |_, _| None,
                            |_, _| None,
                            |_, _| None,
                            |_, _| None,
                            |_, _| None,
                        ).ok_or(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                    }
                    Function::Abs if arguments.len() == 1 => {
                        let arg = executed_arguments.remove(0);

                        arg.map(
                            || Some(Value::Null),
                            |x| Some(x.abs()),
                            |x| Some(x.abs()),
                            |_| None,
                            |_| None,
                            |_| None,
                            |_| None,
                            |_| None,
                        ).ok_or(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                    }
                    Function::Sqrt if arguments.len() == 1 => {
                        let arg = executed_arguments.remove(0);

                        arg.map(
                            || Some(Value::Null),
                            |_| None,
                            |x| Some(x.sqrt()),
                            |_| None,
                            |_| None,
                            |_| None,
                            |_| None,
                            |_| None,
                        ).ok_or(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                    }
                    Function::Pow if arguments.len() == 2 => {
                        let arg0 = executed_arguments.remove(0);
                        let arg1 = executed_arguments.remove(0);

                        arg0.map_same_type(
                            &arg1,
                            || Some(Value::Null),
                            |x, y| {
                                if y >= 0 {
                                    Some(Value::Int(x.pow(y as u32)))
                                } else {
                                    None
                                }
                            },
                            |x, y| Some(Value::Float(Float(x.powf(y)))),
                            |_, _| None,
                            |_, _| None,
                            |_, _| None,
                            |_, _| None,
                            |_, _| None,
                        ).ok_or(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                    }
                    Function::StringLength if arguments.len() == 1 => {
                        let arg = executed_arguments.remove(0);

                        match arg {
                            Value::String(str) => Ok(Value::Int(str.chars().count() as i64)),
                            _ => Err(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                        }
                    }
                    Function::StringToUpper if arguments.len() == 1 => {
                        let arg = executed_arguments.remove(0);

                        match arg {
                            Value::String(str) => Ok(Value::String(str.to_uppercase())),
                            _ => Err(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                        }
                    }
                    Function::StringToLower if arguments.len() == 1 => {
                        let arg = executed_arguments.remove(0);

                        match arg {
                            Value::String(str) => Ok(Value::String(str.to_lowercase())),
                            _ => Err(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                        }
                    }
                    Function::RegexMatches if arguments.len() == 2 => {
                        let value = executed_arguments.remove(0);
                        let pattern = executed_arguments.remove(0);

                        match (value, pattern) {
                            (Value::String(value), Value::String(pattern)) => {
                                let pattern = Regex::new(&pattern).map_err(|err| EvaluationError::InvalidRegex(format!("{}", err)))?;
                                Ok(Value::Bool(pattern.is_match(&value)))
                            },
                            (Value::Null, Value::String(_)) => Ok(Value::Bool(false)),
                            _ => Err(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                        }
                    }
                    Function::CreateArray => {
                        let possible_types = executed_arguments.iter().map(|item| item.value_type()).flatten().collect::<Vec<_>>();
                        if possible_types.is_empty() {
                            return Err(EvaluationError::ExpectedArrayElementType);
                        }

                        let array_type = possible_types[0].clone();
                        for element_type in possible_types {
                            if array_type != element_type {
                                return Err(EvaluationError::TypeError(array_type.clone(), element_type));
                            }
                        }

                        Ok(Value::Array(array_type, executed_arguments))
                    }
                    Function::ArrayUnique if arguments.len() == 1 => {
                        let arg = executed_arguments.remove(0);

                        match arg {
                            Value::Array(element, mut values) => {
                                unique_values(&mut values);
                                Ok(Value::Array(element, values))
                            },
                            _ => Err(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                        }
                    }
                    Function::ArrayLength if arguments.len() == 1 => {
                        let arg = executed_arguments.remove(0);

                        match arg {
                            Value::Array(_, values) => {
                                Ok(Value::Int(values.len() as i64))
                            },
                            _ => Err(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                        }
                    }
                    Function::TimestampNow if arguments.len() == 0 => {
                        Ok(Value::Timestamp(Local::now()))
                    }
                    Function::MakeTimestamp if arguments.len() == 8 => {
                        match (&executed_arguments[0], &executed_arguments[1], &executed_arguments[2], &executed_arguments[3], &executed_arguments[4], &executed_arguments[5], &executed_arguments[6]) {
                            (Value::Int(year), Value::Int(month), Value::Int(day), Value::Int(hour), Value::Int(minute), Value::Int(second), Value::Int(microsecond)) => {
                                Ok(
                                    create_timestamp(*year as i32, *month as u32, *day as u32, *hour as u32, *minute as u32, *second as u32, *microsecond as u32)
                                        .map(|timestamp| Value::Timestamp(timestamp))
                                        .unwrap_or(Value::Null)
                                )
                            }
                            _ => Err(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                        }
                    }
                    Function::TimestampExtractEpoch if arguments.len() == 1 => {
                        match executed_arguments.remove(0) {
                            Value::Timestamp(timestamp) => Ok(Value::Float(Float(timestamp.timestamp_millis() as f64 / 1000.0))),
                            _ => Err(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                        }
                    }
                    Function::TimestampExtractYear if arguments.len() == 1 => {
                        match executed_arguments.remove(0) {
                            Value::Timestamp(timestamp) => Ok(Value::Int(timestamp.year() as i64)),
                            _ => Err(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                        }
                    }
                    Function::TimestampExtractMonth if arguments.len() == 1 => {
                        match executed_arguments.remove(0) {
                            Value::Timestamp(timestamp) => Ok(Value::Int(timestamp.month() as i64)),
                            _ => Err(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                        }
                    }
                    Function::TimestampExtractDay if arguments.len() == 1 => {
                        match executed_arguments.remove(0) {
                            Value::Timestamp(timestamp) => Ok(Value::Int(timestamp.day() as i64)),
                            _ => Err(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                        }
                    }
                    Function::TimestampExtractHour if arguments.len() == 1 => {
                        match executed_arguments.remove(0) {
                            Value::Timestamp(timestamp) => Ok(Value::Int(timestamp.hour() as i64)),
                            _ => Err(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                        }
                    }
                    Function::TimestampExtractMinute if arguments.len() == 1 => {
                        match executed_arguments.remove(0) {
                            Value::Timestamp(timestamp) => Ok(Value::Int(timestamp.minute() as i64)),
                            _ => Err(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                        }
                    }
                    Function::TimestampExtractSecond if arguments.len() == 1 => {
                        match executed_arguments.remove(0) {
                            Value::Timestamp(timestamp) => Ok(Value::Int(timestamp.second() as i64)),
                            _ => Err(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                        }
                    }
                    Function::TruncateTimestamp if arguments.len() == 2 => {
                        match (&executed_arguments[0], &executed_arguments[1]) {
                            (Value::String(part), Value::Timestamp(timestamp)) => {
                                enum NonDurationField { Year, Month, Day }

                                let duration = match part.as_str() {
                                    "year" => Err(NonDurationField::Year),
                                    "month" => Err(NonDurationField::Month),
                                    "day" => Err(NonDurationField::Day),
                                    "hour" => Ok(Duration::hours(1)),
                                    "minute" => Ok(Duration::minutes(1)),
                                    "second" => Ok(Duration::seconds(1)),
                                    "milliseconds" => Ok(Duration::milliseconds(1)),
                                    "microseconds" => Ok(Duration::microseconds(1)),
                                    _ => { return Err(EvaluationError::InvalidTruncatePart); }
                                };

                                match duration {
                                    Ok(duration) => {
                                        let trunc_timestamp = timestamp.duration_trunc(duration).map_err(|_| EvaluationError::FailedToTruncate)?;
                                        Ok(Value::Timestamp(trunc_timestamp))
                                    }
                                    Err(NonDurationField::Year) => {
                                        let trunc_timestamp = timestamp
                                            .with_month(1).unwrap()
                                            .with_day(1).unwrap()
                                            .with_hour(0).unwrap()
                                            .with_minute(0).unwrap()
                                            .with_second(0).unwrap()
                                            .with_nanosecond(0).unwrap();
                                        Ok(Value::Timestamp(trunc_timestamp))
                                    }
                                    Err(NonDurationField::Month) => {
                                        let trunc_timestamp = timestamp
                                            .with_day(1).unwrap()
                                            .with_hour(0).unwrap()
                                            .with_minute(0).unwrap()
                                            .with_second(0).unwrap()
                                            .with_nanosecond(0).unwrap();
                                        Ok(Value::Timestamp(trunc_timestamp))
                                    }
                                    Err(NonDurationField::Day) => {
                                        let trunc_timestamp = timestamp
                                            .with_hour(0).unwrap()
                                            .with_minute(0).unwrap()
                                            .with_second(0).unwrap()
                                            .with_nanosecond(0).unwrap();
                                        Ok(Value::Timestamp(trunc_timestamp))
                                    }
                                }
                            }
                            _ => Err(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                        }
                    }
                    _ => Err(EvaluationError::UndefinedFunction(function.clone(), executed_arguments_types))
                }
            }
            ExpressionTree::ArrayElementAccess { array, index } => {
                let array = self.evaluate(array)?;
                match array {
                    Value::Array(_, values) => {
                        let index = self.evaluate(index)?;
                        match index {
                            Value::Int(value) => {
                                Ok(values.get((value - 1) as usize).cloned().unwrap_or(Value::Null))
                            }
                            _ => {
                                Err(EvaluationError::ExpectedArrayIndexingToBeInt(index.value_type()))
                            }
                        }
                    }
                    _ => { Err(EvaluationError::ExpectedArray(array.value_type())) }
                }
            }
            ExpressionTree::TypeConversion { operand, convert_to_type } => {
                let operand = self.evaluate(operand)?;
                match operand {
                    Value::String(value) => {
                        convert_to_type.parse(&value).ok_or(EvaluationError::FailedToConvert)
                    }
                    _ => {
                        return if let Some(operand_type) = operand.value_type() {
                            Err(EvaluationError::TypeError(ValueType::String, operand_type))
                        } else {
                            Err(EvaluationError::ExpectedNonNull)
                        }
                    }
                }
            }
            ExpressionTree::Aggregate(id, aggregate) => {
                match aggregate.as_ref() {
                    Aggregate::GroupKey(column) => {
                        self.column_access
                            .get(&format!("$group_key_{}", column))
                            .map(|value| value.clone())
                            .ok_or(EvaluationError::GroupKeyNotFound)
                    }
                    aggregate => {
                        let mut hasher = FnvHasher::default();
                        aggregate.hash(&mut hasher);
                        let hash = hasher.finish();

                        self.column_access
                            .get(&format!("$group_value_{}_{}", id, hash))
                            .map(|value| value.clone())
                            .ok_or(EvaluationError::GroupValueNotFound)
                    }
                }
            }
        }
    }
}

pub fn unique_values(values: &mut Vec<Value>) {
    let unique_values = std::mem::take(values);
    *values = Vec::from_iter(BTreeSet::from_iter(unique_values.into_iter()).into_iter());
}

struct TestColumnProvider {
    columns: HashMap<String, Value>,
    keys: Vec<String>
}

impl TestColumnProvider {
    fn new() -> TestColumnProvider {
        TestColumnProvider {
            columns: HashMap::new(),
            keys: Vec::new()
        }
    }

    fn add_column(&mut self, name: &str, value: Value) {
        self.columns.insert(name.to_owned(), value);
    }
}

impl ColumnProvider for TestColumnProvider {
    fn get(&self, name: &str) -> Option<&Value> {
        self.columns.get(name)
    }

    fn add_key(&mut self, key: &str) {
        self.keys.push(key.to_owned());
    }

    fn keys(&self) -> &Vec<String> {
        &self.keys
    }
}

#[test]
fn test_evaluate_column() {
    let mut column_provider = TestColumnProvider::new();
    column_provider.add_column("x", Value::Int(1337));

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Ok(Value::Int(1337)),
        expression_execution_engine.evaluate(&ExpressionTree::ColumnAccess("x".to_owned()))
    );

    assert_eq!(
        Err(EvaluationError::ColumnNotFound("y".to_owned())),
        expression_execution_engine.evaluate(&ExpressionTree::ColumnAccess("y".to_owned()))
    );
}

#[test]
fn test_evaluate_compare1() {
    let column_provider = TestColumnProvider::new();

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Ok(Value::Bool(true)),
        expression_execution_engine.evaluate(&ExpressionTree::Compare {
            left: Box::new(ExpressionTree::Value(Value::Int(10))),
            right: Box::new(ExpressionTree::Value(Value::Int(4))),
            operator: CompareOperator::GreaterThan
        })
    );

    assert_eq!(
        Ok(Value::Bool(false)),
        expression_execution_engine.evaluate(&ExpressionTree::Compare {
            left: Box::new(ExpressionTree::Value(Value::Int(4))),
            right: Box::new(ExpressionTree::Value(Value::Int(10))),
            operator: CompareOperator::GreaterThan
        })
    );
}

#[test]
fn test_evaluate_compare2() {
    let column_provider = TestColumnProvider::new();

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Ok(Value::Bool(false)),
        expression_execution_engine.evaluate(&ExpressionTree::Compare {
            left: Box::new(ExpressionTree::Value(Value::Null)),
            right: Box::new(ExpressionTree::Value(Value::Null)),
            operator: CompareOperator::Equal
        })
    );
}

#[test]
fn test_evaluate_compare3() {
    let column_provider = TestColumnProvider::new();

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Ok(Value::Bool(true)),
        expression_execution_engine.evaluate(&ExpressionTree::NullableCompare {
            operator: NullableCompareOperator::Equal,
            left: Box::new(ExpressionTree::Value(Value::Null)),
            right: Box::new(ExpressionTree::Value(Value::Null)),
        })
    );

    assert_eq!(
        Ok(Value::Bool(true)),
        expression_execution_engine.evaluate(&ExpressionTree::NullableCompare {
            operator: NullableCompareOperator::NotEqual,
            left: Box::new(ExpressionTree::Value(Value::Int(10))),
            right: Box::new(ExpressionTree::Value(Value::Null)),
        })
    );
}

#[test]
fn test_evaluate_and() {
    let column_provider = TestColumnProvider::new();

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Ok(Value::Bool(true)),
        expression_execution_engine.evaluate(&ExpressionTree::BooleanOperation {
            operator: BooleanOperator::And,
            left: Box::new(ExpressionTree::Compare {
                left: Box::new(ExpressionTree::Value(Value::Int(10))),
                right: Box::new(ExpressionTree::Value(Value::Int(4))),
                operator: CompareOperator::GreaterThan
            }),
            right:  Box::new(ExpressionTree::Compare {
                left: Box::new(ExpressionTree::Value(Value::Int(2))),
                right: Box::new(ExpressionTree::Value(Value::Int(10))),
                operator: CompareOperator::LessThan
            })
        })
    );

    assert_eq!(
        Ok(Value::Bool(false)),
        expression_execution_engine.evaluate(&ExpressionTree::BooleanOperation {
            operator: BooleanOperator::And,
            left: Box::new(ExpressionTree::Compare {
                left: Box::new(ExpressionTree::Value(Value::Int(10))),
                right: Box::new(ExpressionTree::Value(Value::Int(4))),
                operator: CompareOperator::GreaterThan
            }),
            right:  Box::new(ExpressionTree::Compare {
                left: Box::new(ExpressionTree::Value(Value::Int(10))),
                right: Box::new(ExpressionTree::Value(Value::Int(2))),
                operator: CompareOperator::LessThan
            })
        })
    );
}

#[test]
fn test_evaluate_or() {
    let column_provider = TestColumnProvider::new();

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Ok(Value::Bool(true)),
        expression_execution_engine.evaluate(&ExpressionTree::BooleanOperation {
            operator: BooleanOperator::Or,
            left: Box::new(ExpressionTree::Compare {
                left: Box::new(ExpressionTree::Value(Value::Int(10))),
                right: Box::new(ExpressionTree::Value(Value::Int(4))),
                operator: CompareOperator::GreaterThan
            }),
            right:  Box::new(ExpressionTree::Compare {
                left: Box::new(ExpressionTree::Value(Value::Int(2))),
                right: Box::new(ExpressionTree::Value(Value::Int(10))),
                operator: CompareOperator::LessThan
            })
        })
    );

    assert_eq!(
        Ok(Value::Bool(true)),
        expression_execution_engine.evaluate(&ExpressionTree::BooleanOperation {
            operator: BooleanOperator::Or,
            left: Box::new(ExpressionTree::Compare {
                left: Box::new(ExpressionTree::Value(Value::Int(10))),
                right: Box::new(ExpressionTree::Value(Value::Int(4))),
                operator: CompareOperator::GreaterThan
            }),
            right:  Box::new(ExpressionTree::Compare {
                left: Box::new(ExpressionTree::Value(Value::Int(10))),
                right: Box::new(ExpressionTree::Value(Value::Int(2))),
                operator: CompareOperator::LessThan
            })
        })
    );
}

#[test]
fn test_arithmetic1() {
    let column_provider = TestColumnProvider::new();

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Ok(Value::Int(5000)),
        expression_execution_engine.evaluate(&ExpressionTree::Arithmetic {
            left: Box::new(ExpressionTree::Value(Value::Int(4000))),
            right: Box::new(ExpressionTree::Value(Value::Int(1000))),
            operator: ArithmeticOperator::Add
        })
    );
}

#[test]
fn test_arithmetic2() {
    let column_provider = TestColumnProvider::new();

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Ok(Value::Timestamp(create_timestamp(2022, 1, 1, 10, 0, 0, 0).unwrap())),
        expression_execution_engine.evaluate(&ExpressionTree::Arithmetic {
            left: Box::new(ExpressionTree::Value(Value::Timestamp(create_timestamp(2022, 1, 1, 0, 0, 0, 0).unwrap()))),
            right: Box::new(ExpressionTree::Value(Value::Interval(IntervalType::hours(10)))),
            operator: ArithmeticOperator::Add
        })
    );
}

#[test]
fn test_arithmetic_fail() {
    let column_provider = TestColumnProvider::new();

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Err(EvaluationError::UndefinedOperation),
        expression_execution_engine.evaluate(&ExpressionTree::Arithmetic {
            left: Box::new(ExpressionTree::Value(Value::Int(4000))),
            right: Box::new(ExpressionTree::Value(Value::Bool(false))),
            operator: ArithmeticOperator::Add
        })
    );
}

#[test]
fn test_unary_arithmetic() {
    let column_provider = TestColumnProvider::new();

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Ok(Value::Int(-5000)),
        expression_execution_engine.evaluate(&ExpressionTree::UnaryArithmetic {
            operand: Box::new(ExpressionTree::Value(Value::Int(5000))),
            operator: UnaryArithmeticOperator::Negative
        })
    );
}

#[test]
fn test_function1() {
    let column_provider = TestColumnProvider::new();

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Ok(Value::Int(5000)),
        expression_execution_engine.evaluate(&ExpressionTree::Function {
            function: Function::Greatest,
            arguments: vec![
                ExpressionTree::Value(Value::Int(-1000)),
                ExpressionTree::Value(Value::Int(5000))
            ]
        })
    );
}

#[test]
fn test_null1() {
    let column_provider = TestColumnProvider::new();

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Ok(Value::Null),
        expression_execution_engine.evaluate(&ExpressionTree::UnaryArithmetic {
            operand: Box::new(ExpressionTree::Value(Value::Null)),
            operator: UnaryArithmeticOperator::Invert
        })
    );
}

#[test]
fn test_null2() {
    let column_provider = TestColumnProvider::new();

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Ok(Value::Null),
        expression_execution_engine.evaluate(&ExpressionTree::Arithmetic {
            left: Box::new(ExpressionTree::Value(Value::Null)),
            right: Box::new(ExpressionTree::Value(Value::Null)),
            operator: ArithmeticOperator::Add
        })
    );
}

#[test]
fn test_regex1() {
    let column_provider = TestColumnProvider::new();

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Ok(Value::Bool(true)),
        expression_execution_engine.evaluate(&ExpressionTree::Function {
            function: Function::RegexMatches,
            arguments: vec![
                ExpressionTree::Value(Value::String("HELLO MY NAME".to_owned())),
                ExpressionTree::Value(Value::String("(my)|(MY)".to_owned()))
            ]
        })
    );

    assert_eq!(
        Ok(Value::Bool(false)),
        expression_execution_engine.evaluate(&ExpressionTree::Function {
            function: Function::RegexMatches,
            arguments: vec![
                ExpressionTree::Value(Value::String("HELLO me NAME".to_owned())),
                ExpressionTree::Value(Value::String("(my)|(MY)".to_owned()))
            ]
        })
    );
}

#[test]
fn test_array_access1() {
    let column_provider = TestColumnProvider::new();

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Ok(Value::Int(20)),
        expression_execution_engine.evaluate(&ExpressionTree::ArrayElementAccess {
            array: Box::new(ExpressionTree::Value(Value::Array(ValueType::Int, vec![Value::Int(10), Value::Int(20), Value::Int(30), Value::Int(40)]))),
            index: Box::new(ExpressionTree::Value(Value::Int(2)))
        })
    );
}

#[test]
fn test_array_access2() {
    let column_provider = TestColumnProvider::new();

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Ok(Value::Null),
        expression_execution_engine.evaluate(&ExpressionTree::ArrayElementAccess {
            array: Box::new(ExpressionTree::Value(Value::Array(ValueType::Int, vec![Value::Int(10), Value::Int(20), Value::Int(30), Value::Int(40)]))),
            index: Box::new(ExpressionTree::Value(Value::Int(24)))
        })
    );
}

#[test]
fn test_create_array1() {
    let column_provider = TestColumnProvider::new();

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Ok(Value::Array(ValueType::Int, vec![Value::Int(1337), Value::Int(4711)])),
        expression_execution_engine.evaluate(&ExpressionTree::Function {
            function: Function::CreateArray,
            arguments: vec![ExpressionTree::Value(Value::Int(1337)), ExpressionTree::Value(Value::Int(4711))]
        })
    );
}

#[test]
fn test_create_array2() {
    let column_provider = TestColumnProvider::new();

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Ok(Value::Array(ValueType::Int, vec![Value::Int(1337), Value::Null])),
        expression_execution_engine.evaluate(&ExpressionTree::Function {
            function: Function::CreateArray,
            arguments: vec![ExpressionTree::Value(Value::Int(1337)), ExpressionTree::Value(Value::Null)]
        })
    );
}

#[test]
fn test_create_array3() {
    let column_provider = TestColumnProvider::new();

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Err(EvaluationError::ExpectedArrayElementType),
        expression_execution_engine.evaluate(&ExpressionTree::Function {
            function: Function::CreateArray,
            arguments: vec![ExpressionTree::Value(Value::Null)]
        })
    );
}

#[test]
fn test_type_convert1() {
    let column_provider = TestColumnProvider::new();

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Ok(Value::Timestamp(create_timestamp(2022, 10, 14, 22, 11, 12, 0).unwrap())),
        expression_execution_engine.evaluate(&ExpressionTree::TypeConversion {
            operand: Box::new(ExpressionTree::Value(Value::String("2022-10-14 22:11:12".to_owned()))),
            convert_to_type: ValueType::Timestamp
        })
    );
}