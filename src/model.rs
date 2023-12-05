use std::str::FromStr;

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::ops::{Add};

use itertools::Itertools;

use chrono::{NaiveDate, NaiveTime, NaiveDateTime, DateTime, Local, TimeZone, Duration};

use crate::data_model::TableDefinition;
use crate::execution::ColumnScope;

#[derive(Debug, Hash, Default)]
pub struct SelectStatement {
    pub projections: Vec<(String, ExpressionTree)>,
    pub from: String,
    pub filename: Option<String>,
    pub filter: Option<ExpressionTree>,
    pub join: Option<JoinClause>,
    pub limit: Option<usize>,
    pub distinct: bool
}

impl SelectStatement {
    pub fn is_wildcard_projection(&self) -> bool {
        self.projections.len() == 1 && self.projections[0].1 == ExpressionTree::Wildcard
    }
}

#[derive(Debug, Hash)]
pub struct AggregateStatementAggregation {
    pub name: String,
    pub aggregate: Aggregate,
    pub transform: Option<ExpressionTree>
}

#[derive(Debug, Hash, Default)]
pub struct AggregateStatement {
    pub aggregates: Vec<AggregateStatementAggregation>,
    pub from: String,
    pub filename: Option<String>,
    pub filter: Option<ExpressionTree>,
    pub group_by: Option<Vec<ExpressionTree>>,
    pub having: Option<ExpressionTree>,
    pub join: Option<JoinClause>,
    pub limit: Option<usize>,
    pub distinct: bool
}

pub struct CreateTableStatement {
    pub name: String,
    pub patterns: Vec<(String, String)>,
    pub columns: Vec<(String, ValueType, String, usize)>
}

#[derive(Debug)]
pub enum Statement {
    Select(SelectStatement),
    Aggregate(AggregateStatement),
    CreateTable(TableDefinition),
    Multiple(Vec<Statement>)
}

impl Statement {
    pub fn filename(&self) -> Option<&str> {
        match self {
            Statement::Select(statement) => statement.filename.as_ref().map(|x| x.as_str()),
            Statement::Aggregate(statement) => statement.filename.as_ref().map(|x| x.as_str()),
            Statement::CreateTable(_) => None,
            Statement::Multiple(_) => None,
        }
    }

    pub fn is_aggregate(&self) -> bool {
        match self {
            Statement::Aggregate(_) => true,
            _ => false
        }
    }

    pub fn extract_select(self) -> Option<SelectStatement> {
        match self {
            Statement::Select(statement) => Some(statement),
            _ => None
        }
    }

    pub fn extract_aggregate(self) -> Option<AggregateStatement> {
        match self {
            Statement::Aggregate(statement) => Some(statement),
            _ => None
        }
    }

    pub fn extract_create_table(self) -> Option<TableDefinition> {
        match self {
            Statement::CreateTable(statement) => Some(statement),
            _ => None
        }
    }

    pub fn extract_multiple(self) -> Option<Vec<Statement>> {
        match self {
            Statement::Multiple(statements) => Some(statements),
            _ => None
        }
    }

    pub fn join_clause(&self) -> Option<&JoinClause> {
        match self {
            Statement::Select(select) => select.join.as_ref(),
            Statement::Aggregate(aggregate) => aggregate.join.as_ref(),
            Statement::CreateTable(_) => None,
            Statement::Multiple(_) => None
        }
    }
}

#[derive(PartialEq, Debug, Hash)]
pub struct JoinClause {
    pub joiner_column: String,
    pub joined_table: String,
    pub joined_filename: String,
    pub joined_column: String,
    pub is_outer: bool
}

pub type TimestampType = DateTime<Local>;
pub type IntervalType = Duration;

#[derive(Debug, PartialEq, PartialOrd, Clone, Eq, Hash, Ord)]
pub enum Value {
    Null,
    Int(i64),
    Float(Float),
    Bool(bool),
    String(String),
    Array(ValueType, Vec<Value>),
    Timestamp(TimestampType),
    Interval(IntervalType)
}

impl Value {
    pub fn from_option(option: Option<Value>) -> Value {
        match option {
            Some(x) => x,
            None => Value::Null
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            Value::Null => true,
            _ => false
        }
    }

    pub fn is_not_null(&self) -> bool {
        !self.is_null()
    }

    pub fn bool(&self) -> bool {
        match self {
            Value::Bool(value) => *value,
            _ => false,
        }
    }

    pub fn value_type(&self) -> Option<ValueType> {
        match self {
            Value::Null => None,
            Value::Int(_) => Some(ValueType::Int),
            Value::Float(_) => Some(ValueType::Float),
            Value::Bool(_) => Some(ValueType::Bool),
            Value::String(_) => Some(ValueType::String),
            Value::Array(element, _) => Some(ValueType::Array(Box::new(element.clone()))),
            Value::Timestamp(_) => Some(ValueType::Timestamp),
            Value::Interval(_) => Some(ValueType::Interval)
        }
    }

    pub fn default_value(&self) -> Value {
        if let Some(value_type) = self.value_type() {
            value_type.default_value()
        } else {
            Value::Null
        }
    }

    pub fn map_same_type<
        F1: Fn() -> Option<Value>,
        F2: Fn(i64, i64) -> Option<Value>,
        F3: Fn(f64, f64) -> Option<Value>,
        F4: Fn(bool, bool) -> Option<Value>,
        F5: Fn(&str, &str) -> Option<Value>,
        F6: Fn(&Vec<Value>, &Vec<Value>) -> Option<Value>,
        F7: Fn(TimestampType, TimestampType) -> Option<Value>,
        F8: Fn(IntervalType, IntervalType) -> Option<Value>
    >(&self, other: &Value, null_f: F1, int_f: F2, float_f: F3, bool_f: F4, string_f: F5, array_f: F6, timestamp_f: F7, interval_f: F8) -> Option<Value> {
        match (self, other) {
            (Value::Null, _) => null_f(),
            (_, Value::Null) => null_f(),
            (Value::Int(x), Value::Int(y)) => int_f(*x, *y),
            (Value::Float(x), Value::Float(y)) => float_f(x.0, y.0),
            (Value::Bool(x), Value::Bool(y)) => bool_f(*x, *y),
            (Value::String(x), Value::String(y)) => string_f(x, y),
            (Value::Array(_, x), Value::Array(_, y)) => array_f(x, y),
            (Value::Timestamp(x), Value::Timestamp(y)) => timestamp_f(*x, *y),
            (Value::Interval(x), Value::Interval(y)) => interval_f(*x, *y),
            _ => None
        }
    }

    pub fn map<
        F1: Fn() -> Option<Value>,
        F2: Fn(i64) -> Option<i64>,
        F3: Fn(f64) -> Option<f64>,
        F4: Fn(bool) -> Option<bool>,
        F5: Fn(&str) -> Option<String>,
        F6: Fn(&Vec<Value>) -> Option<Vec<Value>>,
        F7: Fn(TimestampType) -> Option<TimestampType>,
        F8: Fn(IntervalType) -> Option<IntervalType>
    >(&self, null_f: F1, int_f: F2, float_f: F3, bool_f: F4, string_f: F5, array_f: F6, timestamp_f: F7, interval_f: F8) -> Option<Value> {
        match self {
            Value::Null => null_f(),
            Value::Int(x) => int_f(*x).map(|x| Value::Int(x)),
            Value::Float(x) => float_f(x.0).map(|x| Value::Float(Float(x))),
            Value::Bool(x) => bool_f(*x).map(|x| Value::Bool(x)),
            Value::String(x) => string_f(x).map(|x| Value::String(x)),
            Value::Array(element, x) => array_f(x).map(|x| Value::Array(element.clone(), x)),
            Value::Timestamp(x) => timestamp_f(*x).map(|x| Value::Timestamp(x)),
            Value::Interval(x) => interval_f(*x).map(|x| Value::Interval(x)),
        }
    }

    pub fn map_numeric<
        F1: Fn(i64) -> Option<i64>,
        F2: Fn(f64) -> Option<f64>,
        F3: Fn(IntervalType) -> Option<IntervalType>
    >(&self, int_f: F1, float_f: F2, interval_f: F3) -> Option<Value> {
        match self {
            Value::Int(x) => int_f(*x).map(|x| Value::Int(x)),
            Value::Float(x) => float_f(x.0).map(|x| Value::Float(Float(x))),
            Value::Interval(x) => interval_f(*x).map(|x| Value::Interval(x)),
            _ => None
        }
    }

    pub fn modify<
        F1: Fn(&mut i64),
        F2: Fn(&mut f64),
        F3: Fn(&mut bool),
        F4: Fn(&mut String),
        F5: Fn(&mut Vec<Value>),
        F6: Fn(&mut TimestampType),
        F7: Fn(&mut IntervalType)
    >(&mut self, int_f: F1, float_f: F2, bool_f: F3, string_f: F4, array_f: F5, timestamp_f: F6, interval_f: F7) {
        match self {
            Value::Null => {},
            Value::Int(x) => int_f(x),
            Value::Float(x) => float_f(&mut x.0),
            Value::Bool(x) => bool_f(x),
            Value::String(x) => string_f(x),
            Value::Array(_, x) => array_f(x),
            Value::Timestamp(x) => timestamp_f(x),
            Value::Interval(x) => interval_f(x)
        }
    }

    pub fn modify_same_type<
        F1: Fn(&mut i64, i64),
        F2: Fn(&mut f64, f64),
        F3: Fn(&mut bool, bool),
        F4: Fn(&mut String, &str),
        F5: Fn(&mut Vec<Value>, &Vec<Value>),
        F6: Fn(&mut TimestampType, TimestampType),
        F7: Fn(&mut IntervalType, IntervalType)
    >(&mut self, value: &Value, int_f: F1, float_f: F2, bool_f: F3, string_f: F4, array_f: F5, timestamp_f: F6, interval_f: F7) {
        match (self, value) {
            (Value::Int(x), Value::Int(y)) => int_f(x, *y),
            (Value::Float(x), Value::Float(y)) => float_f(&mut x.0, y.0),
            (Value::Bool(x), Value::Bool(y)) => bool_f(x, *y),
            (Value::String(x), Value::String(y)) => string_f(x, y),
            (Value::Array(_, x), Value::Array(_, y)) => array_f(x, y),
            (Value::Timestamp(x), Value::Timestamp(y)) => timestamp_f(x, *y),
            (Value::Interval(x), Value::Interval(y)) => interval_f(x, *y),
            _ => {}
        }
    }

    pub fn modify_same_type_numeric<
        F1: Fn(&mut i64, i64),
        F2: Fn(&mut f64, f64),
        F3: Fn(&mut IntervalType, IntervalType)
    >(&mut self, value: &Value, int_f: F1, float_f: F2, interval_f: F3) {
        match (self, value) {
            (Value::Int(x), Value::Int(y)) => int_f(x, *y),
            (Value::Float(x), Value::Float(y)) => float_f(&mut x.0, y.0),
            (Value::Interval(x), Value::Interval(y)) => interval_f(x, *y),
            _ => {}
        }
    }

    pub fn modify_same_type_numeric_nullable<
        F1: Fn(&mut i64, i64),
        F2: Fn(&mut f64, f64),
        F3: Fn(&mut IntervalType, IntervalType)
    >(&mut self, value: &Value, int_f: F1, float_f: F2, interval_f: F3) {
        match (self, value) {
            (Value::Int(x), Value::Int(y)) => int_f(x, *y),
            (Value::Float(x), Value::Float(y)) => float_f(&mut x.0, y.0),
            (Value::Interval(x), Value::Interval(y)) => interval_f(x, *y),
            (x @ Value::Null, Value::Int(y)) => { *x = Value::Int(*y) },
            (x @ Value::Null, Value::Float(y)) => { *x = Value::Float(*y) },
            (x @ Value::Null, Value::Interval(y)) => { *x = Value::Interval(*y) },
            _ => {}
        }
    }

    pub fn json_value(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Int(value) => serde_json::Value::Number(serde_json::Number::from(*value)),
            Value::Float(Float(value)) => serde_json::Value::Number(serde_json::Number::from_f64(*value).unwrap()),
            Value::Bool(value) => serde_json::Value::Bool(*value),
            Value::String(value) => serde_json::Value::String(value.clone()),
            Value::Array(_, value) => serde_json::Value::Array(value.iter().map(|x| x.json_value()).collect()),
            Value::Timestamp(_) => serde_json::Value::String(self.to_string()),
            Value::Interval(_) => serde_json::Value::String(self.to_string())
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Int(x) => write!(f, "{}", x),
            Value::Float(x) => write!(f, "{:.2}", x.0),
            Value::Bool(x) => write!(f, "{}", x),
            Value::String(x) => write!(f, "'{}'", x),
            Value::Array(_, x) => write!(f, "{{{}}}", x.iter().map(|x| format!("{}", x)).join(", ")),
            Value::Timestamp(x) => write!(f, "{}", x.format("%Y-%m-%d %H:%M:%S.%3f")),
            Value::Interval(x) => {
                let seconds = x.num_seconds() % 60;
                let minutes = (x.num_seconds() / 60) % 60;
                let hours = (x.num_seconds() / 60) / 60;
                write!(f, "{:0>2}:{:0>2}:{:0>2}.{:0>3}", hours, minutes, seconds, x.num_milliseconds() - x.num_seconds() * 1000)
            }
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Clone, Hash, Eq, Ord)]
pub enum ValueType {
    Int,
    Float,
    Bool,
    String,
    Array(Box<ValueType>),
    Timestamp,
    Interval
}

impl ValueType {
    pub fn parse(&self, value_str: &str) -> Option<Value> {
        match self {
            ValueType::Int => i64::from_str(value_str).map(|x| Value::Int(x)).ok(),
            ValueType::Float => f64::from_str(value_str).map(|x| Value::Float(Float(x))).ok(),
            ValueType::Bool => bool::from_str(value_str).map(|x| Value::Bool(x)).ok(),
            ValueType::String => Some(Value::String(value_str.to_owned())),
            ValueType::Array(_) => None,
            ValueType::Timestamp => {
                NaiveDateTime::parse_from_str(value_str, "%Y-%m-%d %H:%M:%S")
                    .map(|x| Value::Timestamp(Local {}.from_local_datetime(&x).unwrap()))
                    .ok()
            }
            ValueType::Interval => {
                let parts = value_str.split(":").collect::<Vec<_>>();
                if parts.len() == 3 {
                    let hours = i64::from_str(parts[0]).ok()?;
                    let minutes = i64::from_str(parts[1]).ok()?;
                    let seconds = i64::from_str(parts[2]).ok()?;
                    let duration = IntervalType::hours(hours)
                        .add(IntervalType::minutes(minutes))
                        .add(IntervalType::seconds(seconds));
                    Some(Value::Interval(duration))
                } else {
                    None
                }
            }
        }
    }

    pub fn from_str(text: &str) -> Option<ValueType> {
        if let Some(array_end) = text.rfind("[]") {
            let element_text = &text[0..array_end];
            return Some(ValueType::Array(Box::new(ValueType::from_str(element_text)?)));
        }

        match text {
            "int" => Some(ValueType::Int),
            "real" => Some(ValueType::Float),
            "text" => Some(ValueType::String),
            "boolean" => Some(ValueType::Bool),
            "timestamp" => Some(ValueType::Timestamp),
            "interval" => Some(ValueType::Interval),
            _ => None
        }
    }

    pub fn convert_from_json(&self, value: &serde_json::Value) -> Value {
        match self {
            ValueType::Int => value.as_i64().map(|value| Value::Int(value)),
            ValueType::Float => value.as_f64().map(|value| Value::Float(Float(value))),
            ValueType::Bool => value.as_bool().map(|value| Value::Bool(value)),
            ValueType::String => value.as_str().map(|value| Value::String(value.to_owned())),
            ValueType::Array(element) => {
                value
                    .as_array()
                    .map(|value|
                        Value::Array(
                            *element.clone(),
                            value.iter().map(|x| element.convert_from_json(x)).collect()
                        )
                    )
            }
            ValueType::Timestamp => None,
            ValueType::Interval => None
        }.unwrap_or(Value::Null)
    }

    pub fn default_value(&self) -> Value {
        match self {
            ValueType::Int => Value::Int(0),
            ValueType::Float => Value::Float(Float(0.0)),
            ValueType::Bool => Value::Bool(false),
            ValueType::String => Value::String(String::new()),
            ValueType::Array(element) => Value::Array(*element.clone(), Vec::new()),
            ValueType::Timestamp => Value::Timestamp(create_timestamp(2000, 1, 1, 0, 0, 0, 0).unwrap()),
            ValueType::Interval => Value::Interval(IntervalType::seconds(0))
        }
    }
}

impl std::fmt::Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueType::Int => write!(f, "int"),
            ValueType::Float => write!(f, "real"),
            ValueType::Bool => write!(f, "boolean"),
            ValueType::String => write!(f, "text"),
            ValueType::Array(element) => write!(f, "{}[]", element),
            ValueType::Timestamp => write!(f, "timestamp"),
            ValueType::Interval => write!(f, "interval")
        }
    }
}

pub fn value_type_to_string(value: &Option<ValueType>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => "NULL".to_owned()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Float(pub f64);

impl Eq for Float {}
impl Ord for Float {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.0 < other.0 {
            Ordering::Less
        } else if self.0 > other.0 {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }
}

impl Hash for Float {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let bits: u64 = unsafe { std::mem::transmute(self.0) };
        bits.hash(state)
    }
}

impl std::fmt::Display for Float {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<f64> for Float {
    fn from(x: f64) -> Self {
        Float(x)
    }
}

#[derive(Debug, PartialEq, Hash, Clone)]
pub enum ExpressionTree {
    Value(Value),
    ColumnAccess(String),
    ScopedColumnAccess(ColumnScope, String),
    Wildcard,
    Compare { operator: CompareOperator, left: Box<ExpressionTree>, right: Box<ExpressionTree> },
    NullableCompare { operator: NullableCompareOperator, left: Box<ExpressionTree>, right: Box<ExpressionTree> },
    Arithmetic { operator: ArithmeticOperator, left: Box<ExpressionTree>, right: Box<ExpressionTree> },
    BooleanOperation { operator: BooleanOperator, left: Box<ExpressionTree>, right: Box<ExpressionTree> },
    UnaryArithmetic { operator: UnaryArithmeticOperator, operand: Box<ExpressionTree> },
    In { is_not: bool, operand: Box<ExpressionTree>, values: Vec<ExpressionTree> },
    FunctionCall { function: Function, arguments: Vec<ExpressionTree> },
    ArrayElementAccess { array: Box<ExpressionTree>, index: Box<ExpressionTree> },
    TypeConversion { operand: Box<ExpressionTree>, convert_to_type: ValueType },
    Case { clauses: Vec<(ExpressionTree, ExpressionTree)>, else_clause: Box<ExpressionTree> },
    Aggregate(usize, Box<Aggregate>)
}

impl ExpressionTree {
    pub fn visit<'a, E, F: FnMut(&'a ExpressionTree) -> Result<(), E>>(&'a self, f: &mut F) -> Result<(), E> {
        match self {
            ExpressionTree::Value(_) => {}
            ExpressionTree::ColumnAccess(_) => {}
            ExpressionTree::ScopedColumnAccess(_, _) => {}
            ExpressionTree::Wildcard => {}
            ExpressionTree::Compare { left, right, .. } => {
                left.visit(f)?;
                right.visit(f)?;
            }
            ExpressionTree::NullableCompare { left, right, .. } => {
                left.visit(f)?;
                right.visit(f)?;
            }
            ExpressionTree::Arithmetic { left, right, .. } => {
                left.visit(f)?;
                right.visit(f)?;
            }
            ExpressionTree::BooleanOperation { left, right, .. } => {
                left.visit(f)?;
                right.visit(f)?;
            }
            ExpressionTree::UnaryArithmetic { operand, .. } => {
                operand.visit(f)?;
            }
            ExpressionTree::In { operand, values, .. } => {
                operand.visit(f)?;

                for value in values {
                    value.visit(f)?;
                }
            }
            ExpressionTree::FunctionCall { arguments, .. } => {
                for arg in arguments {
                    arg.visit(f)?;
                }
            }
            ExpressionTree::ArrayElementAccess { array, index } => {
                array.visit(f)?;
                index.visit(f)?;
            }
            ExpressionTree::TypeConversion { operand, .. } => {
                operand.visit(f)?;
            }
            ExpressionTree::Case { clauses, else_clause } => {
                for clause in clauses {
                    clause.0.visit(f)?;
                    clause.1.visit(f)?;
                }

                else_clause.visit(f)?;
            }
            ExpressionTree::Aggregate(_, aggregate) => {
                match aggregate.as_ref() {
                    Aggregate::GroupKey(_) | Aggregate::Count(_, _) => {}
                    Aggregate::Min(expression) => {
                        expression.visit(f)?;
                    }
                    Aggregate::Max(expression) => {
                        expression.visit(f)?;
                    }
                    Aggregate::Average(expression) => {
                        expression.visit(f)?;
                    }
                    Aggregate::Sum(expression) => {
                        expression.visit(f)?;
                    }
                    Aggregate::StandardDeviation(expression, _) => {
                        expression.visit(f)?;
                    }
                    Aggregate::Percentile(expression, _) => {
                        expression.visit(f)?;
                    }
                    Aggregate::BoolAnd(expression) => {
                        expression.visit(f)?;
                    }
                    Aggregate::BoolOr(expression) => {
                        expression.visit(f)?;
                    }
                    Aggregate::CollectArray(expression) => {
                        expression.visit(f)?;
                    }
                }
            }
        }

        f(self)?;
        Ok(())
    }
}

impl std::fmt::Display for ExpressionTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        ExpressionTreeVisualizer {}.fmt(self, f)
    }
}

#[derive(Debug, PartialEq, Hash, Clone)]
pub enum CompareOperator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual
}

impl std::fmt::Display for CompareOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompareOperator::Equal => write!(f, "="),
            CompareOperator::NotEqual => write!(f, "!="),
            CompareOperator::GreaterThan => write!(f, ">"),
            CompareOperator::GreaterThanOrEqual => write!(f, ">="),
            CompareOperator::LessThan => write!(f, "<"),
            CompareOperator::LessThanOrEqual => write!(f, "<=")
        }
    }
}

#[derive(Debug, PartialEq, Hash, Clone)]
pub enum NullableCompareOperator {
    Equal,
    NotEqual
}

impl std::fmt::Display for NullableCompareOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NullableCompareOperator::Equal => write!(f, "IS"),
            NullableCompareOperator::NotEqual => write!(f, "IS NOT")
        }
    }
}

#[derive(Debug, PartialEq, Hash, Clone)]
pub enum ArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide
}

impl std::fmt::Display for ArithmeticOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArithmeticOperator::Add => write!(f, "+"),
            ArithmeticOperator::Subtract => write!(f, "-"),
            ArithmeticOperator::Multiply => write!(f, "*"),
            ArithmeticOperator::Divide => write!(f, "/"),
        }
    }
}


#[derive(Debug, PartialEq, Hash, Clone)]
pub enum BooleanOperator {
    And,
    Or
}

impl std::fmt::Display for BooleanOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BooleanOperator::And => write!(f, "AND"),
            BooleanOperator::Or => write!(f, "OR"),
        }
    }
}

#[derive(Debug, PartialEq, Hash, Clone)]
pub enum UnaryArithmeticOperator {
    Negative,
    Invert
}

impl std::fmt::Display for UnaryArithmeticOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnaryArithmeticOperator::Negative => write!(f, "-"),
            UnaryArithmeticOperator::Invert => write!(f, "NOT"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Hash)]
pub enum Function {
    Greatest,
    Least,
    Abs,
    Sqrt,
    Pow,
    StringLength,
    StringToUpper,
    StringToLower,
    RegexMatches,
    CreateArray,
    ArrayUnique,
    ArrayLength,
    ArrayCat,
    ArrayAppend,
    ArrayPrepend,
    TimestampNow,
    MakeTimestamp,
    TimestampExtractEpoch,
    TimestampExtractYear,
    TimestampExtractMonth,
    TimestampExtractDay,
    TimestampExtractHour,
    TimestampExtractSecond,
    TimestampExtractMinute,
    TruncateTimestamp
}

impl std::fmt::Display for Function {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Function::Greatest => write!(f, "greatest"),
            Function::Least => write!(f, "least"),
            Function::Abs => write!(f, "abs"),
            Function::Sqrt => write!(f, "sqrt"),
            Function::Pow => write!(f, "pow"),
            Function::StringLength => write!(f, "length"),
            Function::StringToUpper => write!(f, "upper"),
            Function::StringToLower => write!(f, "lower"),
            Function::RegexMatches => write!(f, "regex_matches"),
            Function::CreateArray => write!(f, "create_array"),
            Function::ArrayUnique => write!(f, "array_unique"),
            Function::ArrayLength => write!(f, "array_length"),
            Function::ArrayCat => write!(f, "array_length"),
            Function::ArrayAppend => write!(f, "array_append"),
            Function::ArrayPrepend => write!(f, "array_prepend"),
            Function::TimestampNow => write!(f, "now"),
            Function::MakeTimestamp => write!(f, "make_timestamp"),
            Function::TimestampExtractEpoch => write!(f, "timestamp_extract_epoch"),
            Function::TimestampExtractYear => write!(f, "timestamp_extract_year"),
            Function::TimestampExtractMonth => write!(f, "timestamp_extract_month"),
            Function::TimestampExtractDay => write!(f, "timestamp_extract_day"),
            Function::TimestampExtractHour => write!(f, "timestamp_extract_hour"),
            Function::TimestampExtractSecond => write!(f, "timestamp_extract_second"),
            Function::TimestampExtractMinute => write!(f, "timestamp_extract_minute"),
            Function::TruncateTimestamp => write!(f, "date_trunc"),
        }
    }
}

#[derive(Debug, PartialEq, Hash, Clone)]
pub enum Aggregate {
    GroupKey(ExpressionTree),
    Count(Option<String>, bool),
    Min(ExpressionTree),
    Max(ExpressionTree),
    Sum(ExpressionTree),
    Average(ExpressionTree),
    StandardDeviation(ExpressionTree, bool),
    Percentile(ExpressionTree, Float),
    BoolAnd(ExpressionTree),
    BoolOr(ExpressionTree),
    CollectArray(ExpressionTree)
}

struct ExpressionTreeVisualizer {

}

impl ExpressionTreeVisualizer {
    fn fmt(&self, value: &ExpressionTree, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match value {
            ExpressionTree::Value(value) => {
                write!(f, "{}", value)
            }
            ExpressionTree::ColumnAccess(name) => {
                write!(f, "{}", name)
            }
            ExpressionTree::ScopedColumnAccess(scope, name) => {
                match scope {
                    ColumnScope::Table => write!(f, "{}", name),
                    ColumnScope::AggregationValue => write!(f, "AggregationValue({})", name),
                    ColumnScope::GroupKey => write!(f, "GroupKey({})", name),
                    ColumnScope::GroupValue => write!(f, "GroupValue({})", name)
                }
            }
            ExpressionTree::Wildcard => {
                write!(f, "*")
            }
            ExpressionTree::Arithmetic { operator, left, right } => {
                write!(f, "(")?;
                self.fmt(&left, f)?;
                write!(f, " {} ", operator)?;
                self.fmt(&right, f)?;
                write!(f, ")")?;
                Ok(())
            }
            ExpressionTree::BooleanOperation { operator, left, right } => {
                write!(f, "(")?;
                self.fmt(&left, f)?;
                write!(f, " {} ", operator)?;
                self.fmt(&right, f)?;
                write!(f, ")")?;
                Ok(())
            }
            ExpressionTree::UnaryArithmetic { operator, operand } => {
                write!(f, "{}", operator)?;
                self.fmt(&operand, f)?;
                Ok(())
            }
            ExpressionTree::Compare { operator, left, right } => {
                write!(f, "(")?;
                self.fmt(&left, f)?;
                write!(f, " {} ", operator)?;
                self.fmt(&right, f)?;
                write!(f, ")")?;
                Ok(())
            }
            ExpressionTree::NullableCompare { operator, left, right } => {
                write!(f, "(")?;
                self.fmt(&left, f)?;
                write!(f, " {} ", operator)?;
                self.fmt(&right, f)?;
                write!(f, ")")?;
                Ok(())
            }
            ExpressionTree::In { is_not, operand, values } => {
                write!(f, "(")?;
                self.fmt(&operand, f)?;
                if *is_not {
                    write!(f, " NOT IN ")?;
                } else {
                    write!(f, " IN ")?;
                }

                write!(f, "(")?;
                let mut is_first = true;
                for value in values {
                    if !is_first {
                        write!(f, ", ")?;
                    } else {
                        is_first = false;
                    }

                    self.fmt(&value, f)?;
                }
                write!(f, ")")?;

                write!(f, ")")?;
                Ok(())
            }
            ExpressionTree::FunctionCall { function, arguments } => {
                write!(f, "{}", function)?;
                write!(f, "(")?;
                let mut is_first = true;
                for argument in arguments {
                    if !is_first {
                        write!(f, ", ")?;
                    } else {
                        is_first = false;
                    }

                    self.fmt(&argument, f)?;
                }

                write!(f, ")")?;
                Ok(())
            }
            ExpressionTree::ArrayElementAccess { array, index } => {
                self.fmt(&array, f)?;
                write!(f, "[")?;
                self.fmt(&index, f)?;
                write!(f, "]")?;
                Ok(())
            }
            ExpressionTree::TypeConversion { operand, convert_to_type } => {
                self.fmt(&operand, f)?;
                write!(f, "::{}", convert_to_type)?;
                Ok(())
            }
            ExpressionTree::Case { clauses, else_clause } => {
                write!(f, "(CASE ")?;
                for clause in clauses {
                    write!(f, "WHEN ")?;
                    self.fmt(&clause.0, f)?;
                    write!(f, " THEN ")?;
                    self.fmt(&clause.1, f)?;
                    write!(f, " ")?;
                }
                write!(f, "ELSE ")?;
                self.fmt(&else_clause, f)?;
                write!(f, " END)")?;
                Ok(())
            }
            ExpressionTree::Aggregate(_, aggregate) => {
                match aggregate.as_ref() {
                    Aggregate::GroupKey(expression) => self.fmt(&expression, f),
                    Aggregate::Count(name, distinct) => {
                        if *distinct {
                            write!(f, "COUNT(DISTINCT {})", name.as_ref().unwrap_or(&String::new()))
                        } else {
                            write!(f, "COUNT({})", name.as_ref().unwrap_or(&String::new()))
                        }
                    }
                    Aggregate::Min(expression) => {
                        write!(f, "MIN(")?;
                        self.fmt(&expression, f)?;
                        write!(f, ")")?;
                        Ok(())
                    }
                    Aggregate::Max(expression) => {
                        write!(f, "MAX(")?;
                        self.fmt(&expression, f)?;
                        write!(f, ")")?;
                        Ok(())
                    }
                    Aggregate::Sum(expression) => {
                        write!(f, "SUM(")?;
                        self.fmt(&expression, f)?;
                        write!(f, ")")?;
                        Ok(())
                    }
                    Aggregate::Average(expression) => {
                        write!(f, "AVG(")?;
                        self.fmt(&expression, f)?;
                        write!(f, ")")?;
                        Ok(())
                    }
                    Aggregate::StandardDeviation(expression, is_variance) => {
                        if *is_variance {
                            write!(f, "VARIANCE(")?;
                        } else {
                            write!(f, "STDDEV(")?;
                        }
                        self.fmt(&expression, f)?;
                        write!(f, ")")?;
                        Ok(())
                    }
                    Aggregate::Percentile(expression, percentile) => {
                        write!(f, "PERCENTILE(")?;
                        self.fmt(&expression, f)?;
                        write!(f, ", {})", percentile)?;
                        Ok(())
                    }
                    Aggregate::BoolAnd(expression) => {
                        write!(f, "BOOL_AND(")?;
                        self.fmt(&expression, f)?;
                        write!(f, ")")?;
                        Ok(())
                    }
                    Aggregate::BoolOr(expression) => {
                        write!(f, "BOOL_OR(")?;
                        self.fmt(&expression, f)?;
                        write!(f, ")")?;
                        Ok(())
                    }
                    Aggregate::CollectArray(expression) => {
                        write!(f, "ARRAY_AGG(")?;
                        self.fmt(&expression, f)?;
                        write!(f, ")")?;
                        Ok(())
                    }
                }
            }
        }
    }
}

pub fn create_timestamp(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32, microsecond: u32) -> Option<TimestampType> {
    let timestamp = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(year, month, day)?,
        NaiveTime::from_hms_micro_opt(hour, minute, second, microsecond)?
    );

    Local {}.from_local_datetime(&timestamp).latest()
}

#[test]
fn test_parse_type1() {
    assert_eq!(Some(ValueType::Float), ValueType::from_str("real"));
    assert_eq!(Some(ValueType::Array(Box::new(ValueType::Float))), ValueType::from_str("real[]"));
    assert_eq!(Some(ValueType::Array(Box::new(ValueType::Array(Box::new(ValueType::Float))))), ValueType::from_str("real[][]"));
}
