use std::str::FromStr;
use std::fmt::Formatter;

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use crate::data_model::TableDefinition;

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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<f64> for Float {
    fn from(x: f64) -> Self {
        Float(x)
    }
}

#[derive(Debug, PartialEq, PartialOrd, Clone, Eq, Hash, Ord)]
pub enum Value {
    Null,
    Int(i64),
    Float(Float),
    Bool(bool),
    String(String)
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

    pub fn int(&self) -> Option<i64> {
        match self {
            Value::Int(x) => Some(*x),
            _ => None
        }
    }

    pub fn int_mut(&mut self) -> Option<&mut i64> {
        match self {
            Value::Int(x) => Some(x),
            _ => None
        }
    }

    pub fn value_type(&self) -> Option<ValueType> {
        match self {
            Value::Null => None,
            Value::Int(_) => Some(ValueType::Int),
            Value::Float(_) => Some(ValueType::Float),
            Value::Bool(_) => Some(ValueType::Bool),
            Value::String(_) => Some(ValueType::String)
        }
    }

    pub fn map_same_type<
        F1: Fn(i64, i64) -> Option<i64>,
        F2: Fn(f64, f64) -> Option<f64>,
        F3: Fn(bool, bool) -> Option<bool>,
        F4: Fn(&str, &str) -> Option<String>
    >(&self, other: &Value, int_f: F1, float_f: F2, bool_f: F3, string_f: F4) -> Option<Value> {
        match (self, other) {
            (Value::Int(x), Value::Int(y)) => int_f(*x, *y).map(|x| Value::Int(x)),
            (Value::Float(x), Value::Float(y)) => float_f(x.0, y.0).map(|x| Value::Float(Float(x))),
            (Value::Bool(x), Value::Bool(y)) => bool_f(*x, *y).map(|x| Value::Bool(x)),
            (Value::String(x), Value::String(y)) => string_f(x, y).map(|x| Value::String(x)),
            _ => None
        }
    }

    pub fn map<
        F1: Fn() -> Option<Value>,
        F2: Fn(i64) -> Option<i64>,
        F3: Fn(f64) -> Option<f64>,
        F4: Fn(bool) -> Option<bool>,
        F5: Fn(&str) -> Option<String>,
    >(&self, null_f: F1, int_f: F2, float_f: F3, bool_f: F4, string_f: F5) -> Option<Value> {
        match self {
            Value::Null => null_f(),
            Value::Int(x) => int_f(*x).map(|x| Value::Int(x)),
            Value::Float(x) => float_f(x.0).map(|x| Value::Float(Float(x))),
            Value::Bool(x) => bool_f(*x).map(|x| Value::Bool(x)),
            Value::String(x) => string_f(x).map(|x| Value::String(x)),
        }
    }

    pub fn modify<
        F1: Fn(&mut i64),
        F2: Fn(&mut f64),
        F3: Fn(&mut bool),
        F4: Fn(&mut String)
    >(&mut self, int_f: F1, float_f: F2, bool_f: F3, string_f: F4) {
        match self {
            Value::Null => {},
            Value::Int(x) => int_f(x),
            Value::Float(x) => float_f(&mut x.0),
            Value::Bool(x) => bool_f(x),
            Value::String(x) => string_f(x)
        }
    }

    pub fn modify_same_type<
        F1: Fn(&mut i64, i64),
        F2: Fn(&mut f64, f64),
        F3: Fn(&mut bool, bool),
        F4: Fn(&mut String, &str)
    >(&mut self, value: &Value, int_f: F1, float_f: F2, bool_f: F3, string_f: F4) {
        match (self, value) {
            (Value::Int(x), Value::Int(y)) => int_f(x, *y),
            (Value::Float(x), Value::Float(y)) => float_f(&mut x.0, y.0),
            (Value::Bool(x), Value::Bool(y)) => bool_f(x, *y),
            (Value::String(x), Value::String(y)) => string_f(x, y),
            _ => {}
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Int(x) => write!(f, "{}", x),
            Value::Float(x) => write!(f, "{:.2}", x.0),
            Value::Bool(x) => write!(f, "{}", x),
            Value::String(x) => write!(f, "{}", x)
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum ValueType {
    Int,
    Float,
    Bool,
    String
}

impl ValueType {
    pub fn parse(&self, value_str: &str) -> Option<Value> {
        match self {
            ValueType::Int => i64::from_str(value_str).map(|x| Value::Int(x)).ok(),
            ValueType::Float => f64::from_str(value_str).map(|x| Value::Float(Float(x))).ok(),
            ValueType::Bool => bool::from_str(value_str).map(|x| Value::Bool(x)).ok(),
            ValueType::String => Some(Value::String(value_str.to_owned()))
        }
    }

    pub fn from_str(text: &str) -> Option<ValueType> {
        match text {
            "int" => Some(ValueType::Int),
            "real" => Some(ValueType::Float),
            "text" => Some(ValueType::String),
            "boolean" => Some(ValueType::Bool),
            _ => None
        }
    }

    pub fn default_value(&self) -> Value {
        match self {
            ValueType::Int => Value::Int(0),
            ValueType::Float => Value::Float(Float(0.0)),
            ValueType::Bool => Value::Bool(false),
            ValueType::String => Value::String(String::new())
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum CompareOperator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual
}

#[derive(Debug, PartialEq)]
pub enum ArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide
}

#[derive(Debug, PartialEq)]
pub enum UnaryArithmeticOperator {
    Negative,
    Invert
}

#[derive(Debug, PartialEq)]
pub enum ExpressionTree {
    Value(Value),
    ColumnAccess(String),
    Wildcard,
    Compare { left: Box<ExpressionTree>, right: Box<ExpressionTree>, operator: CompareOperator },
    And { left: Box<ExpressionTree>, right: Box<ExpressionTree> },
    Or { left: Box<ExpressionTree>, right: Box<ExpressionTree> },
    Arithmetic { left: Box<ExpressionTree>, right: Box<ExpressionTree>, operator: ArithmeticOperator },
    UnaryArithmetic { operand: Box<ExpressionTree>, operator: UnaryArithmeticOperator }
}

#[derive(Debug, PartialEq)]
pub enum Aggregate {
    GroupKey,
    Count,
    Min(ExpressionTree),
    Max(ExpressionTree),
    Average(ExpressionTree),
    Sum(ExpressionTree)
}

pub struct SelectStatement {
    pub projections: Vec<(String, ExpressionTree)>,
    pub from: String,
    pub filename: Option<String>,
    pub filter: Option<ExpressionTree>
}

impl SelectStatement {
    pub fn is_wildcard_projection(&self) -> bool {
        self.projections.len() == 1 && self.projections[0].1 == ExpressionTree::Wildcard
    }
}

pub struct AggregateStatement {
    pub aggregates: Vec<(String, Aggregate)>,
    pub from: String,
    pub filename: Option<String>,
    pub filter: Option<ExpressionTree>,
    pub group_by: Option<String>
}

pub struct CreateTableStatement {
    pub name: String,
    pub patterns: Vec<(String, String)>,
    pub columns: Vec<(String, ValueType, String, usize)>
}

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
}