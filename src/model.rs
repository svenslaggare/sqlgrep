use std::str::FromStr;
use std::fmt::Formatter;
use crate::data_model::TableDefinition;

#[derive(Debug, PartialEq, PartialOrd, Clone, Eq, Hash, Ord)]
pub enum Value {
    Null,
    Int(i64),
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

    pub fn map_same_type<
        F1: Fn(i64, i64) -> Option<i64>,
        F2: Fn(bool, bool) -> Option<bool>,
        F3: Fn(&str, &str) -> Option<String>
    >(&self, other: &Value, int_f: F1, bool_f: F2, string_f: F3) -> Option<Value> {
        match (self, other) {
            (Value::Int(x), Value::Int(y)) => int_f(*x, *y).map(|x| Value::Int(x)),
            (Value::Bool(x), Value::Bool(y)) => bool_f(*x, *y).map(|x| Value::Bool(x)),
            (Value::String(x), Value::String(y)) => string_f(x, y).map(|x| Value::String(x)),
            _ => None
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Int(x) => write!(f, "{}", x),
            Value::Bool(x) => write!(f, "{}", x),
            Value::String(x) => write!(f, "{}", x)
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum ValueType {
    Int,
    Bool,
    String
}

impl ValueType {
    pub fn parse(&self, value_str: &str) -> Option<Value> {
        match self {
            ValueType::Int => i64::from_str(value_str).map(|x| Value::Int(x)).ok(),
            ValueType::Bool => bool::from_str(value_str).map(|x| Value::Bool(x)).ok(),
            ValueType::String => Some(Value::String(value_str.to_owned()))
        }
    }

    pub fn from_str(text: &str) -> Option<ValueType> {
        match text {
            "int" => Some(ValueType::Int),
            "text" => Some(ValueType::String),
            "bool" => Some(ValueType::Bool),
            _ => None
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
pub enum ExpressionTree {
    Value(Value),
    ColumnAccess(String),
    Compare { left: Box<ExpressionTree>, right: Box<ExpressionTree>, operator: CompareOperator },
    And { left: Box<ExpressionTree>, right: Box<ExpressionTree> },
    Or { left: Box<ExpressionTree>, right: Box<ExpressionTree> },
    Arithmetic { left: Box<ExpressionTree>, right: Box<ExpressionTree>, operator: ArithmeticOperator }
}

#[derive(Debug, PartialEq)]
pub enum Aggregate {
    GroupKey,
    Count,
    Min(ExpressionTree),
    Max(ExpressionTree)
}

pub struct SelectStatement {
    pub projections: Vec<(String, ExpressionTree)>,
    pub from: String,
    pub filename: Option<String>,
    pub filter: Option<ExpressionTree>
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
    CreateTable(TableDefinition)
}

impl Statement {
    pub fn filename(&self) -> Option<&str> {
        match self {
            Statement::Select(statement) => statement.filename.as_ref().map(|x| x.as_str()),
            Statement::Aggregate(statement) => statement.filename.as_ref().map(|x| x.as_str()),
            Statement::CreateTable(_) => None
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
}