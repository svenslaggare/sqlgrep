use std::str::FromStr;
use std::collections::HashMap;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, PartialOrd, Clone)]
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
}

#[derive(Debug)]
pub enum CompareOperator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual
}

#[derive(Debug)]
pub enum ArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide
}

#[derive(Debug)]
pub enum ExpressionTree {
    Value(Value),
    ColumnAccess(String),
    Compare { left: Box<ExpressionTree>, right: Box<ExpressionTree>, operator: CompareOperator },
    And { left: Box<ExpressionTree>, right: Box<ExpressionTree> },
    Or { left: Box<ExpressionTree>, right: Box<ExpressionTree> },
    Arithmetic { left: Box<ExpressionTree>, right: Box<ExpressionTree>, operator: ArithmeticOperator }
}

pub struct SelectStatement {
    pub projection: Vec<(String, ExpressionTree)>,
    pub from: String,
    pub filter: Option<ExpressionTree>
}

impl SelectStatement {
    pub fn create_projection_name_mapping(&self) -> HashMap<String, usize> {
        let mut name_mapping = HashMap::new();
        for (index, projection) in self.projection.iter().enumerate() {
            name_mapping.insert(projection.0.clone(), index);
        }

        name_mapping
    }
}