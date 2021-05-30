use std::str::FromStr;

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use itertools::Itertools;

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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    String(String),
    Array(ValueType, Vec<Value>)
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
            Value::Array(element, _) => Some(ValueType::Array(Box::new(element.clone())))
        }
    }

    pub fn map_same_type<
        F1: Fn() -> Option<Value>,
        F2: Fn(i64, i64) -> Option<i64>,
        F3: Fn(f64, f64) -> Option<f64>,
        F4: Fn(bool, bool) -> Option<bool>,
        F5: Fn(&str, &str) -> Option<String>,
        F6: Fn(&Vec<Value>, &Vec<Value>) -> Option<Vec<Value>>,
    >(&self, other: &Value, null_f: F1, int_f: F2, float_f: F3, bool_f: F4, string_f: F5, array_f: F6) -> Option<Value> {
        match (self, other) {
            (Value::Null, _) => null_f(),
            (_, Value::Null) => null_f(),
            (Value::Int(x), Value::Int(y)) => int_f(*x, *y).map(|x| Value::Int(x)),
            (Value::Float(x), Value::Float(y)) => float_f(x.0, y.0).map(|x| Value::Float(Float(x))),
            (Value::Bool(x), Value::Bool(y)) => bool_f(*x, *y).map(|x| Value::Bool(x)),
            (Value::String(x), Value::String(y)) => string_f(x, y).map(|x| Value::String(x)),
            (Value::Array(element, x), Value::Array(_, y)) => array_f(x, y).map(|x| Value::Array(element.clone(), x)),
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
    >(&self, null_f: F1, int_f: F2, float_f: F3, bool_f: F4, string_f: F5, array_f: F6) -> Option<Value> {
        match self {
            Value::Null => null_f(),
            Value::Int(x) => int_f(*x).map(|x| Value::Int(x)),
            Value::Float(x) => float_f(x.0).map(|x| Value::Float(Float(x))),
            Value::Bool(x) => bool_f(*x).map(|x| Value::Bool(x)),
            Value::String(x) => string_f(x).map(|x| Value::String(x)),
            Value::Array(element, x) => array_f(x).map(|x| Value::Array(element.clone(), x)),
        }
    }

    pub fn modify<
        F1: Fn(&mut i64),
        F2: Fn(&mut f64),
        F3: Fn(&mut bool),
        F4: Fn(&mut String),
        F5: Fn(&mut Vec<Value>),
    >(&mut self, int_f: F1, float_f: F2, bool_f: F3, string_f: F4, array_f: F5) {
        match self {
            Value::Null => {},
            Value::Int(x) => int_f(x),
            Value::Float(x) => float_f(&mut x.0),
            Value::Bool(x) => bool_f(x),
            Value::String(x) => string_f(x),
            Value::Array(_, x) => array_f(x)
        }
    }

    pub fn modify_same_type<
        F1: Fn(&mut i64, i64),
        F2: Fn(&mut f64, f64),
        F3: Fn(&mut bool, bool),
        F4: Fn(&mut String, &str),
        F5: Fn(&mut Vec<Value>, &Vec<Value>)
    >(&mut self, value: &Value, int_f: F1, float_f: F2, bool_f: F3, string_f: F4, array_f: F5) {
        match (self, value) {
            (Value::Int(x), Value::Int(y)) => int_f(x, *y),
            (Value::Float(x), Value::Float(y)) => float_f(&mut x.0, y.0),
            (Value::Bool(x), Value::Bool(y)) => bool_f(x, *y),
            (Value::String(x), Value::String(y)) => string_f(x, y),
            (Value::Array(_, x), Value::Array(_, y)) => array_f(x, y),
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
            Value::Array(_, value) => serde_json::Value::Array(value.iter().map(|x| x.json_value()).collect())
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
            Value::String(x) => write!(f, "{}", x),
            Value::Array(_, x) => write!(f, "{{{}}}", x.iter().map(|x| format!("{}", x)).join(", "))
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Clone, Hash, Eq, Ord)]
pub enum ValueType {
    Int,
    Float,
    Bool,
    String,
    Array(Box<ValueType>)
}

impl ValueType {
    pub fn parse(&self, value_str: &str) -> Option<Value> {
        match self {
            ValueType::Int => i64::from_str(value_str).map(|x| Value::Int(x)).ok(),
            ValueType::Float => f64::from_str(value_str).map(|x| Value::Float(Float(x))).ok(),
            ValueType::Bool => bool::from_str(value_str).map(|x| Value::Bool(x)).ok(),
            ValueType::String => Some(Value::String(value_str.to_owned())),
            ValueType::Array(_) => None
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
            _ => None
        }
    }

    pub fn convert_json(&self, value: &serde_json::Value) -> Value {
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
                            value.iter().map(|x| element.convert_json(x)).collect()
                        )
                    )
            }
        }.unwrap_or(Value::Null)
    }

    pub fn default_value(&self) -> Value {
        match self {
            ValueType::Int => Value::Int(0),
            ValueType::Float => Value::Float(Float(0.0)),
            ValueType::Bool => Value::Bool(false),
            ValueType::String => Value::String(String::new()),
            ValueType::Array(element) => Value::Array(*element.clone(), Vec::new())
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
            ValueType::Array(element) => write!(f, "{}[]", element)
        }
    }
}

pub fn value_type_to_string(value: &Option<ValueType>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => "NULL".to_owned()
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

#[derive(Debug, PartialEq, Hash, Clone)]
pub enum ArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide
}

#[derive(Debug, PartialEq, Hash, Clone)]
pub enum UnaryArithmeticOperator {
    Negative,
    Invert
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
    ArrayUnique,
    ArrayLength
}

#[derive(Debug, PartialEq, Hash, Clone)]
pub enum Aggregate {
    GroupKey(String),
    Count(Option<String>),
    Min(ExpressionTree),
    Max(ExpressionTree),
    Average(ExpressionTree),
    Sum(ExpressionTree),
    CollectArray(ExpressionTree, bool)
}

#[derive(Debug, PartialEq, Hash, Clone)]
pub enum ExpressionTree {
    Value(Value),
    ColumnAccess(String),
    Wildcard,
    Compare { operator: CompareOperator, left: Box<ExpressionTree>, right: Box<ExpressionTree> },
    Is { left: Box<ExpressionTree>, right: Box<ExpressionTree> },
    IsNot { left: Box<ExpressionTree>, right: Box<ExpressionTree> },
    And { left: Box<ExpressionTree>, right: Box<ExpressionTree> },
    Or { left: Box<ExpressionTree>, right: Box<ExpressionTree> },
    Arithmetic { operator: ArithmeticOperator, left: Box<ExpressionTree>, right: Box<ExpressionTree> },
    UnaryArithmetic { operator: UnaryArithmeticOperator, operand: Box<ExpressionTree> },
    Function { function: Function, arguments: Vec<ExpressionTree> },
    ArrayElementAccess { array: Box<ExpressionTree>, index: Box<ExpressionTree> },
    Aggregate(usize, Box<Aggregate>)
}

impl ExpressionTree {
    pub fn visit<'a, E, F: FnMut(&'a ExpressionTree) -> Result<(), E>>(&'a self, f: &mut F) -> Result<(), E> {
        match self {
            ExpressionTree::Value(_) => {}
            ExpressionTree::ColumnAccess(_) => {}
            ExpressionTree::Wildcard => {}
            ExpressionTree::Compare { left, right, .. } => {
                left.visit(f)?;
                right.visit(f)?;
            }
            ExpressionTree::Is { left, right } => {
                left.visit(f)?;
                right.visit(f)?;
            }
            ExpressionTree::IsNot { left, right } => {
                left.visit(f)?;
                right.visit(f)?;
            }
            ExpressionTree::And { left, right } => {
                left.visit(f)?;
                right.visit(f)?;
            }
            ExpressionTree::Or { left, right } => {
                left.visit(f)?;
                right.visit(f)?;
            }
            ExpressionTree::Arithmetic { left, right, .. } => {
                left.visit(f)?;
                right.visit(f)?;
            }
            ExpressionTree::UnaryArithmetic { operand, .. } => {
                operand.visit(f)?;
            }
            ExpressionTree::Function { arguments, .. } => {
                for arg in arguments {
                    arg.visit(f)?;
                }
            }
            ExpressionTree::ArrayElementAccess { array, index } => {
                array.visit(f)?;
                index.visit(f)?;
            }
            ExpressionTree::Aggregate(_, aggregate) => {
                match aggregate.as_ref() {
                    Aggregate::GroupKey(_) | Aggregate::Count(_) => {}
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
                    Aggregate::CollectArray(expression, _) => {
                        expression.visit(f)?;
                    }
                }
            }
        }

        f(self)?;
        Ok(())
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

#[derive(Debug)]
pub struct SelectStatement {
    pub projections: Vec<(String, ExpressionTree)>,
    pub from: String,
    pub filename: Option<String>,
    pub filter: Option<ExpressionTree>,
    pub join: Option<JoinClause>
}

impl SelectStatement {
    pub fn is_wildcard_projection(&self) -> bool {
        self.projections.len() == 1 && self.projections[0].1 == ExpressionTree::Wildcard
    }
}

#[derive(Debug, Hash)]
pub struct AggregateStatement {
    pub aggregates: Vec<(String, Aggregate)>,
    pub from: String,
    pub filename: Option<String>,
    pub filter: Option<ExpressionTree>,
    pub group_by: Option<Vec<String>>,
    pub having: Option<ExpressionTree>,
    pub join: Option<JoinClause>
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

#[test]
fn test_parse_type1() {
    assert_eq!(Some(ValueType::Float), ValueType::from_str("real"));
    assert_eq!(Some(ValueType::Array(Box::new(ValueType::Float))), ValueType::from_str("real[]"));
    assert_eq!(Some(ValueType::Array(Box::new(ValueType::Array(Box::new(ValueType::Float))))), ValueType::from_str("real[][]"));
}