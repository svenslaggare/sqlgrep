use std::hash::{Hasher, Hash};
use std::collections::{HashMap, BTreeSet};
use std::iter::FromIterator;

use regex::Regex;

use fnv::FnvHasher;

use crate::model::{ExpressionTree, Value, CompareOperator, ArithmeticOperator, UnaryArithmeticOperator, Function, Aggregate, ValueType, value_type_to_string};
use crate::execution::{ColumnProvider};


#[derive(Debug, PartialEq)]
pub enum EvaluationError {
    ColumnNotFound(String),
    GroupKeyNotFound,
    GroupValueNotFound,
    UndefinedOperation,
    UndefinedFunction,
    InvalidRegex(String),
    ExpectedArray(Option<ValueType>),
    ExpectedArrayIndexingToBeInt(Option<ValueType>)
}

impl std::fmt::Display for EvaluationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvaluationError::ColumnNotFound(name) => { write!(f, "Column '{}' not found", name)  }
            EvaluationError::GroupKeyNotFound => { write!(f, "Group key not found") }
            EvaluationError::GroupValueNotFound => { write!(f, "Group value not found") }
            EvaluationError::UndefinedOperation => { write!(f, "Undefined operation") }
            EvaluationError::UndefinedFunction => { write!(f, "Undefined function") },
            EvaluationError::InvalidRegex(error) => { write!(f, "Invalid regex: {}", error) },
            EvaluationError::ExpectedArray(other_type) => { write!(f, "Expected value to be of array type but got {}", value_type_to_string(other_type)) },
            EvaluationError::ExpectedArrayIndexingToBeInt(other_type) => { write!(f, "Expected array indexing to be of type 'int' but got {}", value_type_to_string(other_type)) },
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
                let left_value = self.evaluate(left)?;
                let right_value = self.evaluate(right)?;

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
            ExpressionTree::Is { left, right } => {
                let left_value = self.evaluate(left)?;
                let right_value = self.evaluate(right)?;

                Ok(Value::Bool(left_value == right_value))
            }
            ExpressionTree::IsNot { left, right } => {
                let left_value = self.evaluate(left)?;
                let right_value = self.evaluate(right)?;

                Ok(Value::Bool(left_value != right_value))
            }
            ExpressionTree::Arithmetic { left, right, operator } => {
                let left_value = self.evaluate(left)?;
                let right_value = self.evaluate(right)?;

                left_value.map_same_type(
                    &right_value,
                    || Some(Value::Null),
                    |x, y| {
                        Some(
                            match operator {
                                ArithmeticOperator::Add => x + y,
                                ArithmeticOperator::Subtract => x - y,
                                ArithmeticOperator::Multiply => x * y,
                                ArithmeticOperator::Divide => x / y
                            }
                        )
                    },
                    |x, y| {
                        Some(
                            match operator {
                                ArithmeticOperator::Add => x + y,
                                ArithmeticOperator::Subtract => x - y,
                                ArithmeticOperator::Multiply => x * y,
                                ArithmeticOperator::Divide => x / y
                            }
                        )
                    },
                    |_, _| None,
                    |_, _| None,
                    |_, _| None
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
                ).ok_or(EvaluationError::UndefinedOperation)
            }
            ExpressionTree::And { left, right } => {
                Ok(Value::Bool(self.evaluate(left)?.bool() && self.evaluate(right)?.bool()))
            }
            ExpressionTree::Or { left, right } => {
                Ok(Value::Bool(self.evaluate(left)?.bool() || self.evaluate(right)?.bool()))
            }
            ExpressionTree::Function { function, arguments } => {
                let mut executed_arguments = Vec::new();
                for argument in arguments {
                    executed_arguments.push(self.evaluate(argument)?);
                }

                match function {
                    Function::Greatest if arguments.len() == 2 => {
                        let arg0 = executed_arguments.remove(0);
                        let arg1 = executed_arguments.remove(0);

                        arg0.map_same_type(
                            &arg1,
                            || Some(Value::Null),
                            |x, y| Some(x.max(y)),
                            |x, y| Some(x.max(y)),
                            |_, _| None,
                            |_, _| None,
                            |_, _| None
                        ).ok_or(EvaluationError::UndefinedOperation)
                    }
                    Function::Least if arguments.len() == 2 => {
                        let arg0 = executed_arguments.remove(0);
                        let arg1 = executed_arguments.remove(0);

                        arg0.map_same_type(
                            &arg1,
                            || Some(Value::Null),
                            |x, y| Some(x.min(y)),
                            |x, y| Some(x.min(y)),
                            |_, _| None,
                            |_, _| None,
                            |_, _| None
                        ).ok_or(EvaluationError::UndefinedOperation)
                    }
                    Function::Abs if arguments.len() == 1 => {
                        let arg = executed_arguments.remove(0);

                        arg.map(
                            || Some(Value::Null),
                            |x| Some(x.abs()),
                            |x| Some(x.abs()),
                            |_| None,
                            |_| None,
                            |_| None
                        ).ok_or(EvaluationError::UndefinedOperation)
                    }
                    Function::Sqrt if arguments.len() == 1 => {
                        let arg = executed_arguments.remove(0);

                        arg.map(
                            || Some(Value::Null),
                            |_| None,
                            |x| Some(x.sqrt()),
                            |_| None,
                            |_| None,
                            |_| None
                        ).ok_or(EvaluationError::UndefinedOperation)
                    }
                    Function::Pow if arguments.len() == 2 => {
                        let arg0 = executed_arguments.remove(0);
                        let arg1 = executed_arguments.remove(0);

                        arg0.map_same_type(
                            &arg1,
                            || Some(Value::Null),
                            |x, y| {
                                if y >= 0 {
                                    Some(x.pow(y as u32))
                                } else {
                                    None
                                }
                            },
                            |x, y| Some(x.powf(y)),
                            |_, _| None,
                            |_, _| None,
                            |_, _| None
                        ).ok_or(EvaluationError::UndefinedOperation)
                    }
                    Function::StringLength if arguments.len() == 1 => {
                        let arg = executed_arguments.remove(0);

                        match arg {
                            Value::String(str) => Ok(Value::Int(str.chars().count() as i64)),
                            _ => Err(EvaluationError::UndefinedOperation)
                        }
                    }
                    Function::StringToUpper if arguments.len() == 1 => {
                        let arg = executed_arguments.remove(0);

                        match arg {
                            Value::String(str) => Ok(Value::String(str.to_uppercase())),
                            _ => Err(EvaluationError::UndefinedOperation)
                        }
                    }
                    Function::StringToLower if arguments.len() == 1 => {
                        let arg = executed_arguments.remove(0);

                        match arg {
                            Value::String(str) => Ok(Value::String(str.to_lowercase())),
                            _ => Err(EvaluationError::UndefinedOperation)
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
                            _ => Err(EvaluationError::UndefinedOperation)
                        }
                    }
                    Function::ArrayUnique if arguments.len() == 1 => {
                        let arg = executed_arguments.remove(0);

                        match arg {
                            Value::Array(element, mut values) => {
                                unique_values(&mut values);
                                Ok(Value::Array(element, values))
                            },
                            _ => Err(EvaluationError::UndefinedOperation)
                        }
                    }
                    _ => Err(EvaluationError::UndefinedFunction)
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
        expression_execution_engine.evaluate(&ExpressionTree::Is {
            left: Box::new(ExpressionTree::Value(Value::Null)),
            right: Box::new(ExpressionTree::Value(Value::Null)),
        })
    );

    assert_eq!(
        Ok(Value::Bool(true)),
        expression_execution_engine.evaluate(&ExpressionTree::IsNot {
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
        expression_execution_engine.evaluate(&ExpressionTree::And {
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
        expression_execution_engine.evaluate(&ExpressionTree::And {
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
        expression_execution_engine.evaluate(&ExpressionTree::Or {
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
        expression_execution_engine.evaluate(&ExpressionTree::Or {
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
fn test_arithmetic() {
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
