use std::collections::HashMap;

use crate::model::{ExpressionTree, Value, CompareOperator, ArithmeticOperator};
use crate::execution_model::ColumnProvider;

#[derive(Debug, PartialEq)]
pub enum EvaluationError {
    ColumnNotFound,
    UndefinedOperation
}

impl std::fmt::Display for EvaluationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
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
                self.column_access.get(name.as_str()).map(|value| value.clone()).ok_or(EvaluationError::ColumnNotFound)
            }
            ExpressionTree::Compare { left, right, operator } => {
                let left_value = self.evaluate(left)?;
                let right_value = self.evaluate(right)?;

                match operator {
                    CompareOperator::Equal => Ok(Value::Bool(left_value == right_value)),
                    CompareOperator::NotEqual => Ok(Value::Bool(left_value != right_value)),
                    CompareOperator::GreaterThan => Ok(Value::Bool(left_value > right_value)),
                    CompareOperator::GreaterThanOrEqual => Ok(Value::Bool(left_value >= right_value)),
                    CompareOperator::LessThan => Ok(Value::Bool(left_value < right_value)),
                    CompareOperator::LessThanOrEqual => Ok(Value::Bool(left_value <= right_value))
                }
            }
            ExpressionTree::Arithmetic { left, right, operator } => {
                let left_value = self.evaluate(left)?;
                let right_value = self.evaluate(right)?;

                left_value.map_same_type(
                    &right_value,
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
                    |_, _| {
                        None
                    },
                    |_, _| {
                        None
                    }
                ).ok_or(EvaluationError::UndefinedOperation)
            }
            ExpressionTree::And { left, right } => {
                Ok(Value::Bool(self.evaluate(left)?.bool() && self.evaluate(right)?.bool()))
            }
            ExpressionTree::Or { left, right } => {
                Ok(Value::Bool(self.evaluate(left)?.bool() || self.evaluate(right)?.bool()))
            }
        }
    }
}

struct TestColumnProvider {
    columns: HashMap<String, Value>
}

impl TestColumnProvider {
    fn new() -> TestColumnProvider {
        TestColumnProvider {
            columns: HashMap::new()
        }
    }

    fn add_column(&mut self, name: String, value: Value) {
        self.columns.insert(name, value);
    }
}

impl ColumnProvider for TestColumnProvider {
    fn get(&self, name: &str) -> Option<&Value> {
        self.columns.get(name)
    }
}

#[test]
fn test_evaluate_column() {
    let mut column_provider = TestColumnProvider::new();
    column_provider.add_column("x".to_owned(), Value::Int(1337));

    let expression_execution_engine = ExpressionExecutionEngine::new(&column_provider);

    assert_eq!(
        Ok(Value::Int(1337)),
        expression_execution_engine.evaluate(&ExpressionTree::ColumnAccess("x".to_owned()))
    );

    assert_eq!(
        Err(EvaluationError::ColumnNotFound),
        expression_execution_engine.evaluate(&ExpressionTree::ColumnAccess("y".to_owned()))
    );
}

#[test]
fn test_evaluate_compare() {
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