use crate::parser::{ParseOperationTree, ParseExpressionTree, Operator};
use crate::model::{Statement, ExpressionTree, ArithmeticOperator, CompareOperator, SelectStatement, Value, Aggregate, AggregateStatement};

#[derive(Debug)]
pub enum ConvertParseTreeError {
    UndefinedOperator,
    UnexpectedArguments,
    ExpectedArgument,
    TooManyArguments,
    ExpectedColumnAccess,
    UndefinedAggregate,
    UndefinedStatement,
    UndefinedExpression
}

pub fn transform_statement(tree: ParseOperationTree) -> Result<Statement, ConvertParseTreeError> {
    match tree {
        ParseOperationTree::Select { projections, from, filter, group_by } => {
            if let Some(group_by) = group_by {
                return create_aggregate_statement(projections, from, filter, Some(group_by));
            } else {
                let any_aggregates = projections.iter().any(|(_, projection)| {
                    match projection {
                        ParseExpressionTree::Call(_, _) => true,
                        _ => false
                    }
                });

                if !any_aggregates {
                    let mut transformed_projections = Vec::new();
                    for (name, tree) in projections {
                        transformed_projections.push((name, transform_expression(tree)?));
                    }

                    let transformed_filter = if let Some(filter) = filter {
                        Some(transform_expression(filter)?)
                    } else {
                        None
                    };

                    let select_statement = SelectStatement {
                        projections: transformed_projections,
                        from,
                        filter: transformed_filter
                    };

                    return Ok(Statement::Select(select_statement));
                } else {
                    return create_aggregate_statement(projections, from, filter, None);
                }
            }
        }
    }

    return Err(ConvertParseTreeError::UndefinedStatement);
}

fn create_aggregate_statement(projections: Vec<(String, ParseExpressionTree)>,
                              from: String,
                              filter: Option<ParseExpressionTree>,
                              group_by: Option<String>) -> Result<Statement, ConvertParseTreeError> {
    let mut transformed_aggregates = Vec::new();
    for (name, tree) in projections {
        let (override_name, aggregate) = transform_aggregate(tree)?;
        let name = override_name.unwrap_or(name);

        transformed_aggregates.push((name, aggregate));
    }

    let transformed_filter = if let Some(filter) = filter {
        Some(transform_expression(filter)?)
    } else {
        None
    };

    let aggregate_statement = AggregateStatement {
        aggregates: transformed_aggregates,
        from,
        filter: transformed_filter,
        group_by
    };

    Ok(Statement::Aggregate(aggregate_statement))
}

pub fn transform_expression(tree: ParseExpressionTree) -> Result<ExpressionTree, ConvertParseTreeError> {
    match tree {
        ParseExpressionTree::Value(value) => Ok(ExpressionTree::Value(value)),
        ParseExpressionTree::ColumnAccess(name) => Ok(ExpressionTree::ColumnAccess(name)),
        ParseExpressionTree::BinaryOperator { operator, left, right } => {
            let left = Box::new(transform_expression(*left)?);
            let right = Box::new(transform_expression(*right)?);

            match operator {
                Operator::Single('+') => Ok(ExpressionTree::Arithmetic { operator: ArithmeticOperator::Add, left, right }),
                Operator::Single('-') => Ok(ExpressionTree::Arithmetic { operator: ArithmeticOperator::Subtract, left, right }),
                Operator::Single('*') => Ok(ExpressionTree::Arithmetic { operator: ArithmeticOperator::Multiply, left, right }),
                Operator::Single('/') => Ok(ExpressionTree::Arithmetic { operator: ArithmeticOperator::Divide, left, right }),
                Operator::Single('<') => Ok(ExpressionTree::Compare { operator: CompareOperator::LessThan, left, right }),
                Operator::Single('>') => Ok(ExpressionTree::Compare { operator: CompareOperator::GreaterThan, left, right }),
                Operator::Single('=') => Ok(ExpressionTree::Compare { operator: CompareOperator::Equal, left, right }),
                Operator::Dual('!', '=') => Ok(ExpressionTree::Compare { operator: CompareOperator::NotEqual, left, right }),
                Operator::Dual('>', '=') => Ok(ExpressionTree::Compare { operator: CompareOperator::GreaterThanOrEqual, left, right }),
                Operator::Dual('<', '=') => Ok(ExpressionTree::Compare { operator: CompareOperator::LessThanOrEqual, left, right }),
                _ => { return Err(ConvertParseTreeError::UndefinedOperator); }
            }
        }
        ParseExpressionTree::UnaryOperator { .. } =>  Err(ConvertParseTreeError::UndefinedExpression),
        ParseExpressionTree::AndExpression { .. } =>  Err(ConvertParseTreeError::UndefinedExpression),
        ParseExpressionTree::OrExpression { .. } =>  Err(ConvertParseTreeError::UndefinedExpression),
        ParseExpressionTree::Call(_, _) => Err(ConvertParseTreeError::UndefinedExpression)
    }
}

pub fn transform_aggregate(tree: ParseExpressionTree) -> Result<(Option<String>, Aggregate), ConvertParseTreeError> {
    match tree {
        ParseExpressionTree::ColumnAccess(name) => Ok((Some(name), Aggregate::GroupKey)),
        ParseExpressionTree::Call(name, mut arguments) => {
            match name.to_lowercase().as_str() {
                "count" => {
                    if arguments.is_empty() {
                        Ok((None, Aggregate::Count))
                    } else {
                        Err(ConvertParseTreeError::UnexpectedArguments)
                    }
                },
                "max" => {
                    if arguments.len() == 1 {
                        Ok((None, Aggregate::Max(transform_expression(arguments.remove(0))?)))
                    } else if arguments.is_empty() {
                        Err(ConvertParseTreeError::ExpectedArgument)
                    } else {
                        Err(ConvertParseTreeError::TooManyArguments)
                    }
                },
                "min" => {
                    if arguments.len() == 1 {
                        Ok((None, Aggregate::Min(transform_expression(arguments.remove(0))?)))
                    } else if arguments.is_empty() {
                        Err(ConvertParseTreeError::ExpectedArgument)
                    } else {
                        Err(ConvertParseTreeError::TooManyArguments)
                    }
                }
                _ => Err(ConvertParseTreeError::UndefinedAggregate)
            }
        }
        _ => { return Err(ConvertParseTreeError::UndefinedAggregate); }
    }
}

fn transform_column_access(tree: ParseExpressionTree) -> Result<String, ConvertParseTreeError> {
    match tree {
        ParseExpressionTree::ColumnAccess(name) => Ok(name),
        _ => Err(ConvertParseTreeError::ExpectedColumnAccess)
    }
}

#[test]
fn test_select_statement1() {
    let tree = ParseOperationTree::Select {
        projections: vec![("p0".to_owned(), ParseExpressionTree::ColumnAccess("x".to_owned()))],
        from: "test".to_string(),
        filter: None,
        group_by: None
    };

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_select();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.projections.len());
    assert_eq!("p0", statement.projections[0].0.as_str());
    assert_eq!(&ExpressionTree::ColumnAccess("x".to_owned()), &statement.projections[0].1);

    assert_eq!("test", statement.from);
}

#[test]
fn test_select_statement2() {
    let tree = ParseOperationTree::Select {
        projections: vec![("p0".to_owned(), ParseExpressionTree::ColumnAccess("x".to_owned()))],
        from: "test".to_string(),
        filter: Some(
            ParseExpressionTree::BinaryOperator {
                operator: Operator::Single('>'),
                left: Box::new(ParseExpressionTree::ColumnAccess("x".to_owned())),
                right: Box::new(ParseExpressionTree::Value(Value::Int(10)))
            }
        ),
        group_by: None
    };

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_select();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.projections.len());
    assert_eq!("p0", statement.projections[0].0.as_str());
    assert_eq!(&ExpressionTree::ColumnAccess("x".to_owned()), &statement.projections[0].1);

    assert_eq!("test", statement.from);

    assert_eq!(
        Some(ExpressionTree::Compare {
            operator: CompareOperator::GreaterThan,
            left: Box::new(ExpressionTree::ColumnAccess("x".to_owned())),
            right: Box::new(ExpressionTree::Value(Value::Int(10))),
        }),
        statement.filter
    );
}

#[test]
fn test_select_statement3() {
    let tree = ParseOperationTree::Select {
        projections: vec![
            (
                "p0".to_owned(),
                ParseExpressionTree::BinaryOperator {
                    operator: Operator::Single('*'),
                    left: Box::new(ParseExpressionTree::ColumnAccess("x".to_owned())),
                    right: Box::new(ParseExpressionTree::Value(Value::Int(2))),
                }
            )
        ],
        from: "test".to_string(),
        filter: Some(
            ParseExpressionTree::BinaryOperator {
                operator: Operator::Single('>'),
                left: Box::new(ParseExpressionTree::ColumnAccess("x".to_owned())),
                right: Box::new(ParseExpressionTree::Value(Value::Int(10)))
            }
        ),
        group_by: None
    };

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_select();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.projections.len());
    assert_eq!("p0", statement.projections[0].0.as_str());
    assert_eq!(
        &ExpressionTree::Arithmetic {
            left: Box::new(ExpressionTree::ColumnAccess("x".to_owned())),
            right: Box::new(ExpressionTree::Value(Value::Int(2))),
            operator: ArithmeticOperator::Multiply
        },
        &statement.projections[0].1
    );

    assert_eq!("test", statement.from);

    assert_eq!(
        Some(ExpressionTree::Compare {
            operator: CompareOperator::GreaterThan,
            left: Box::new(ExpressionTree::ColumnAccess("x".to_owned())),
            right: Box::new(ExpressionTree::Value(Value::Int(10))),
        }),
        statement.filter
    );
}

#[test]
fn test_aggregate_statement1() {
    let tree = ParseOperationTree::Select {
        projections: vec![
            ("p0".to_owned(), ParseExpressionTree::ColumnAccess("x".to_owned())),
            ("p1".to_owned(), ParseExpressionTree::Call("MAX".to_owned(), vec![ParseExpressionTree::ColumnAccess("x".to_owned())])),
        ],
        from: "test".to_string(),
        filter: None,
        group_by: Some("x".to_owned())
    };

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(2, statement.aggregates.len());
    assert_eq!("x", statement.aggregates[0].0);
    assert_eq!(Aggregate::GroupKey, statement.aggregates[0].1);

    assert_eq!("p1", statement.aggregates[1].0);
    assert_eq!(Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[1].1);


    assert_eq!("test", statement.from);
    assert_eq!(Some("x".to_owned()), statement.group_by);
}

#[test]
fn test_aggregate_statement2() {
    let tree = ParseOperationTree::Select {
        projections: vec![
            ("p0".to_owned(), ParseExpressionTree::ColumnAccess("x".to_owned())),
            ("p1".to_owned(), ParseExpressionTree::Call("COUNT".to_owned(), vec![])),
        ],
        from: "test".to_string(),
        filter: None,
        group_by: Some("x".to_owned())
    };

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(2, statement.aggregates.len());
    assert_eq!("x", statement.aggregates[0].0);
    assert_eq!(Aggregate::GroupKey, statement.aggregates[0].1);

    assert_eq!("p1", statement.aggregates[1].0);
    assert_eq!(Aggregate::Count, statement.aggregates[1].1);

    assert_eq!("test", statement.from);
    assert_eq!(Some("x".to_owned()), statement.group_by);
}

#[test]
fn test_aggregate_statement3() {
    let tree = ParseOperationTree::Select {
        projections: vec![
            ("p0".to_owned(), ParseExpressionTree::Call("COUNT".to_owned(), vec![])),
        ],
        from: "test".to_string(),
        filter: None,
        group_by: None
    };

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.aggregates.len());
    assert_eq!("p0", statement.aggregates[0].0);
    assert_eq!(Aggregate::Count, statement.aggregates[0].1);
}