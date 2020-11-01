use std::fmt::Formatter;
use std::collections::HashSet;
use std::iter::FromIterator;

use lazy_static::lazy_static;

use crate::parser::{ParseOperationTree, ParseExpressionTree, Operator};
use crate::model::{Statement, ExpressionTree, ArithmeticOperator, CompareOperator, SelectStatement, Value, Aggregate, AggregateStatement, ValueType, UnaryArithmeticOperator, Function};
use crate::data_model::{ColumnDefinition, TableDefinition};

#[derive(Debug)]
pub enum ConvertParseTreeError {
    UndefinedOperator,
    UnexpectedArguments,
    ExpectedArgument,
    TooManyArguments,
    ExpectedColumnAccess,
    UndefinedAggregate,
    UndefinedStatement,
    UndefinedExpression,
    InvalidPattern
}

impl std::fmt::Display for ConvertParseTreeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

pub fn transform_statement(tree: ParseOperationTree) -> Result<Statement, ConvertParseTreeError> {
    match tree {
        ParseOperationTree::Select { projections, from, filter, group_by } => {
            if let Some(group_by) = group_by {
                create_aggregate_statement(projections, from, filter, Some(group_by))
            } else {
                if any_aggregates(&projections) {
                    create_aggregate_statement(projections, from, filter, None)
                } else {
                    create_select_statement(projections, from, filter)
                }
            }
        }
        ParseOperationTree::CreateTable { name, patterns, columns } => create_create_table_statement(name, patterns, columns),
        ParseOperationTree::Multiple(statements) => {
            let mut transformed_statements = Vec::new();
            for statement in statements {
                transformed_statements.push(transform_statement(statement)?);
            }

            Ok(Statement::Multiple(transformed_statements))
        }
    }
}

fn create_select_statement(projections: Vec<(Option<String>, ParseExpressionTree)>,
                           from: (String, Option<String>),
                           filter: Option<ParseExpressionTree>) -> Result<Statement, ConvertParseTreeError> {
    let mut transformed_projections = Vec::new();
    for (projection_index, (name, tree)) in projections.into_iter().enumerate() {
        let expression = transform_expression(tree)?;

        let mut default_name = format!("p{}", projection_index);
        if let ExpressionTree::ColumnAccess(column) = &expression {
            default_name = column.clone();
        }

        let name = name.unwrap_or(default_name);
        transformed_projections.push((name, expression));
    }

    let transformed_filter = if let Some(filter) = filter {
        Some(transform_expression(filter)?)
    } else {
        None
    };

    let select_statement = SelectStatement {
        projections: transformed_projections,
        from: from.0,
        filename: from.1,
        filter: transformed_filter
    };

    Ok(Statement::Select(select_statement))
}

fn create_aggregate_statement(projections: Vec<(Option<String>, ParseExpressionTree)>,
                              from: (String, Option<String>),
                              filter: Option<ParseExpressionTree>,
                              group_by: Option<String>) -> Result<Statement, ConvertParseTreeError> {
    let mut transformed_aggregates = Vec::new();
    for (projection_index, (name, tree)) in projections.into_iter().enumerate() {
        let (default_name, aggregate) = transform_aggregate(tree)?;
        let name = name.or(default_name).unwrap_or(format!("p{}", projection_index));

        transformed_aggregates.push((name, aggregate));
    }

    let transformed_filter = if let Some(filter) = filter {
        Some(transform_expression(filter)?)
    } else {
        None
    };

    let aggregate_statement = AggregateStatement {
        aggregates: transformed_aggregates,
        from: from.0,
        filename: from.1,
        filter: transformed_filter,
        group_by
    };

    Ok(Statement::Aggregate(aggregate_statement))
}

fn create_create_table_statement(name: String,
                                 patterns: Vec<(String, String)>,
                                 columns: Vec<(String, ValueType, String, usize)>) -> Result<Statement, ConvertParseTreeError> {
    let column_definitions = columns
        .into_iter()
        .map(|column| ColumnDefinition::new(&column.2, column.3, &column.0, column.1))
        .collect::<Vec<_>>();

    let table_definition = TableDefinition::new(
        &name,
        patterns.iter().map(|(x, y)| (x.as_str(), y.as_str())).collect(),
        column_definitions,
    ).ok_or(ConvertParseTreeError::InvalidPattern)?;

    Ok(Statement::CreateTable(table_definition))
}

lazy_static! {
    static ref AGGREGATE_FUNCTIONS: HashSet<String> = HashSet::from_iter(
        vec!["min".to_owned(), "max".to_owned(), "avg".to_owned(), "sum".to_owned()].into_iter()
    );
}

fn any_aggregates(projections: &Vec<(Option<String>, ParseExpressionTree)>) -> bool {
    projections.iter().any(|(_, projection)| {
        match projection {
            ParseExpressionTree::Call(name, arguments) if name.to_lowercase() == "count" && arguments.is_empty() => true,
            ParseExpressionTree::Call(name, arguments) if AGGREGATE_FUNCTIONS.contains(&name.to_lowercase()) && arguments.len() == 1 => true,
            _ => false
        }
    })
}

pub fn transform_expression(tree: ParseExpressionTree) -> Result<ExpressionTree, ConvertParseTreeError> {
    match tree {
        ParseExpressionTree::Value(value) => Ok(ExpressionTree::Value(value)),
        ParseExpressionTree::ColumnAccess(name) => Ok(ExpressionTree::ColumnAccess(name)),
        ParseExpressionTree::Wildcard => Ok(ExpressionTree::Wildcard),
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
        ParseExpressionTree::UnaryOperator { operator, operand } => {
            let operand = Box::new(transform_expression(*operand)?);

            match operator {
                Operator::Single('-') => Ok(ExpressionTree::UnaryArithmetic { operator: UnaryArithmeticOperator::Negative, operand }),
                _ => { return Err(ConvertParseTreeError::UndefinedOperator); }
            }
        }
        ParseExpressionTree::Invert { operand } => {
            let operand = Box::new(transform_expression(*operand)?);
            Ok(ExpressionTree::UnaryArithmetic { operator: UnaryArithmeticOperator::Invert, operand })
        }
        ParseExpressionTree::AndExpression { left, right } => {
            let left = Box::new(transform_expression(*left)?);
            let right = Box::new(transform_expression(*right)?);

            Ok(ExpressionTree::And { left, right })
        }
        ParseExpressionTree::OrExpression { left, right } => {
            let left = Box::new(transform_expression(*left)?);
            let right = Box::new(transform_expression(*right)?);

            Ok(ExpressionTree::Or { left, right })
        }
        ParseExpressionTree::Call(name, arguments) => {
            let mut transformed_arguments = Vec::new();
            for argument in arguments {
                transformed_arguments.push(transform_expression(argument)?);
            }

            let name_lower = name.to_lowercase();
            match name_lower.as_str() {
                "min" => Ok(ExpressionTree::Function { function: Function::Min, arguments: transformed_arguments }),
                "max" => Ok(ExpressionTree::Function { function: Function::Max, arguments: transformed_arguments }),
                _ => Err(ConvertParseTreeError::UndefinedExpression)
            }
        }
    }
}

fn transform_aggregate(tree: ParseExpressionTree) -> Result<(Option<String>, Aggregate), ConvertParseTreeError> {
    match tree {
        ParseExpressionTree::ColumnAccess(name) => Ok((Some(name.clone()), Aggregate::GroupKey(name))),
        ParseExpressionTree::Call(name, mut arguments) => {
            let name_lowercase = name.to_lowercase();
            if name_lowercase == "count" {
                if arguments.is_empty() {
                    Ok((None, Aggregate::Count))
                } else {
                    Err(ConvertParseTreeError::UnexpectedArguments)
                }
            } else if AGGREGATE_FUNCTIONS.contains(&name_lowercase) {
                if arguments.len() == 1 {
                    let expression = transform_expression(arguments.remove(0))?;
                    let aggregate = match name_lowercase.as_str() {
                        "max" => Aggregate::Max(expression),
                        "min" => Aggregate::Min(expression),
                        "avg" => Aggregate::Average(expression),
                        "sum" => Aggregate::Sum(expression),
                        _ => { panic!("should not happen") }
                    };

                    Ok((None, aggregate))
                } else if arguments.is_empty() {
                    Err(ConvertParseTreeError::ExpectedArgument)
                } else {
                    Err(ConvertParseTreeError::TooManyArguments)
                }
            } else {
                Err(ConvertParseTreeError::UndefinedAggregate)
            }
        }
        _ => { return Err(ConvertParseTreeError::UndefinedAggregate); }
    }
}

#[test]
fn test_select_statement1() {
    let tree = ParseOperationTree::Select {
        projections: vec![(None, ParseExpressionTree::ColumnAccess("x".to_owned()))],
        from: ("test".to_string(), None),
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
    assert_eq!("x", statement.projections[0].0.as_str());
    assert_eq!(&ExpressionTree::ColumnAccess("x".to_owned()), &statement.projections[0].1);

    assert_eq!("test", statement.from);
}

#[test]
fn test_select_statement2() {
    let tree = ParseOperationTree::Select {
        projections: vec![(Some("x".to_owned()), ParseExpressionTree::ColumnAccess("x".to_owned()))],
        from: ("test".to_string(), None),
        filter: None,
        group_by: None
    };

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let parsed_statement = statement.unwrap();

    let statement = parsed_statement.extract_select();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.projections.len());
    assert_eq!("x", statement.projections[0].0.as_str());
    assert_eq!(&ExpressionTree::ColumnAccess("x".to_owned()), &statement.projections[0].1);

    assert_eq!("test", statement.from);
}

#[test]
fn test_select_statement3() {
    let tree = ParseOperationTree::Select {
        projections: vec![(None, ParseExpressionTree::ColumnAccess("x".to_owned()))],
        from: ("test".to_string(), None),
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
    let parsed_statement = statement.unwrap();

    let statement = parsed_statement.extract_select();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.projections.len());
    assert_eq!("x", statement.projections[0].0.as_str());
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
fn test_select_statement4() {
    let tree = ParseOperationTree::Select {
        projections: vec![
            (
                None,
                ParseExpressionTree::BinaryOperator {
                    operator: Operator::Single('*'),
                    left: Box::new(ParseExpressionTree::ColumnAccess("x".to_owned())),
                    right: Box::new(ParseExpressionTree::Value(Value::Int(2))),
                }
            )
        ],
        from: ("test".to_string(), None),
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
fn test_select_statement5() {
    let tree = ParseOperationTree::Select {
        projections: vec![(None, ParseExpressionTree::ColumnAccess("x".to_owned()))],
        from: ("test".to_string(), Some("test.log".to_owned())),
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
    assert_eq!("x", statement.projections[0].0.as_str());
    assert_eq!(&ExpressionTree::ColumnAccess("x".to_owned()), &statement.projections[0].1);

    assert_eq!("test", statement.from);
    assert_eq!(Some("test.log".to_owned()), statement.filename);
}

#[test]
fn test_select_statement6() {
    let tree = ParseOperationTree::Select {
        projections: vec![
            (
                None,
                ParseExpressionTree::UnaryOperator {
                    operator: Operator::Single('-'),
                    operand: Box::new(ParseExpressionTree::ColumnAccess("x".to_owned()))
                }
            )
        ],
        from: ("test".to_string(), None),
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
    assert_eq!(
        &ExpressionTree::UnaryArithmetic { operator: UnaryArithmeticOperator::Negative, operand: Box::new(ExpressionTree::ColumnAccess("x".to_owned())) },
        &statement.projections[0].1
    );

    assert_eq!("test", statement.from);
}

#[test]
fn test_select_statement7() {
    let tree = ParseOperationTree::Select {
        projections: vec![
            (
                None,
                ParseExpressionTree::Call(
                    "MAX".to_owned(),
                    vec![
                        ParseExpressionTree::ColumnAccess("x".to_owned()),
                        ParseExpressionTree::ColumnAccess("y".to_owned())
                    ]
                )
            )
        ],
        from: ("test".to_string(), None),
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
    assert_eq!(
        &ExpressionTree::Function {
            function: Function::Max,
            arguments: vec![ExpressionTree::ColumnAccess("x".to_owned()), ExpressionTree::ColumnAccess("y".to_owned())]
        },
        &statement.projections[0].1
    );

    assert_eq!("test", statement.from);
}

#[test]
fn test_aggregate_statement1() {
    let tree = ParseOperationTree::Select {
        projections: vec![
            (None, ParseExpressionTree::ColumnAccess("x".to_owned())),
            (None, ParseExpressionTree::Call("MAX".to_owned(), vec![ParseExpressionTree::ColumnAccess("x".to_owned())])),
        ],
        from: ("test".to_string(), None),
        filter: None,
        group_by: Some("x".to_owned())
    };

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statemnt = statement.unwrap();

    let statement = statemnt.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(2, statement.aggregates.len());
    assert_eq!("x", statement.aggregates[0].0);
    assert_eq!(Aggregate::GroupKey("x".to_owned()), statement.aggregates[0].1);

    assert_eq!("p1", statement.aggregates[1].0);
    assert_eq!(Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[1].1);


    assert_eq!("test", statement.from);
    assert_eq!(Some("x".to_owned()), statement.group_by);
}

#[test]
fn test_aggregate_statement2() {
    let tree = ParseOperationTree::Select {
        projections: vec![
            (None, ParseExpressionTree::ColumnAccess("x".to_owned())),
            (None, ParseExpressionTree::Call("SUM".to_owned(), vec![ParseExpressionTree::ColumnAccess("x".to_owned())])),
        ],
        from: ("test".to_string(), None),
        filter: None,
        group_by: Some("x".to_owned())
    };

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statemnt = statement.unwrap();

    let statement = statemnt.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(2, statement.aggregates.len());
    assert_eq!("x", statement.aggregates[0].0);
    assert_eq!(Aggregate::GroupKey("x".to_owned()), statement.aggregates[0].1);

    assert_eq!("p1", statement.aggregates[1].0);
    assert_eq!(Aggregate::Sum(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[1].1);


    assert_eq!("test", statement.from);
    assert_eq!(Some("x".to_owned()), statement.group_by);
}

#[test]
fn test_aggregate_statement3() {
    let tree = ParseOperationTree::Select {
        projections: vec![
            (None, ParseExpressionTree::ColumnAccess("x".to_owned())),
            (None, ParseExpressionTree::Call("COUNT".to_owned(), vec![])),
        ],
        from: ("test".to_string(), None),
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
    assert_eq!(Aggregate::GroupKey("x".to_owned()), statement.aggregates[0].1);

    assert_eq!("p1", statement.aggregates[1].0);
    assert_eq!(Aggregate::Count, statement.aggregates[1].1);

    assert_eq!("test", statement.from);
    assert_eq!(Some("x".to_owned()), statement.group_by);
}

#[test]
fn test_aggregate_statement4() {
    let tree = ParseOperationTree::Select {
        projections: vec![
            (None, ParseExpressionTree::Call("COUNT".to_owned(), vec![])),
        ],
        from: ("test".to_string(), None),
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

#[test]
fn test_aggregate_statement5() {
    let tree = ParseOperationTree::Select {
        projections: vec![
            (None, ParseExpressionTree::Call("MAX".to_owned(), vec![ParseExpressionTree::ColumnAccess("x".to_owned())])),
        ],
        from: ("test".to_string(), None),
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
    assert_eq!(Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[0].1);
}

#[test]
fn test_create_table_statement1() {
    let tree = ParseOperationTree::CreateTable {
        name: "test".to_string(),
        patterns: vec![("line".to_owned(), "A: ([0-9]+)".to_owned())],
        columns: vec![("x".to_owned(), ValueType::Int, "line".to_owned(), 1)]
    };

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_create_table();
    assert!(statement.is_some());
    let table_definition = statement.unwrap();

    assert_eq!("test", table_definition.name);

    assert_eq!(1, table_definition.patterns.len());
    assert_eq!("line", table_definition.patterns[0].0);
    assert_eq!("A: ([0-9]+)", table_definition.patterns[0].1.as_str());

    assert_eq!(1, table_definition.columns.len());
    assert_eq!("x", table_definition.columns[0].name);
    assert_eq!(ValueType::Int, table_definition.columns[0].column_type);
    assert_eq!("line", table_definition.columns[0].pattern_name);
    assert_eq!(1, table_definition.columns[0].group_index);
}