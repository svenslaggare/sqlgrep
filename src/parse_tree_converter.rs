use std::fmt::Formatter;
use std::collections::HashSet;
use std::collections::HashMap;
use std::iter::FromIterator;

use lazy_static::lazy_static;

use crate::parser::{ParseOperationTree, ParseExpressionTree, Operator, ParseColumnDefinition};
use crate::model::{Statement, ExpressionTree, ArithmeticOperator, CompareOperator, SelectStatement, Value, Aggregate, AggregateStatement, ValueType, UnaryArithmeticOperator, Function};
use crate::data_model::{ColumnDefinition, TableDefinition};

#[derive(Debug)]
pub enum ConvertParseTreeError {
    UndefinedOperator(Operator),
    UnexpectedArguments,
    ExpectedArgument,
    TooManyArguments,
    ExpectedColumnAccess,
    UndefinedAggregate,
    UndefinedStatement,
    UndefinedExpression,
    UndefinedFunction,
    InvalidPattern,
    HavingClauseNotPossible
}

impl std::fmt::Display for ConvertParseTreeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConvertParseTreeError::UndefinedOperator(operator) => { write!(f, "The operator '{}' is not defined", operator) }
            ConvertParseTreeError::UnexpectedArguments => { write!(f, "Expected arguments") }
            ConvertParseTreeError::ExpectedArgument => { write!(f, "Expected an argument") }
            ConvertParseTreeError::TooManyArguments => { write!(f, "Too many arguments") }
            ConvertParseTreeError::ExpectedColumnAccess => { write!(f, "Expected column access") }
            ConvertParseTreeError::UndefinedAggregate => { write!(f, "Undefined aggregate") }
            ConvertParseTreeError::UndefinedStatement => { write!(f, "Undefined statement") }
            ConvertParseTreeError::UndefinedExpression => { write!(f, "Undefined expression") }
            ConvertParseTreeError::UndefinedFunction => { write!(f, "Undefined function") }
            ConvertParseTreeError::InvalidPattern => { write!(f, "Undefined pattern") }
            ConvertParseTreeError::HavingClauseNotPossible => { write!(f, "Having clause only available for aggregate expressions") }
        }
    }
}

pub fn transform_statement(tree: ParseOperationTree) -> Result<Statement, ConvertParseTreeError> {
    match tree {
        ParseOperationTree::Select { projections, from, filter, group_by, having } => {
            if let Some(group_by) = group_by {
                create_aggregate_statement(projections, from, filter, Some(group_by), having)
            } else {
                if any_aggregates(&projections) {
                    create_aggregate_statement(projections, from, filter, None, having)
                } else {
                    if having.is_some() {
                        return Err(ConvertParseTreeError::HavingClauseNotPossible);
                    }

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
        let expression = transform_expression(tree, &mut TransformExpressionState::default())?;

        let mut default_name = format!("p{}", projection_index);
        if let ExpressionTree::ColumnAccess(column) = &expression {
            default_name = column.clone();
        }

        let name = name.unwrap_or(default_name);
        transformed_projections.push((name, expression));
    }

    let transformed_filter = if let Some(filter) = filter {
        Some(transform_expression(filter, &mut TransformExpressionState::default())?)
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
                              group_by: Option<Vec<String>>,
                              having: Option<ParseExpressionTree>) -> Result<Statement, ConvertParseTreeError> {
    let mut transformed_aggregates = Vec::new();
    for (projection_index, (name, tree)) in projections.into_iter().enumerate() {
        let (default_name, aggregate) = transform_aggregate(tree, projection_index)?;
        let name = name.or(default_name).unwrap_or(format!("p{}", projection_index));

        transformed_aggregates.push((name, aggregate));
    }

    let transformed_filter = if let Some(filter) = filter {
        Some(transform_expression(filter, &mut TransformExpressionState::default())?)
    } else {
        None
    };

    let transformed_having = if let Some(having) = having {
        let mut state = TransformExpressionState::default();
        state.allow_aggregates = true;
        Some(transform_expression(having, &mut state)?)
    } else {
        None
    };

    let aggregate_statement = AggregateStatement {
        aggregates: transformed_aggregates,
        from: from.0,
        filename: from.1,
        filter: transformed_filter,
        group_by,
        having: transformed_having
    };

    Ok(Statement::Aggregate(aggregate_statement))
}

fn create_create_table_statement(name: String,
                                 patterns: Vec<(String, String)>,
                                 columns: Vec<ParseColumnDefinition>) -> Result<Statement, ConvertParseTreeError> {
    let column_definitions = columns
        .into_iter()
        .map(|column| {
            let mut column_definition = ColumnDefinition::new(
                &column.pattern_name,
                column.pattern_index,
                &column.name,
                column.column_type
            );

            if let Some(nullable) = column.nullable.as_ref() {
                column_definition.options.nullable = *nullable;
            }

            if let Some(trim) = column.trim.as_ref() {
                column_definition.options.trim = *trim;
            }

            column_definition
        })
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
        vec![
            "count".to_owned(),
            "min".to_owned(),
            "max".to_owned(),
            "avg".to_owned(),
            "sum".to_owned()
         ].into_iter()
    );

    static ref FUNCTIONS: HashMap<String, Function> = HashMap::from_iter(
        vec![
            ("least".to_owned(), Function::Least),
            ("greatest".to_owned(), Function::Greatest),
            ("abs".to_owned(), Function::Abs),
            ("sqrt".to_owned(), Function::Sqrt),
            ("pow".to_owned(), Function::Pow),
            ("length".to_owned(), Function::StringLength),
            ("upper".to_owned(), Function::StringToUpper),
            ("lower".to_owned(), Function::StringToLower)
        ].into_iter()
    );
}

fn any_aggregates(projections: &Vec<(Option<String>, ParseExpressionTree)>) -> bool {
    projections.iter().any(|(_, projection)| {
        match projection {
            ParseExpressionTree::Call(name, _) if AGGREGATE_FUNCTIONS.contains(&name.to_lowercase()) => true,
            _ => false
        }
    })
}

pub struct TransformExpressionState {
    allow_aggregates: bool,
    next_aggregate_index: usize
}

impl Default for TransformExpressionState {
    fn default() -> Self {
        TransformExpressionState {
            allow_aggregates: false,
            next_aggregate_index: 0
        }
    }
}

pub fn transform_expression(tree: ParseExpressionTree, state: &mut TransformExpressionState) -> Result<ExpressionTree, ConvertParseTreeError> {
    match tree {
        ParseExpressionTree::Value(value) => Ok(ExpressionTree::Value(value)),
        ParseExpressionTree::ColumnAccess(name) => {
            if state.allow_aggregates {
                let aggregate_index = state.next_aggregate_index;
                state.next_aggregate_index += 1;
                Ok(ExpressionTree::Aggregate(aggregate_index, Box::new(Aggregate::GroupKey(name))))
            } else {
                Ok(ExpressionTree::ColumnAccess(name))
            }
        }
        ParseExpressionTree::Wildcard => Ok(ExpressionTree::Wildcard),
        ParseExpressionTree::BinaryOperator { operator, left, right } => {
            let left = Box::new(transform_expression(*left, state)?);
            let right = Box::new(transform_expression(*right, state)?);

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
                _ => { return Err(ConvertParseTreeError::UndefinedOperator(operator)); }
            }
        }
        ParseExpressionTree::UnaryOperator { operator, operand } => {
            let operand = Box::new(transform_expression(*operand, state)?);

            match operator {
                Operator::Single('-') => Ok(ExpressionTree::UnaryArithmetic { operator: UnaryArithmeticOperator::Negative, operand }),
                _ => { return Err(ConvertParseTreeError::UndefinedOperator(operator)); }
            }
        }
        ParseExpressionTree::Invert { operand } => {
            let operand = Box::new(transform_expression(*operand, state)?);
            Ok(ExpressionTree::UnaryArithmetic { operator: UnaryArithmeticOperator::Invert, operand })
        }
        ParseExpressionTree::Is { left, right } => {
            let left = Box::new(transform_expression(*left, state)?);
            let right = Box::new(transform_expression(*right, state)?);

            Ok(ExpressionTree::Is { left, right })
        }
        ParseExpressionTree::IsNot { left, right } => {
            let left = Box::new(transform_expression(*left, state)?);
            let right = Box::new(transform_expression(*right, state)?);

            Ok(ExpressionTree::IsNot { left, right })
        }
        ParseExpressionTree::And { left, right } => {
            let left = Box::new(transform_expression(*left, state)?);
            let right = Box::new(transform_expression(*right, state)?);

            Ok(ExpressionTree::And { left, right })
        }
        ParseExpressionTree::Or { left, right } => {
            let left = Box::new(transform_expression(*left, state)?);
            let right = Box::new(transform_expression(*right, state)?);

            Ok(ExpressionTree::Or { left, right })
        }
        ParseExpressionTree::Call(name, arguments) => {
            if state.allow_aggregates {
                match transform_call_aggregate(&name, arguments.clone(), 0) {
                    Ok((_, aggregate)) => {
                        let aggregate_index = state.next_aggregate_index;
                        state.next_aggregate_index += 1;
                        return Ok(ExpressionTree::Aggregate(aggregate_index, Box::new(aggregate)));
                    }
                    Err(err) => {
                        match err {
                            ConvertParseTreeError::UndefinedAggregate => {}
                            err => { return Err(err); }
                        }
                    }
                }
            }

            let mut transformed_arguments = Vec::new();
            for argument in arguments {
                transformed_arguments.push(transform_expression(argument, state)?);
            }

            FUNCTIONS.get(&name.to_lowercase()).map(|function| {
                ExpressionTree::Function { function: function.clone(), arguments: transformed_arguments }
            }).ok_or(ConvertParseTreeError::UndefinedFunction)
        }
    }
}

fn transform_aggregate(tree: ParseExpressionTree, index: usize) -> Result<(Option<String>, Aggregate), ConvertParseTreeError> {
    match tree {
        ParseExpressionTree::ColumnAccess(name) => Ok((Some(name.clone()), Aggregate::GroupKey(name))),
        ParseExpressionTree::Call(name, arguments) => transform_call_aggregate(&name, arguments, index),
        _ => { return Err(ConvertParseTreeError::UndefinedAggregate); }
    }
}

fn transform_call_aggregate(name: &str,
                            mut arguments: Vec<ParseExpressionTree>,
                            index: usize) -> Result<(Option<String>, Aggregate), ConvertParseTreeError> {
    let name_lowercase = name.to_lowercase();
    if name_lowercase == "count" {
        if arguments.is_empty() {
            Ok((Some(format!("count{}", index)), Aggregate::Count))
        } else {
            Err(ConvertParseTreeError::UnexpectedArguments)
        }
    } else if AGGREGATE_FUNCTIONS.contains(&name_lowercase) {
        if arguments.len() == 1 {
            let expression = transform_expression(arguments.remove(0), &mut TransformExpressionState::default())?;
            let aggregate = match name_lowercase.as_str() {
                "max" => Aggregate::Max(expression),
                "min" => Aggregate::Min(expression),
                "avg" => Aggregate::Average(expression),
                "sum" => Aggregate::Sum(expression),
                _ => { panic!("should not happen") }
            };

            Ok((Some(format!("{}{}", name_lowercase, index)), aggregate))
        } else if arguments.is_empty() {
            Err(ConvertParseTreeError::ExpectedArgument)
        } else {
            Err(ConvertParseTreeError::TooManyArguments)
        }
    } else {
        Err(ConvertParseTreeError::UndefinedAggregate)
    }
}

#[test]
fn test_select_statement1() {
    let tree = ParseOperationTree::Select {
        projections: vec![(None, ParseExpressionTree::ColumnAccess("x".to_owned()))],
        from: ("test".to_string(), None),
        filter: None,
        group_by: None,
        having: None
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
        group_by: None,
        having: None
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
        group_by: None,
        having: None
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
        group_by: None,
        having: None
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
        group_by: None,
        having: None
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
        group_by: None,
        having: None
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
                    "GREATEST".to_owned(),
                    vec![
                        ParseExpressionTree::ColumnAccess("x".to_owned()),
                        ParseExpressionTree::ColumnAccess("y".to_owned())
                    ]
                )
            )
        ],
        from: ("test".to_string(), None),
        filter: None,
        group_by: None,
        having: None
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
            function: Function::Greatest,
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
        group_by: Some(vec!["x".to_owned()]),
        having: None
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

    assert_eq!("max1", statement.aggregates[1].0);
    assert_eq!(Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[1].1);


    assert_eq!("test", statement.from);
    assert_eq!(Some(vec!["x".to_owned()]), statement.group_by);
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
        group_by: Some(vec!["x".to_owned()]),
        having: None
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

    assert_eq!("sum1", statement.aggregates[1].0);
    assert_eq!(Aggregate::Sum(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[1].1);


    assert_eq!("test", statement.from);
    assert_eq!(Some(vec!["x".to_owned()]), statement.group_by);
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
        group_by: Some(vec!["x".to_owned()]),
        having: None
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

    assert_eq!("count1", statement.aggregates[1].0);
    assert_eq!(Aggregate::Count, statement.aggregates[1].1);

    assert_eq!("test", statement.from);
    assert_eq!(Some(vec!["x".to_owned()]), statement.group_by);
}

#[test]
fn test_aggregate_statement4() {
    let tree = ParseOperationTree::Select {
        projections: vec![
            (None, ParseExpressionTree::Call("COUNT".to_owned(), vec![])),
        ],
        from: ("test".to_string(), None),
        filter: None,
        group_by: None,
        having: None
    };

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.aggregates.len());
    assert_eq!("count0", statement.aggregates[0].0);
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
        group_by: None,
        having: None
    };

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.aggregates.len());
    assert_eq!("max0", statement.aggregates[0].0);
    assert_eq!(Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[0].1);
}

#[test]
fn test_aggregate_statement6() {
    let tree = ParseOperationTree::Select {
        projections: vec![
            (None, ParseExpressionTree::Call("MAX".to_owned(), vec![ParseExpressionTree::ColumnAccess("x".to_owned())])),
        ],
        from: ("test".to_string(), None),
        filter: None,
        group_by: None,
        having: Some(
            ParseExpressionTree::And {
                left: Box::new(ParseExpressionTree::BinaryOperator {
                    operator: Operator::Single('='),
                    left: Box::new(ParseExpressionTree::ColumnAccess("x".to_owned())),
                    right: Box::new(ParseExpressionTree::Value(Value::Int(1337))),
                }),
                right: Box::new(ParseExpressionTree::BinaryOperator {
                    operator: Operator::Single('>'),
                    left: Box::new(ParseExpressionTree::Call("MAX".to_owned(), vec![ParseExpressionTree::ColumnAccess("x".to_owned())])),
                    right: Box::new(ParseExpressionTree::Value(Value::Int(2000))),
                })
            }
        )
    };

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.aggregates.len());
    assert_eq!("max0", statement.aggregates[0].0);
    assert_eq!(Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[0].1);
    assert_eq!(
        Some(
            ExpressionTree::And {
                left: Box::new(ExpressionTree::Compare {
                    operator: CompareOperator::Equal,
                    left: Box::new(ExpressionTree::Aggregate(0, Box::new(Aggregate::GroupKey("x".to_owned())))),
                    right: Box::new(ExpressionTree::Value(Value::Int(1337))),
                }),
                right: Box::new(ExpressionTree::Compare {
                    operator: CompareOperator::GreaterThan,
                    left: Box::new(ExpressionTree::Aggregate(1, Box::new(Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned()))))),
                    right: Box::new(ExpressionTree::Value(Value::Int(2000))),
                })
            }
        ),
        statement.having
    );
}

#[test]
fn test_create_table_statement1() {
    let tree = ParseOperationTree::CreateTable {
        name: "test".to_string(),
        patterns: vec![("line".to_owned(), "A: ([0-9]+)".to_owned())],
        columns: vec![
            ParseColumnDefinition::new(
                "line".to_string(),
                1,
                "x".to_string(),
                ValueType::Int
            )
        ]
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