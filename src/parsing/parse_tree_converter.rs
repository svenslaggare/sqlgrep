use std::fmt::Formatter;
use std::collections::HashSet;
use std::collections::HashMap;
use std::iter::FromIterator;

use lazy_static::lazy_static;

use crate::parsing::parser::{ParserOperationTree, ParserExpressionTreeData, ParserColumnDefinition, ParserJoinClause, ParserExpressionTree};
use crate::model::{Statement, ExpressionTree, ArithmeticOperator, CompareOperator, SelectStatement, Value, Aggregate, AggregateStatement, ValueType, UnaryArithmeticOperator, Function, JoinClause};
use crate::data_model::{ColumnDefinition, TableDefinition, ColumnParsing, RegexResultReference, RegexMode};
use crate::parsing::operator::Operator;
use crate::parsing::tokenizer::TokenLocation;

#[derive(Debug)]
pub struct ConvertParseTreeError {
    pub location: TokenLocation,
    pub error: ConvertParseTreeErrorType
}

impl ConvertParseTreeError {
    pub fn new(location: TokenLocation, error: ConvertParseTreeErrorType) -> ConvertParseTreeError {
        ConvertParseTreeError {
            location,
            error
        }
    }
}

#[derive(Debug)]
pub enum ConvertParseTreeErrorType {
    UndefinedOperator(Operator),
    ExpectedArgument,
    TooManyArguments,
    ExpectedColumnAccess,
    UndefinedAggregate,
    UndefinedStatement,
    UndefinedExpression,
    UndefinedFunction(String),
    InvalidPattern,
    HavingClauseNotPossible,
    InvalidOnJoin,
    InvalidJoinerTable(String)
}

impl ConvertParseTreeErrorType {
    pub fn with_location(self, location: TokenLocation) -> ConvertParseTreeError {
        ConvertParseTreeError::new(location, self)
    }
}

impl std::fmt::Display for ConvertParseTreeErrorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConvertParseTreeErrorType::UndefinedOperator(operator) => { write!(f, "The operator '{}' is not defined", operator) }
            ConvertParseTreeErrorType::ExpectedArgument => { write!(f, "Expected an argument") }
            ConvertParseTreeErrorType::TooManyArguments => { write!(f, "Too many arguments") }
            ConvertParseTreeErrorType::ExpectedColumnAccess => { write!(f, "Expected column access") }
            ConvertParseTreeErrorType::UndefinedAggregate => { write!(f, "Undefined aggregate") }
            ConvertParseTreeErrorType::UndefinedStatement => { write!(f, "Undefined statement") }
            ConvertParseTreeErrorType::UndefinedExpression => { write!(f, "Undefined expression") }
            ConvertParseTreeErrorType::UndefinedFunction(name) => { write!(f, "Undefined function: {}", name) }
            ConvertParseTreeErrorType::InvalidPattern => { write!(f, "Undefined pattern") }
            ConvertParseTreeErrorType::HavingClauseNotPossible => { write!(f, "Having clause only available for aggregate expressions") },
            ConvertParseTreeErrorType::InvalidOnJoin => { write!(f, "Left or right join side does not exist") },
            ConvertParseTreeErrorType::InvalidJoinerTable(table) => { write!(f, "Expected left or right side to be {}", table) },
        }
    }
}

pub fn transform_statement(tree: ParserOperationTree) -> Result<Statement, ConvertParseTreeError> {
    match tree {
        ParserOperationTree::Select { location, projections, from, filter, group_by, having, join } => {
            if let Some(group_by) = group_by {
                create_aggregate_statement(location, projections, from, filter, Some(group_by), having, join)
            } else {
                if any_aggregates(&projections) {
                    create_aggregate_statement(location, projections, from, filter, None, having, join)
                } else {
                    if having.is_some() {
                        return Err(ConvertParseTreeErrorType::HavingClauseNotPossible.with_location(location));
                    }

                    create_select_statement(location, projections, from, filter, join)
                }
            }
        }
        ParserOperationTree::CreateTable { location, name, patterns, columns } => create_create_table_statement(location, name, patterns, columns),
        ParserOperationTree::Multiple(statements) => {
            let mut transformed_statements = Vec::new();
            for statement in statements {
                transformed_statements.push(transform_statement(statement)?);
            }

            Ok(Statement::Multiple(transformed_statements))
        }
    }
}

fn create_select_statement(location: TokenLocation,
                           projections: Vec<(Option<String>, ParserExpressionTree)>,
                           from: (String, Option<String>),
                           filter: Option<ParserExpressionTree>,
                           join: Option<ParserJoinClause>) -> Result<Statement, ConvertParseTreeError> {
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

    let transformed_join = transform_join(location, &from, join)?;

    let select_statement = SelectStatement {
        projections: transformed_projections,
        from: from.0,
        filename: from.1,
        filter: transformed_filter,
        join: transformed_join
    };

    Ok(Statement::Select(select_statement))
}

fn create_aggregate_statement(location: TokenLocation,
                              projections: Vec<(Option<String>, ParserExpressionTree)>,
                              from: (String, Option<String>),
                              filter: Option<ParserExpressionTree>,
                              group_by: Option<Vec<String>>,
                              having: Option<ParserExpressionTree>,
                              join: Option<ParserJoinClause>) -> Result<Statement, ConvertParseTreeError> {
    let mut transformed_aggregates = Vec::new();
    for (projection_index, (name, tree)) in projections.into_iter().enumerate() {
        let (default_name, aggregate, transform) = transform_aggregate(tree, projection_index)?;
        let name = name.or(default_name).unwrap_or(format!("p{}", projection_index));

        transformed_aggregates.push((name, aggregate, transform));
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

    let transformed_join = transform_join(location, &from, join)?;

    let aggregate_statement = AggregateStatement {
        aggregates: transformed_aggregates,
        from: from.0,
        filename: from.1,
        filter: transformed_filter,
        group_by,
        having: transformed_having,
        join: transformed_join
    };

    Ok(Statement::Aggregate(aggregate_statement))
}

fn transform_join(location: TokenLocation,
                  from: &(String, Option<String>),
                  join: Option<ParserJoinClause>) -> Result<Option<JoinClause>, ConvertParseTreeError> {
    if let Some(join) = join {
        if join.left_table == from.0 {
            if join.right_table == join.joiner_table {
                Ok(
                    Some(
                        JoinClause {
                            joined_table: join.joiner_table,
                            joined_filename: join.joiner_filename,
                            joined_column: join.right_column,
                            joiner_column: join.left_column,
                            is_outer: join.is_outer
                        }
                    )
                )
            } else {
                return Err(ConvertParseTreeErrorType::InvalidJoinerTable(join.joiner_table).with_location(location));
            }
        } else if join.right_table == from.0 {
            if join.left_table == join.joiner_table {
                Ok(
                    Some(
                        JoinClause {
                            joined_table: join.joiner_table,
                            joined_filename: join.joiner_filename,
                            joined_column: join.left_column,
                            joiner_column: join.right_column,
                            is_outer: join.is_outer
                        }
                    )
                )
            } else {
                return Err(ConvertParseTreeErrorType::InvalidJoinerTable(join.joiner_table).with_location(location));
            }
        } else {
            return Err(ConvertParseTreeErrorType::InvalidOnJoin.with_location(location));
        }
    } else {
        Ok(None)
    }
}

fn create_create_table_statement(location: TokenLocation,
                                 name: String,
                                 patterns: Vec<(String, String, RegexMode)>,
                                 columns: Vec<ParserColumnDefinition>) -> Result<Statement, ConvertParseTreeError> {
    let column_definitions = columns
        .into_iter()
        .map(|column| {
            let mut column_definition = ColumnDefinition::with_parsing(
                column.parsing,
                &column.name,
                column.column_type
            );

            if let Some(nullable) = column.nullable.as_ref() {
                column_definition.options.nullable = *nullable;
            }

            if let Some(trim) = column.trim.as_ref() {
                column_definition.options.trim = *trim;
            }

            if let Some(convert) = column.convert.as_ref() {
                column_definition.options.convert = *convert;
            }

            column_definition.options.default_value = column.default_value.clone();

            column_definition
        })
        .collect::<Vec<_>>();

    let table_definition = TableDefinition::new(
        &name,
        patterns.iter().map(|(pattern_name, pattern, mode)| (pattern_name.as_str(), pattern.as_str(), mode.clone())).collect(),
        column_definitions,
    ).ok_or(ConvertParseTreeErrorType::InvalidPattern.with_location(location))?;

    Ok(Statement::CreateTable(table_definition))
}

lazy_static! {
    static ref AGGREGATE_FUNCTIONS: HashSet<String> = HashSet::from_iter(
        vec![
            "count".to_owned(),
            "min".to_owned(),
            "max".to_owned(),
            "avg".to_owned(),
            "sum".to_owned(),
            "array_agg".to_owned(),
            "array_agg_unique".to_owned()
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
            ("lower".to_owned(), Function::StringToLower),
            ("regexp_matches".to_owned(), Function::RegexMatches),
            ("array_unique".to_owned(), Function::ArrayUnique),
            ("array_length".to_owned(), Function::ArrayLength),
            ("now".to_owned(), Function::TimestampNow),
            ("make_timestamp".to_owned(), Function::MakeTimestamp),
            ("timestamp_extract_epoch".to_owned(), Function::TimestampExtractEpoch),
            ("timestamp_extract_year".to_owned(), Function::TimestampExtractYear),
            ("timestamp_extract_month".to_owned(), Function::TimestampExtractMonth),
            ("timestamp_extract_day".to_owned(), Function::TimestampExtractDay),
            ("timestamp_extract_hour".to_owned(), Function::TimestampExtractHour),
            ("timestamp_extract_minute".to_owned(), Function::TimestampExtractMinute),
            ("timestamp_extract_second".to_owned(), Function::TimestampExtractSecond)
        ].into_iter()
    );
}

fn any_aggregates(projections: &Vec<(Option<String>, ParserExpressionTree)>) -> bool {
    projections.iter().any(|(_, projection)| any_aggregates_in_expression(projection))
}

fn any_aggregates_in_expression(expression: &ParserExpressionTree) -> bool {
    let mut is_aggregate = false;
    expression.tree.visit::<(), _>(&mut |expr| {
        match expr {
            ParserExpressionTreeData::Call { name, arguments: _ } if AGGREGATE_FUNCTIONS.contains(&name.to_lowercase()) => {
                is_aggregate = true;
            }
            _ => {}
        }
        Ok(())
    }).unwrap();

    is_aggregate
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

pub fn transform_expression(tree: ParserExpressionTree, state: &mut TransformExpressionState) -> Result<ExpressionTree, ConvertParseTreeError> {
    match tree.tree {
        ParserExpressionTreeData::Value(value) => Ok(ExpressionTree::Value(value)),
        ParserExpressionTreeData::ColumnAccess(name) => {
            if state.allow_aggregates {
                let aggregate_index = state.next_aggregate_index;
                state.next_aggregate_index += 1;
                Ok(ExpressionTree::Aggregate(aggregate_index, Box::new(Aggregate::GroupKey(name))))
            } else {
                Ok(ExpressionTree::ColumnAccess(name))
            }
        }
        ParserExpressionTreeData::Wildcard => Ok(ExpressionTree::Wildcard),
        ParserExpressionTreeData::BinaryOperator { operator, left, right } => {
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
                _ => { return Err(ConvertParseTreeErrorType::UndefinedOperator(operator).with_location(tree.location)); }
            }
        }
        ParserExpressionTreeData::UnaryOperator { operator, operand } => {
            let operand = Box::new(transform_expression(*operand, state)?);

            match operator {
                Operator::Single('-') => Ok(ExpressionTree::UnaryArithmetic { operator: UnaryArithmeticOperator::Negative, operand }),
                _ => { return Err(ConvertParseTreeErrorType::UndefinedOperator(operator).with_location(tree.location)); }
            }
        }
        ParserExpressionTreeData::Invert { operand } => {
            let operand = Box::new(transform_expression(*operand, state)?);
            Ok(ExpressionTree::UnaryArithmetic { operator: UnaryArithmeticOperator::Invert, operand })
        }
        ParserExpressionTreeData::Is { left, right } => {
            let left = Box::new(transform_expression(*left, state)?);
            let right = Box::new(transform_expression(*right, state)?);

            Ok(ExpressionTree::Is { left, right })
        }
        ParserExpressionTreeData::IsNot { left, right } => {
            let left = Box::new(transform_expression(*left, state)?);
            let right = Box::new(transform_expression(*right, state)?);

            Ok(ExpressionTree::IsNot { left, right })
        }
        ParserExpressionTreeData::And { left, right } => {
            let left = Box::new(transform_expression(*left, state)?);
            let right = Box::new(transform_expression(*right, state)?);

            Ok(ExpressionTree::And { left, right })
        }
        ParserExpressionTreeData::Or { left, right } => {
            let left = Box::new(transform_expression(*left, state)?);
            let right = Box::new(transform_expression(*right, state)?);

            Ok(ExpressionTree::Or { left, right })
        }
        ParserExpressionTreeData::Call { name, arguments } => {
            if state.allow_aggregates {
                match transform_call_aggregate(tree.location.clone(), &name, arguments.clone(), 0) {
                    Ok((_, aggregate, _)) => {
                        let aggregate_index = state.next_aggregate_index;
                        state.next_aggregate_index += 1;
                        return Ok(ExpressionTree::Aggregate(aggregate_index, Box::new(aggregate)));
                    }
                    Err(err) => {
                        match err.error {
                            ConvertParseTreeErrorType::UndefinedAggregate => {}
                            _ => { return Err(err); }
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
            }).ok_or(ConvertParseTreeErrorType::UndefinedFunction(name).with_location(tree.location.clone()))
        }
        ParserExpressionTreeData::ArrayElementAccess { array, index } => {
            let array = Box::new(transform_expression(*array, state)?);
            let index = Box::new(transform_expression(*index, state)?);
            Ok(ExpressionTree::ArrayElementAccess { array, index })
        }
    }
}

fn transform_aggregate(tree: ParserExpressionTree, aggregate_index: usize) -> Result<(Option<String>, Aggregate, Option<ExpressionTree>), ConvertParseTreeError> {
    let mut aggregate = Box::new(ParserExpressionTreeData::ColumnAccess("$agg".to_owned()).with_location(tree.location.clone()));
    match tree.tree {
        ParserExpressionTreeData::ColumnAccess(name) => Ok((Some(name.clone()), Aggregate::GroupKey(name), None)),
        ParserExpressionTreeData::Call { name, mut arguments } => {
            let mut aggregate = *aggregate;

            let mut aggregate_in_argument = false;
            for argument in &mut arguments {
                if any_aggregates_in_expression(argument) {
                    std::mem::swap(&mut aggregate, argument);
                    aggregate_in_argument = true;
                    break;
                }
            }

            if aggregate_in_argument {
                let (aggregate_name, aggregate, _) = transform_aggregate(aggregate, aggregate_index)?;
                let transform = transform_expression(
                    ParserExpressionTreeData::Call { name, arguments }.with_location(tree.location),
                    &mut TransformExpressionState::default()
                )?;

                Ok((aggregate_name, aggregate, Some(transform)))
            } else {
                transform_call_aggregate(tree.location, &name, arguments, aggregate_index)
            }
        }
        ParserExpressionTreeData::BinaryOperator { operator, mut left, mut right } => {
            if any_aggregates_in_expression(&left) {
                std::mem::swap(&mut aggregate, &mut left);
            } else if any_aggregates_in_expression(&right) {
                std::mem::swap(&mut aggregate, &mut right);
            }

            let (aggregate_name, aggregate, _) = transform_aggregate(*aggregate, aggregate_index)?;
            let transform = transform_expression(
                ParserExpressionTreeData::BinaryOperator { operator, left, right }.with_location(tree.location),
                &mut TransformExpressionState::default()
            )?;

            Ok((aggregate_name, aggregate, Some(transform)))
        }
        ParserExpressionTreeData::UnaryOperator { operator, mut operand } => {
            if any_aggregates_in_expression(&operand) {
                std::mem::swap(&mut aggregate, &mut operand);
            }

            let (aggregate_name, aggregate, _) = transform_aggregate(*aggregate, aggregate_index)?;
            let transform = transform_expression(
                ParserExpressionTreeData::UnaryOperator { operator, operand }.with_location(tree.location),
                &mut TransformExpressionState::default()
            )?;

            Ok((aggregate_name, aggregate, Some(transform)))
        }
        ParserExpressionTreeData::ArrayElementAccess { mut array, mut index } => {
            if any_aggregates_in_expression(&array) {
                std::mem::swap(&mut aggregate, &mut array);
            } else if any_aggregates_in_expression(&index) {
                std::mem::swap(&mut aggregate, &mut index);
            }

            let (aggregate_name, aggregate, _) = transform_aggregate(*aggregate, aggregate_index)?;
            let transform = transform_expression(
                ParserExpressionTreeData::ArrayElementAccess { array, index }.with_location(tree.location),
                &mut TransformExpressionState::default()
            )?;

            Ok((aggregate_name, aggregate, Some(transform)))
        }
        ParserExpressionTreeData::And { mut left, mut right } => {
            if any_aggregates_in_expression(&left) {
                std::mem::swap(&mut aggregate, &mut left);
            } else if any_aggregates_in_expression(&right) {
                std::mem::swap(&mut aggregate, &mut right);
            }

            let (aggregate_name, aggregate, _) = transform_aggregate(*aggregate, aggregate_index)?;
            let transform = transform_expression(
                ParserExpressionTreeData::And { left, right }.with_location(tree.location),
                &mut TransformExpressionState::default()
            )?;

            Ok((aggregate_name, aggregate, Some(transform)))
        }
        ParserExpressionTreeData::Or { mut left, mut right } => {
            if any_aggregates_in_expression(&left) {
                std::mem::swap(&mut aggregate, &mut left);
            } else if any_aggregates_in_expression(&right) {
                std::mem::swap(&mut aggregate, &mut right);
            }

            let (aggregate_name, aggregate, _) = transform_aggregate(*aggregate, aggregate_index)?;
            let transform = transform_expression(
                ParserExpressionTreeData::Or { left, right }.with_location(tree.location),
                &mut TransformExpressionState::default()
            )?;

            Ok((aggregate_name, aggregate, Some(transform)))
        }
        ParserExpressionTreeData::Is { mut left, mut right } => {
            if any_aggregates_in_expression(&left) {
                std::mem::swap(&mut aggregate, &mut left);
            } else if any_aggregates_in_expression(&right) {
                std::mem::swap(&mut aggregate, &mut right);
            }

            let (aggregate_name, aggregate, _) = transform_aggregate(*aggregate, aggregate_index)?;
            let transform = transform_expression(
                ParserExpressionTreeData::Is { left, right }.with_location(tree.location),
                &mut TransformExpressionState::default()
            )?;

            Ok((aggregate_name, aggregate, Some(transform)))
        }
        ParserExpressionTreeData::IsNot { mut left, mut right } => {
            if any_aggregates_in_expression(&left) {
                std::mem::swap(&mut aggregate, &mut left);
            } else if any_aggregates_in_expression(&right) {
                std::mem::swap(&mut aggregate, &mut right);
            }

            let (aggregate_name, aggregate, _) = transform_aggregate(*aggregate, aggregate_index)?;
            let transform = transform_expression(
                ParserExpressionTreeData::IsNot { left, right }.with_location(tree.location),
                &mut TransformExpressionState::default()
            )?;

            Ok((aggregate_name, aggregate, Some(transform)))
        }
        ParserExpressionTreeData::Invert { mut operand } => {
            if any_aggregates_in_expression(&operand) {
                std::mem::swap(&mut aggregate, &mut operand);
            }

            let (aggregate_name, aggregate, _) = transform_aggregate(*aggregate, aggregate_index)?;
            let transform = transform_expression(
                ParserExpressionTreeData::Invert { operand }.with_location(tree.location),
                &mut TransformExpressionState::default()
            )?;

            Ok((aggregate_name, aggregate, Some(transform)))
        }
        _ => { return Err(ConvertParseTreeErrorType::UndefinedAggregate.with_location(tree.location)); }
    }
}

fn transform_call_aggregate(location: TokenLocation,
                            name: &str,
                            mut arguments: Vec<ParserExpressionTree>,
                            index: usize) -> Result<(Option<String>, Aggregate, Option<ExpressionTree>), ConvertParseTreeError> {
    let name_lowercase = name.to_lowercase();
    if name_lowercase == "count" {
        let default_name = Some(format!("count{}", index));
        if arguments.is_empty() {
            Ok((default_name, Aggregate::Count(None), None))
        } else if arguments.len() == 1 {
            let argument = arguments.remove(0);
            let location = argument.location.clone();
            let expression = transform_expression(argument, &mut TransformExpressionState::default())?;
            match expression {
                ExpressionTree::Wildcard => Ok((default_name, Aggregate::Count(None), None)),
                ExpressionTree::ColumnAccess(column) => Ok((default_name, Aggregate::Count(Some(column)), None)),
                _ => { Err(ConvertParseTreeErrorType::ExpectedColumnAccess.with_location(location)) }
            }
        } else {
            Err(ConvertParseTreeErrorType::TooManyArguments.with_location(location))
        }
    } else if AGGREGATE_FUNCTIONS.contains(&name_lowercase) {
        if arguments.len() == 1 {
            let expression = transform_expression(arguments.remove(0), &mut TransformExpressionState::default())?;
            let aggregate = match name_lowercase.as_str() {
                "max" => Aggregate::Max(expression),
                "min" => Aggregate::Min(expression),
                "avg" => Aggregate::Average(expression),
                "sum" => Aggregate::Sum(expression),
                "array_agg" => Aggregate::CollectArray(expression, false),
                "array_agg_unique" => Aggregate::CollectArray(expression, true),
                _ => { panic!("should not happen") }
            };

            Ok((Some(format!("{}{}", name_lowercase, index)), aggregate, None))
        } else if arguments.is_empty() {
            Err(ConvertParseTreeErrorType::ExpectedArgument.with_location(location))
        } else {
            Err(ConvertParseTreeErrorType::TooManyArguments.with_location(location))
        }
    } else {
        Err(ConvertParseTreeErrorType::UndefinedAggregate.with_location(location))
    }
}

#[test]
fn test_select_statement1() {
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![(None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default()))],
        from: ("test".to_string(), None),
        filter: None,
        group_by: None,
        having: None,
        join: None
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
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![(Some("x".to_owned()), ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default()))],
        from: ("test".to_string(), None),
        filter: None,
        group_by: None,
        having: None,
        join: None
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
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![(None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default()))],
        from: ("test".to_string(), None),
        filter: Some(
            ParserExpressionTreeData::BinaryOperator {
                operator: Operator::Single('>'),
                left: Box::new(ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())),
                right: Box::new(ParserExpressionTreeData::Value(Value::Int(10)).with_location(Default::default()))
            }.with_location(Default::default())
        ),
        group_by: None,
        having: None,
        join: None
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
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![
            (
                None,
                ParserExpressionTreeData::BinaryOperator {
                    operator: Operator::Single('*'),
                    left: Box::new(ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())),
                    right: Box::new(ParserExpressionTreeData::Value(Value::Int(2)).with_location(Default::default())),
                }.with_location(Default::default())
            )
        ],
        from: ("test".to_string(), None),
        filter: Some(
            ParserExpressionTreeData::BinaryOperator {
                operator: Operator::Single('>'),
                left: Box::new(ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())),
                right: Box::new(ParserExpressionTreeData::Value(Value::Int(10)).with_location(Default::default()))
            }.with_location(Default::default())
        ),
        group_by: None,
        having: None,
        join: None
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
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![(None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default()))],
        from: ("test".to_string(), Some("test.log".to_owned())),
        filter: None,
        group_by: None,
        having: None,
        join: None,
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
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![
            (
                None,
                ParserExpressionTreeData::UnaryOperator {
                    operator: Operator::Single('-'),
                    operand: Box::new(ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default()))
                }.with_location(Default::default())
            )
        ],
        from: ("test".to_string(), None),
        filter: None,
        group_by: None,
        having: None,
        join: None
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
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![
            (
                None,
                ParserExpressionTreeData::Call {
                    name: "GREATEST".to_owned(),
                    arguments: vec![
                        ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default()),
                        ParserExpressionTreeData::ColumnAccess("y".to_owned()).with_location(Default::default())
                    ]
                }.with_location(Default::default())
            )
        ],
        from: ("test".to_string(), None),
        filter: None,
        group_by: None,
        having: None,
        join: None
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
fn test_select_inner_join1() {
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![(None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default()))],
        from: ("test".to_string(), Some("test.log".to_owned())),
        filter: None,
        group_by: None,
        having: None,
        join: Some(ParserJoinClause {
            joiner_table: "other".to_string(),
            joiner_filename: "other.log".to_string(),
            left_table: "test".to_string(),
            left_column: "x".to_string(),
            right_table: "other".to_string(),
            right_column: "y".to_string(),
            is_outer: false
        }),
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

    assert_eq!(
        Some(JoinClause {
            joined_table: "other".to_string(),
            joined_filename: "other.log".to_string(),
            joined_column: "y".to_string(),
            joiner_column: "x".to_string(),
            is_outer: false
        }),
        statement.join
    );
}

#[test]
fn test_select_inner_join2() {
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![(None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default()))],
        from: ("test".to_string(), Some("test.log".to_owned())),
        filter: None,
        group_by: None,
        having: None,
        join: Some(ParserJoinClause {
            joiner_table: "other".to_string(),
            joiner_filename: "other.log".to_string(),
            right_table: "test".to_string(),
            right_column: "x".to_string(),
            left_table: "other".to_string(),
            left_column: "y".to_string(),
            is_outer: false
        }),
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

    assert_eq!(
        Some(JoinClause {
            joined_table: "other".to_string(),
            joined_filename: "other.log".to_string(),
            joined_column: "y".to_string(),
            joiner_column: "x".to_string(),
            is_outer: false
        }),
        statement.join
    );
}

#[test]
fn test_aggregate_statement1() {
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![
            (None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())),
            (
                None,
                ParserExpressionTreeData::Call {
                    name: "MAX".to_owned(),
                    arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())]
                }.with_location(Default::default())
            )
        ],
        from: ("test".to_string(), None),
        filter: None,
        group_by: Some(vec!["x".to_owned()]),
        having: None,
        join: None
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
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![
            (None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())),
            (
                None,
                ParserExpressionTreeData::Call {
                    name: "SUM".to_owned(),
                    arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())]
                }.with_location(Default::default())
            ),
        ],
        from: ("test".to_string(), None),
        filter: None,
        group_by: Some(vec!["x".to_owned()]),
        having: None,
        join: None,
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
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![
            (None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())),
            (None, ParserExpressionTreeData::Call { name: "COUNT".to_owned(), arguments: vec![] }.with_location(Default::default()) ),
        ],
        from: ("test".to_string(), None),
        filter: None,
        group_by: Some(vec!["x".to_owned()]),
        having: None,
        join: None,
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
    assert_eq!(Aggregate::Count(None), statement.aggregates[1].1);

    assert_eq!("test", statement.from);
    assert_eq!(Some(vec!["x".to_owned()]), statement.group_by);
}

#[test]
fn test_aggregate_statement4() {
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![
            (None, ParserExpressionTreeData::Call { name: "COUNT".to_owned(), arguments: vec![] }.with_location(Default::default()) ),
        ],
        from: ("test".to_string(), None),
        filter: None,
        group_by: None,
        having: None,
        join: None,
    };

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.aggregates.len());
    assert_eq!("count0", statement.aggregates[0].0);
    assert_eq!(Aggregate::Count(None), statement.aggregates[0].1);
}

#[test]
fn test_aggregate_statement5() {
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![
            (
                None,
                ParserExpressionTreeData::Call {
                    name: "MAX".to_owned(),
                    arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())]
                }.with_location(Default::default())
            ),
        ],
        from: ("test".to_string(), None),
        filter: None,
        group_by: None,
        having: None,
        join: None,
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
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![
            (
                None,
                ParserExpressionTreeData::Call {
                    name: "MAX".to_owned(),
                    arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())]
                }.with_location(Default::default())
            ),
        ],
        from: ("test".to_string(), None),
        filter: None,
        group_by: None,
        having: Some(
            ParserExpressionTreeData::And {
                left: Box::new(
                    ParserExpressionTreeData::BinaryOperator {
                        operator: Operator::Single('='),
                        left: Box::new(ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())),
                        right: Box::new(ParserExpressionTreeData::Value(Value::Int(1337)).with_location(Default::default())),
                    }.with_location(Default::default())
                ),
                right: Box::new(
                    ParserExpressionTreeData::BinaryOperator {
                        operator: Operator::Single('>'),
                        left: Box::new(ParserExpressionTreeData::Call {
                            name: "MAX".to_owned(),
                            arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())]
                        }.with_location(Default::default())),
                        right: Box::new(ParserExpressionTreeData::Value(Value::Int(2000)).with_location(Default::default())),
                    }.with_location(Default::default())
                )
            }.with_location(Default::default())
        ),
        join: None,
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
fn test_aggregate_statement7() {
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![
            (
                None,
                ParserExpressionTreeData::BinaryOperator {
                    operator: Operator::Single('*'),
                    left: Box::new(
                        ParserExpressionTreeData::Call {
                            name: "MAX".to_owned(),
                            arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())]
                        }.with_location(Default::default())
                    ),
                    right: Box::new(ParserExpressionTreeData::Value(Value::Int(2)).with_location(Default::default()))
                }.with_location(Default::default())
            ),
        ],
        from: ("test".to_string(), None),
        filter: None,
        group_by: None,
        having: None,
        join: None,
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
        Some(ExpressionTree::Arithmetic {
            operator: ArithmeticOperator::Multiply,
            left: Box::new(ExpressionTree::ColumnAccess("$agg".to_owned())),
            right: Box::new(ExpressionTree::Value(Value::Int(2)))
        }),
        statement.aggregates[0].2
    );
}

#[test]
fn test_aggregate_statement8() {
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![
            (
                None,
                ParserExpressionTreeData::BinaryOperator {
                    operator: Operator::Single('*'),
                    left: Box::new(ParserExpressionTreeData::Value(Value::Int(2)).with_location(Default::default())),
                    right: Box::new(ParserExpressionTreeData::Call {
                        name: "MAX".to_owned(),
                        arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())]
                    }.with_location(Default::default())),
                }.with_location(Default::default())
            ),
        ],
        from: ("test".to_string(), None),
        filter: None,
        group_by: None,
        having: None,
        join: None,
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
        Some(ExpressionTree::Arithmetic {
            operator: ArithmeticOperator::Multiply,
            left: Box::new(ExpressionTree::Value(Value::Int(2))),
            right: Box::new(ExpressionTree::ColumnAccess("$agg".to_owned())),
        }),
        statement.aggregates[0].2
    );
}

#[test]
fn test_aggregate_statement9() {
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![
            (
                None,
                ParserExpressionTreeData::Call {
                    name: "SQRT".to_owned(),
                    arguments: vec![
                        ParserExpressionTreeData::Call {
                            name: "MAX".to_owned(),
                            arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())]
                        }.with_location(Default::default())
                    ]
                }.with_location(Default::default())
            ),
        ],
        from: ("test".to_string(), None),
        filter: None,
        group_by: None,
        having: None,
        join: None,
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
        Some(ExpressionTree::Function {
            function: Function::Sqrt,
            arguments: vec![ExpressionTree::ColumnAccess("$agg".to_owned())]
        }),
        statement.aggregates[0].2
    );
}

#[test]
fn test_create_table_statement1() {
    let tree = ParserOperationTree::CreateTable {
        location: Default::default(),
        name: "test".to_string(),
        patterns: vec![("line".to_owned(), "A: ([0-9]+)".to_owned(), RegexMode::Captures)],
        columns: vec![
            ParserColumnDefinition::new(
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
    assert_eq!(ColumnParsing::Regex(RegexResultReference { pattern_name: "line".to_string(), group_index: 1 }), table_definition.columns[0].parsing);
}