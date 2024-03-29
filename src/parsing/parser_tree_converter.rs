use std::fmt::Formatter;
use std::collections::HashSet;
use std::collections::HashMap;
use std::iter::FromIterator;

use lazy_static::lazy_static;

use crate::data_model::{ColumnDefinition, RegexMode, TableDefinition};
use crate::execution::ColumnScope;
use crate::helpers::{IterExt, tuple_result};
use crate::model::{Aggregate, AggregateStatement, AggregateStatementAggregation, ArithmeticOperator, CompareOperator, ExpressionTree, Float, JoinClause, SelectStatement, UnaryArithmeticOperator, Value};
use crate::parsing::parser::{ParserOperationTree, ParserExpressionTreeData, ParserColumnDefinition, ParserJoinClause, ParserExpressionTree};
use crate::parsing::operator::Operator;
use crate::parsing::tokenizer::TokenLocation;
use crate::Statement;
use crate::model::Function;

#[derive(Debug, PartialEq)]
pub struct ConvertParserTreeError {
    pub location: TokenLocation,
    pub error: ConvertParserTreeErrorType
}

impl ConvertParserTreeError {
    pub fn new(location: TokenLocation, error: ConvertParserTreeErrorType) -> ConvertParserTreeError {
        ConvertParserTreeError {
            location,
            error
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ConvertParserTreeErrorType {
    UndefinedOperator(Operator),
    ExpectedArgument,
    TooManyArguments,
    ExpectedColumnAccess,
    UnexpectedTuple,
    UndefinedAggregate,
    TooManyAggregates,
    UndefinedStatement,
    UndefinedExpression,
    UndefinedFunction(String),
    InvalidPattern,
    HavingClauseNotPossible,
    InvalidOnJoin,
    InvalidJoinerTable(String),
    ExpectedFloat,
    ExpectedString
}

impl ConvertParserTreeErrorType {
    pub fn with_location(self, location: TokenLocation) -> ConvertParserTreeError {
        ConvertParserTreeError::new(location, self)
    }
}

impl std::fmt::Display for ConvertParserTreeErrorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConvertParserTreeErrorType::UndefinedOperator(operator) => { write!(f, "The operator '{}' is not defined", operator) }
            ConvertParserTreeErrorType::ExpectedArgument => { write!(f, "Expected an argument") }
            ConvertParserTreeErrorType::TooManyArguments => { write!(f, "Too many arguments") }
            ConvertParserTreeErrorType::ExpectedColumnAccess => { write!(f, "Expected column access") }
            ConvertParserTreeErrorType::UnexpectedTuple => { write!(f, "Tuple not expected here") }
            ConvertParserTreeErrorType::UndefinedAggregate => { write!(f, "Undefined aggregate") }
            ConvertParserTreeErrorType::TooManyAggregates => { write!(f, "Too many aggregates") }
            ConvertParserTreeErrorType::UndefinedStatement => { write!(f, "Undefined statement") }
            ConvertParserTreeErrorType::UndefinedExpression => { write!(f, "Undefined expression") }
            ConvertParserTreeErrorType::UndefinedFunction(name) => { write!(f, "Undefined function: {}", name) }
            ConvertParserTreeErrorType::InvalidPattern => { write!(f, "Undefined pattern") }
            ConvertParserTreeErrorType::HavingClauseNotPossible => { write!(f, "Having clause only available for aggregate expressions") },
            ConvertParserTreeErrorType::InvalidOnJoin => { write!(f, "Left or right join side does not exist") },
            ConvertParserTreeErrorType::InvalidJoinerTable(table) => { write!(f, "Expected left or right side to be {}", table) },
            ConvertParserTreeErrorType::ExpectedFloat => { write!(f, "Expected floating point value") },
            ConvertParserTreeErrorType::ExpectedString => { write!(f, "Expected string value") },
        }
    }
}

pub fn transform_statement(tree: ParserOperationTree) -> Result<Statement, ConvertParserTreeError> {
    match tree {
        ParserOperationTree::Select { location, projections, from, filter, group_by, having, join, limit, distinct } => {
            if let Some(group_by) = group_by {
                create_aggregate_statement(location, projections, from, filter, Some(group_by), having, join, limit, distinct)
            } else {
                if any_aggregates(&projections) {
                    create_aggregate_statement(location, projections, from, filter, None, having, join, limit, distinct)
                } else {
                    if having.is_some() {
                        return Err(ConvertParserTreeErrorType::HavingClauseNotPossible.with_location(location));
                    }

                    create_select_statement(location, projections, from, filter, join, limit, distinct)
                }
            }
        }
        ParserOperationTree::CreateTable { location, name, patterns, columns, .. } => create_create_table_statement(location, name, patterns, columns),
        ParserOperationTree::Multiple(statements) => {
            Ok(Statement::Multiple(statements.consume_result_vec(|statement| transform_statement(statement))?))
        }
    }
}

fn create_select_statement(location: TokenLocation,
                           projections: Vec<(Option<String>, ParserExpressionTree)>,
                           from: (String, Option<String>),
                           filter: Option<ParserExpressionTree>,
                           join: Option<ParserJoinClause>,
                           limit: Option<usize>,
                           distinct: bool) -> Result<Statement, ConvertParserTreeError> {
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
        join: transformed_join,
        limit,
        distinct,
    };

    Ok(Statement::Select(select_statement))
}

fn create_aggregate_statement(location: TokenLocation,
                              projections: Vec<(Option<String>, ParserExpressionTree)>,
                              from: (String, Option<String>),
                              filter: Option<ParserExpressionTree>,
                              group_by: Option<Vec<ParserExpressionTree>>,
                              having: Option<ParserExpressionTree>,
                              join: Option<ParserJoinClause>,
                              limit: Option<usize>,
                              distinct: bool) -> Result<Statement, ConvertParserTreeError> {
    let mut transformed_aggregates = Vec::new();
    for (projection_index, (name, tree)) in projections.into_iter().enumerate() {
        let (default_name, aggregate, transform) = transform_aggregate(tree, projection_index)?;
        let name = name.or(default_name).unwrap_or(format!("p{}", projection_index));

        transformed_aggregates.push(
            AggregateStatementAggregation {
                name,
                aggregate,
                transform,
            }
        );
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

    let group_by = if let Some(group_by) = group_by {
        Some(group_by.consume_result_vec(|part| transform_expression(part, &mut TransformExpressionState::default()))?)
    } else {
        None
    };

    let aggregate_statement = AggregateStatement {
        aggregates: transformed_aggregates,
        from: from.0,
        filename: from.1,
        filter: transformed_filter,
        group_by,
        having: transformed_having,
        join: transformed_join,
        limit,
        distinct
    };

    Ok(Statement::Aggregate(aggregate_statement))
}

fn transform_join(location: TokenLocation,
                  from: &(String, Option<String>),
                  join: Option<ParserJoinClause>) -> Result<Option<JoinClause>, ConvertParserTreeError> {
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
                return Err(ConvertParserTreeErrorType::InvalidJoinerTable(join.joiner_table).with_location(location));
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
                return Err(ConvertParserTreeErrorType::InvalidJoinerTable(join.joiner_table).with_location(location));
            }
        } else {
            return Err(ConvertParserTreeErrorType::InvalidOnJoin.with_location(location));
        }
    } else {
        Ok(None)
    }
}

fn create_create_table_statement(location: TokenLocation,
                                 name: String,
                                 patterns: Vec<(String, String, RegexMode)>,
                                 columns: Vec<ParserColumnDefinition>) -> Result<Statement, ConvertParserTreeError> {
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

            if let Some(microseconds) = column.microseconds.as_ref() {
                column_definition.options.microseconds = *microseconds;
            }

            column_definition.options.default_value = column.default_value.clone();

            column_definition
        })
        .collect::<Vec<_>>();

    let table_definition = TableDefinition::new(
        &name,
        patterns.iter().map(|(pattern_name, pattern, mode)| (pattern_name.as_str(), pattern.as_str(), mode.clone())).collect(),
        column_definitions,
    ).ok_or(ConvertParserTreeErrorType::InvalidPattern.with_location(location))?;

    Ok(Statement::CreateTable(table_definition))
}

lazy_static! {
    static ref AGGREGATE_FUNCTIONS: HashSet<String> = HashSet::from_iter(
        vec![
            "count".to_owned(),
            "min".to_owned(),
            "max".to_owned(),
            "sum".to_owned(),
            "avg".to_owned(),
            "stddev".to_owned(),
            "variance".to_owned(),
            "percentile".to_owned(),
            "bool_and".to_owned(),
            "bool_or".to_owned(),
            "array_agg".to_owned(),
            "string_agg".to_owned(),
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
            ("create_array".to_owned(), Function::CreateArray),
            ("array_unique".to_owned(), Function::ArrayUnique),
            ("array_length".to_owned(), Function::ArrayLength),
            ("array_cat".to_owned(), Function::ArrayCat),
            ("array_append".to_owned(), Function::ArrayAppend),
            ("array_prepend".to_owned(), Function::ArrayPrepend),
            ("now".to_owned(), Function::TimestampNow),
            ("make_timestamp".to_owned(), Function::MakeTimestamp),
            ("timestamp_extract_epoch".to_owned(), Function::TimestampExtractEpoch),
            ("timestamp_extract_year".to_owned(), Function::TimestampExtractYear),
            ("timestamp_extract_month".to_owned(), Function::TimestampExtractMonth),
            ("timestamp_extract_day".to_owned(), Function::TimestampExtractDay),
            ("timestamp_extract_hour".to_owned(), Function::TimestampExtractHour),
            ("timestamp_extract_minute".to_owned(), Function::TimestampExtractMinute),
            ("timestamp_extract_second".to_owned(), Function::TimestampExtractSecond),
            ("date_trunc".to_owned(), Function::TruncateTimestamp)
        ].into_iter()
    );
}

pub fn completion_words() -> Vec<String> {
    let mut words = Vec::new();
    words.extend(AGGREGATE_FUNCTIONS.iter().cloned());
    words.extend(FUNCTIONS.keys().cloned());

    for word in &mut words {
        *word = word.to_uppercase();
    }

    words.sort();
    words
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

pub fn transform_expression(tree: ParserExpressionTree, state: &mut TransformExpressionState) -> Result<ExpressionTree, ConvertParserTreeError> {
    match tree.tree {
        ParserExpressionTreeData::Value(value) => Ok(ExpressionTree::Value(value)),
        ParserExpressionTreeData::ColumnAccess(name) => {
            if state.allow_aggregates {
                let aggregate_index = state.next_aggregate_index;
                state.next_aggregate_index += 1;
                Ok(ExpressionTree::Aggregate(aggregate_index, Box::new(Aggregate::GroupKey(ExpressionTree::ColumnAccess(name)))))
            } else {
                Ok(ExpressionTree::ColumnAccess(name))
            }
        }
        ParserExpressionTreeData::ScopedColumnAccess(scope, name) => {
            if state.allow_aggregates {
                let aggregate_index = state.next_aggregate_index;
                state.next_aggregate_index += 1;
                Ok(ExpressionTree::Aggregate(aggregate_index, Box::new(Aggregate::GroupKey(ExpressionTree::ScopedColumnAccess(scope, name)))))
            } else {
                Ok(ExpressionTree::ScopedColumnAccess(scope, name))
            }
        }
        ParserExpressionTreeData::Wildcard => Ok(ExpressionTree::Wildcard),
        ParserExpressionTreeData::Tuple { .. } => { return Err(ConvertParserTreeErrorType::UnexpectedTuple.with_location(tree.location)); }
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
                _ => { return Err(ConvertParserTreeErrorType::UndefinedOperator(operator).with_location(tree.location)); }
            }
        }
        ParserExpressionTreeData::BooleanOperation { operator, left, right } => {
            let left = Box::new(transform_expression(*left, state)?);
            let right = Box::new(transform_expression(*right, state)?);

            Ok(ExpressionTree::BooleanOperation { operator, left, right })
        }
        ParserExpressionTreeData::UnaryOperator { operator, operand } => {
            let operand = Box::new(transform_expression(*operand, state)?);

            match operator {
                Operator::Single('-') => Ok(ExpressionTree::UnaryArithmetic { operator: UnaryArithmeticOperator::Negative, operand }),
                _ => { return Err(ConvertParserTreeErrorType::UndefinedOperator(operator).with_location(tree.location)); }
            }
        }
        ParserExpressionTreeData::Invert { operand } => {
            let operand = Box::new(transform_expression(*operand, state)?);
            Ok(ExpressionTree::UnaryArithmetic { operator: UnaryArithmeticOperator::Invert, operand })
        }
        ParserExpressionTreeData::NullableCompare { operator, left, right } => {
            let left = Box::new(transform_expression(*left, state)?);
            let right = Box::new(transform_expression(*right, state)?);

            Ok(ExpressionTree::NullableCompare { operator, left, right })
        }
        ParserExpressionTreeData::In { is_not, operand, values } => {
            let operand = Box::new(transform_expression(*operand, state)?);
            let transformed_values = values.consume_result_vec(|value| transform_expression(value, state))?;

            Ok(ExpressionTree::In { is_not, operand, values: transformed_values, })
        }
        ParserExpressionTreeData::Call { name, arguments, distinct } => {
            if state.allow_aggregates {
                match transform_call_aggregate(tree.location.clone(), &name, arguments.clone(), distinct, 0) {
                    Ok((_, aggregate, _)) => {
                        let aggregate_index = state.next_aggregate_index;
                        state.next_aggregate_index += 1;
                        return Ok(ExpressionTree::Aggregate(aggregate_index, Box::new(aggregate)));
                    }
                    Err(err) => {
                        match err.error {
                            ConvertParserTreeErrorType::UndefinedAggregate => {}
                            _ => { return Err(err); }
                        }
                    }
                }
            }

            let transformed_arguments = arguments.consume_result_vec(|argument| transform_expression(argument, state))?;
            FUNCTIONS.get(&name.to_lowercase())
                .map(|function| {
                    ExpressionTree::FunctionCall { function: function.clone(), arguments: transformed_arguments }
                })
                .ok_or(ConvertParserTreeErrorType::UndefinedFunction(name).with_location(tree.location.clone()))
        }
        ParserExpressionTreeData::ArrayElementAccess { array, index } => {
            let array = Box::new(transform_expression(*array, state)?);
            let index = Box::new(transform_expression(*index, state)?);
            Ok(ExpressionTree::ArrayElementAccess { array, index })
        }
        ParserExpressionTreeData::TypeConversion { operand, convert_to_type } => {
            let operand = Box::new(transform_expression(*operand, state)?);
            Ok(ExpressionTree::TypeConversion { operand, convert_to_type })
        }
        ParserExpressionTreeData::Case { clauses, else_clause } => {
            let transformed_clauses = clauses.consume_result_vec(|clause| tuple_result(
                transform_expression(clause.0, state),
                transform_expression(clause.1, state)
            ))?;
            let else_clause = Box::new(transform_expression(*else_clause, state)?);
            Ok(ExpressionTree::Case { clauses: transformed_clauses, else_clause })
        }
    }
}

fn transform_aggregate(mut tree: ParserExpressionTree, aggregate_index: usize) -> Result<(Option<String>, Aggregate, Option<ExpressionTree>), ConvertParserTreeError> {
    if count_aggregates_in_expression(&tree) > 1 {
        return Err(ConvertParserTreeErrorType::TooManyAggregates.with_location(tree.location));
    }

    if any_aggregates_in_expression(&tree) {
        let (aggregate, not_transformed) = extract_aggregate(&mut tree);
        match aggregate {
            Some(ParserExpressionTreeData::Call { name, arguments, distinct }) => {
                let mut result = transform_call_aggregate(tree.location.clone(), &name, arguments, distinct, aggregate_index)?;
                if !not_transformed {
                    result.2 = Some(
                        transform_expression(
                            tree,
                            &mut TransformExpressionState::default()
                        )?
                    );
                }

                Ok(result)
            }
            _ => { Err(ConvertParserTreeErrorType::UndefinedAggregate.with_location(tree.location)) }
        }
    } else {
        let tree = transform_expression(tree, &mut TransformExpressionState::default())?;
        let default_name = if let ExpressionTree::ColumnAccess(name) = &tree {
            Some(name.clone())
        } else {
            None
        };

        Ok((default_name, Aggregate::GroupKey(tree), None))
    }
}

fn extract_aggregate(tree: &mut ParserExpressionTree) -> (Option<ParserExpressionTreeData>, bool) {
    let tree_location = tree.location.clone();
    let create_agg_access = || {
        ParserExpressionTreeData::ScopedColumnAccess(ColumnScope::AggregationValue, "$value".to_owned()).with_location(tree_location.clone())
    };

    match &mut tree.tree {
        ParserExpressionTreeData::ColumnAccess(name) => (Some(ParserExpressionTreeData::ColumnAccess(name.clone())), true),
        ParserExpressionTreeData::Call { name, arguments, distinct } => {
            let name_lowercase = name.to_lowercase();
            if AGGREGATE_FUNCTIONS.contains(&name_lowercase) {
                (Some(ParserExpressionTreeData::Call { name: name.clone(), arguments: arguments.clone(), distinct: *distinct }), true)
            } else {
                let mut aggregate = None;

                for argument in arguments {
                    let (extracted_aggregate, found) = extract_aggregate(argument);
                    if extracted_aggregate.is_some() {
                        aggregate = extracted_aggregate;
                    }

                    if found {
                        std::mem::swap(&mut create_agg_access(), argument);
                    }
                }

                (aggregate, false)
            }
        }
        ParserExpressionTreeData::BinaryOperator { left, right, .. } => {
            let (left_aggregate, left_found) = extract_aggregate(left.as_mut());
            let (right_aggregate, right_found) = extract_aggregate(right.as_mut());

            if left_found {
                std::mem::swap(&mut Box::new(create_agg_access()), left);
            }

            if right_found {
                std::mem::swap(&mut Box::new(create_agg_access()), right);
            }

            if left_aggregate.is_some() {
                (left_aggregate, false)
            } else {
                (right_aggregate, false)
            }
        }
        ParserExpressionTreeData::BooleanOperation { left, right, .. } => {
            let (left_aggregate, left_found) = extract_aggregate(left.as_mut());
            let (right_aggregate, right_found) = extract_aggregate(right.as_mut());

            if left_found {
                std::mem::swap(&mut Box::new(create_agg_access()), left);
            }

            if right_found {
                std::mem::swap(&mut Box::new(create_agg_access()), right);
            }

            if left_aggregate.is_some() {
                (left_aggregate, false)
            } else {
                (right_aggregate, false)
            }
        }
        ParserExpressionTreeData::UnaryOperator { operand, .. } => {
            let (aggregate, found) = extract_aggregate(operand.as_mut());

            if found {
                std::mem::swap(&mut Box::new(create_agg_access()), operand);
            }

            (aggregate, false)
        }
        ParserExpressionTreeData::ArrayElementAccess { array, index, .. } => {
            let (array_aggregate, array_found) = extract_aggregate(array.as_mut());
            let (index_aggregate, index_found) = extract_aggregate(index.as_mut());

            if array_found {
                std::mem::swap(&mut Box::new(create_agg_access()), array);
            }

            if index_found {
                std::mem::swap(&mut Box::new(create_agg_access()), index);
            }

            if array_aggregate.is_some() {
                (array_aggregate, false)
            } else {
                (index_aggregate, false)
            }
        }
        ParserExpressionTreeData::NullableCompare { left, right, .. } => {
            let (left_aggregate, left_found) = extract_aggregate(left.as_mut());
            let (right_aggregate, right_found) = extract_aggregate(right.as_mut());

            if left_found {
                std::mem::swap(&mut Box::new(create_agg_access()), left);
            }

            if right_found {
                std::mem::swap(&mut Box::new(create_agg_access()), right);
            }

            if left_aggregate.is_some() {
                (left_aggregate, false)
            } else {
                (right_aggregate, false)
            }
        }
        ParserExpressionTreeData::Invert { operand, .. } => {
            let (aggregate, found) = extract_aggregate(operand.as_mut());

            if found {
                std::mem::swap(&mut Box::new(create_agg_access()), operand);
            }

            (aggregate, false)
        }
        ParserExpressionTreeData::TypeConversion { operand, .. } => {
            let (aggregate, found) = extract_aggregate(operand.as_mut());

            if found {
                std::mem::swap(&mut Box::new(create_agg_access()), operand);
            }

            (aggregate, false)
        }
        _ => {
            (None, false)
        }
    }
}

fn transform_call_aggregate(location: TokenLocation,
                            name: &str,
                            mut arguments: Vec<ParserExpressionTree>,
                            distinct: Option<bool>,
                            index: usize) -> Result<(Option<String>, Aggregate, Option<ExpressionTree>), ConvertParserTreeError> {
    let name_lowercase = name.to_lowercase();
    if name_lowercase == "count" {
        let distinct = distinct.unwrap_or(false);

        let default_name = Some(format!("count{}", index));
        if arguments.is_empty() {
            Ok((default_name, Aggregate::Count(None, distinct), None))
        } else if arguments.len() == 1 {
            let argument = arguments.remove(0);
            let location = argument.location.clone();
            let expression = transform_expression(argument, &mut TransformExpressionState::default())?;
            match expression {
                ExpressionTree::Wildcard => Ok((default_name, Aggregate::Count(None, distinct), None)),
                ExpressionTree::ColumnAccess(column) => Ok((default_name, Aggregate::Count(Some(column), distinct), None)),
                _ => { Err(ConvertParserTreeErrorType::ExpectedColumnAccess.with_location(location)) }
            }
        } else {
            Err(ConvertParserTreeErrorType::TooManyArguments.with_location(location))
        }
    } else if AGGREGATE_FUNCTIONS.contains(&name_lowercase) {
        if arguments.len() == 1 {
            let expression = transform_expression(arguments.remove(0), &mut TransformExpressionState::default())?;
            let aggregate = match name_lowercase.as_str() {
                "max" => Aggregate::Max(expression),
                "min" => Aggregate::Min(expression),
                "sum" => Aggregate::Sum(expression),
                "avg" => Aggregate::Average(expression),
                "stddev" => Aggregate::StandardDeviation(expression, false),
                "variance" => Aggregate::StandardDeviation(expression, true),
                "percentile" => { return Err(ConvertParserTreeErrorType::ExpectedArgument.with_location(location)); }
                "bool_and" => Aggregate::BoolAnd(expression),
                "bool_or" => Aggregate::BoolOr(expression),
                "array_agg" => Aggregate::CollectArray(expression),
                _ => { panic!("should not happen") }
            };

            Ok((Some(format!("{}{}", name_lowercase, index)), aggregate, None))
        } else if arguments.len() == 2 {
            let expression = transform_expression(arguments.remove(0), &mut TransformExpressionState::default())?;
            let argument = transform_expression(arguments.remove(0), &mut TransformExpressionState::default())?;

            let aggregate = match name_lowercase.as_str() {
                "percentile" => {
                    let percentile = if let ExpressionTree::Value(Value::Float(percentile)) = argument {
                        percentile.0
                    } else {
                        return Err(ConvertParserTreeErrorType::ExpectedFloat.with_location(location));
                    };

                    Aggregate::Percentile(expression, Float(percentile.clamp(0.0, 1.0)))
                },
                "string_agg" => {
                    let delimiter = if let ExpressionTree::Value(Value::String(delimiter)) = argument {
                        delimiter
                    } else {
                        return Err(ConvertParserTreeErrorType::ExpectedString.with_location(location));
                    };

                    Aggregate::CollectString(expression, delimiter)
                }
                _ => { return Err(ConvertParserTreeErrorType::TooManyArguments.with_location(location)); }
            };

            Ok((Some(format!("{}{}", name_lowercase, index)), aggregate, None))
        } else if arguments.is_empty() {
            Err(ConvertParserTreeErrorType::ExpectedArgument.with_location(location))
        } else {
            Err(ConvertParserTreeErrorType::TooManyArguments.with_location(location))
        }
    } else {
        Err(ConvertParserTreeErrorType::UndefinedAggregate.with_location(location))
    }
}

fn any_aggregates(projections: &Vec<(Option<String>, ParserExpressionTree)>) -> bool {
    projections.iter().any(|(_, projection)| any_aggregates_in_expression(projection))
}

fn any_aggregates_in_expression(expression: &ParserExpressionTree) -> bool {
    count_aggregates_in_expression(expression) > 0
}

fn count_aggregates_in_expression(expression: &ParserExpressionTree) -> usize {
    let mut num_aggregates = 0;
    expression.tree.visit::<(), _>(&mut |expr| {
        match expr {
            ParserExpressionTreeData::Call { name, arguments: _, distinct: _ } if AGGREGATE_FUNCTIONS.contains(&name.to_lowercase()) => {
                num_aggregates += 1;
            }
            _ => {}
        }
        Ok(())
    }).unwrap();

    num_aggregates
}