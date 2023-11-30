use crate::data_model::{ColumnParsing, RegexResultReference};
use crate::execution::ColumnScope;

use crate::model::{Aggregate, ArithmeticOperator, BooleanOperator, CompareOperator, ExpressionTree, Function, JoinClause, UnaryArithmeticOperator, Value, ValueType};

use crate::parsing::parser::{parse_str};
use crate::parsing::parser_tree_converter::{ConvertParserTreeErrorType, transform_statement};
use crate::parsing::tokenizer::TokenLocation;

#[test]
fn test_select_statement1() {
    let tree = parse_str("SELECT x FROM test").unwrap();

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
    let tree = parse_str("SELECT x AS X FROM test").unwrap();

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let parsed_statement = statement.unwrap();

    let statement = parsed_statement.extract_select();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.projections.len());
    assert_eq!("X", statement.projections[0].0.as_str());
    assert_eq!(&ExpressionTree::ColumnAccess("x".to_owned()), &statement.projections[0].1);

    assert_eq!("test", statement.from);
}

#[test]
fn test_select_statement3() {
    let tree = parse_str("SELECT x FROM test WHERE x > 10").unwrap();

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
    let tree = parse_str("SELECT x * 2 FROM test WHERE x > 10").unwrap();

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
    let tree = parse_str("SELECT x FROM test::'test.log'").unwrap();

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
    let tree = parse_str("SELECT -x FROM test").unwrap();

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_select();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.projections.len());
    assert_eq!("p0", statement.projections[0].0.as_str());
    assert_eq!(
        &ExpressionTree::UnaryArithmetic {
            operator: UnaryArithmeticOperator::Negative,
            operand: Box::new(ExpressionTree::ColumnAccess("x".to_owned()))
        },
        &statement.projections[0].1
    );

    assert_eq!("test", statement.from);
}

#[test]
fn test_select_statement7() {
    let tree = parse_str("SELECT GREATEST(x, y) FROM test").unwrap();

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
fn test_select_statement8() {
    let tree = parse_str("SELECT (CASE WHEN x > 0 THEN 1 ELSE 0 END) FROM test").unwrap();

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_select();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.projections.len());
    assert_eq!("p0", statement.projections[0].0.as_str());
    assert_eq!(
        &ExpressionTree::Case {
            clauses: vec![
                (
                    ExpressionTree::Compare {
                        operator: CompareOperator::GreaterThan,
                        left: Box::new(ExpressionTree::ColumnAccess("x".to_owned())),
                        right: Box::new(ExpressionTree::Value(Value::Int(0)))
                    },
                    ExpressionTree::Value(Value::Int(1))
                )
            ],
            else_clause: Box::new(ExpressionTree::Value(Value::Int(0)))
        },
        &statement.projections[0].1
    );

    assert_eq!("test", statement.from);
}

#[test]
fn test_select_statement_distinct1() {
    let tree = parse_str("SELECT DISTINCT x FROM test").unwrap();

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_select();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.projections.len());
    assert_eq!("x", statement.projections[0].0.as_str());
    assert_eq!(
        &ExpressionTree::ColumnAccess("x".to_owned()),
        &statement.projections[0].1
    );

    assert_eq!("test", statement.from);
    assert_eq!(true, statement.distinct);
}

#[test]
fn test_select_inner_join1() {
    let tree = parse_str("SELECT x FROM test::'test.log' INNER JOIN other::'other.log' ON test.x = other.y").unwrap();

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
    let tree = parse_str("SELECT x FROM test::'test.log' INNER JOIN other::'other.log' ON other.y = test.x").unwrap();

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
fn test_aggregate_group_by_statement1() {
    let tree = parse_str("SELECT x, MAX(x) FROM test GROUP BY x").unwrap();

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(2, statement.aggregates.len());
    assert_eq!("x", statement.aggregates[0].name);
    assert_eq!(Aggregate::GroupKey(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[0].aggregate);

    assert_eq!("max1", statement.aggregates[1].name);
    assert_eq!(Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[1].aggregate);

    assert_eq!("test", statement.from);
    assert_eq!(Some(vec![ExpressionTree::ColumnAccess("x".to_owned())]), statement.group_by);
}

#[test]
fn test_aggregate_group_by_statement2() {
    let tree = parse_str("SELECT x, SUM(x) FROM test GROUP BY x").unwrap();

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(2, statement.aggregates.len());
    assert_eq!("x", statement.aggregates[0].name);
    assert_eq!(Aggregate::GroupKey(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[0].aggregate);

    assert_eq!("sum1", statement.aggregates[1].name);
    assert_eq!(Aggregate::Sum(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[1].aggregate);

    assert_eq!("test", statement.from);
    assert_eq!(Some(vec![ExpressionTree::ColumnAccess("x".to_owned())]), statement.group_by);
}

#[test]
fn test_aggregate_group_by_statement3() {
    let tree = parse_str("SELECT x, COUNT(*) FROM test GROUP BY x").unwrap();

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(2, statement.aggregates.len());
    assert_eq!("x", statement.aggregates[0].name);
    assert_eq!(Aggregate::GroupKey(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[0].aggregate);

    assert_eq!("count1", statement.aggregates[1].name);
    assert_eq!(Aggregate::Count(None, false), statement.aggregates[1].aggregate);

    assert_eq!("test", statement.from);
    assert_eq!(Some(vec![ExpressionTree::ColumnAccess("x".to_owned())]), statement.group_by);
}

#[test]
fn test_aggregate_group_by_statement4() {
    let tree = parse_str("SELECT COUNT(*) FROM test GROUP BY x").unwrap();

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.aggregates.len());
    assert_eq!("count0", statement.aggregates[0].name);
    assert_eq!(Aggregate::Count(None, false), statement.aggregates[0].aggregate);
}

#[test]
fn test_aggregate_group_by_statement5() {
    let tree = parse_str("SELECT x, COUNT(DISTINCT x) FROM test GROUP BY x").unwrap();

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(2, statement.aggregates.len());
    assert_eq!("x", statement.aggregates[0].name);
    assert_eq!(Aggregate::GroupKey(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[0].aggregate);

    assert_eq!("count1", statement.aggregates[1].name);
    assert_eq!(Aggregate::Count(Some("x".to_owned()), true), statement.aggregates[1].aggregate);

    assert_eq!("test", statement.from);
    assert_eq!(Some(vec![ExpressionTree::ColumnAccess("x".to_owned())]), statement.group_by);
}

#[test]
fn test_aggregate_group_by_statement6() {
    let tree = parse_str("SELECT (x * 2) AS x, MAX(x) FROM test GROUP BY x * 2").unwrap();

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(2, statement.aggregates.len());
    assert_eq!("x", statement.aggregates[0].name);
    assert_eq!(
        Aggregate::GroupKey(
            ExpressionTree::Arithmetic {
                operator: ArithmeticOperator::Multiply,
                left: Box::new(ExpressionTree::ColumnAccess("x".to_owned())),
                right: Box::new(ExpressionTree::Value(Value::Int(2))),
            }
        ),
        statement.aggregates[0].aggregate
    );

    assert_eq!("max1", statement.aggregates[1].name);
    assert_eq!(Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[1].aggregate);

    assert_eq!("test", statement.from);
    assert_eq!(
        Some(
            vec![
                ExpressionTree::Arithmetic {
                    operator: ArithmeticOperator::Multiply,
                    left: Box::new(ExpressionTree::ColumnAccess("x".to_owned())),
                    right: Box::new(ExpressionTree::Value(Value::Int(2))),
                }
            ]
        ),
        statement.group_by
    );
}

#[test]
fn test_aggregate_statement1() {
    let tree = parse_str("SELECT MAX(x) FROM test").unwrap();

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.aggregates.len());
    assert_eq!("max0", statement.aggregates[0].name);
    assert_eq!(Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[0].aggregate);
}

#[test]
fn test_aggregate_statement2() {
    let tree = parse_str("SELECT MAX(x) FROM test HAVING x = 1337 AND MAX(x) > 2000").unwrap();

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.aggregates.len());
    assert_eq!("max0", statement.aggregates[0].name);
    assert_eq!(Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[0].aggregate);
    assert_eq!(
        Some(
            ExpressionTree::BooleanOperation {
                operator: BooleanOperator::And,
                left: Box::new(ExpressionTree::Compare {
                    operator: CompareOperator::Equal,
                    left: Box::new(ExpressionTree::Aggregate(0, Box::new(Aggregate::GroupKey(ExpressionTree::ColumnAccess("x".to_owned()))))),
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
fn test_aggregate_statement3() {
    let tree = parse_str("SELECT STDDEV(x) FROM test").unwrap();

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.aggregates.len());
    assert_eq!("stddev0", statement.aggregates[0].name);
    assert_eq!(Aggregate::StandardDeviation(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[0].aggregate);
}

#[test]
fn test_transform_aggregate_statement1() {
    let tree = parse_str("SELECT MAX(x) * 2 FROM test").unwrap();

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.aggregates.len());
    assert_eq!("max0", statement.aggregates[0].name);
    assert_eq!(Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[0].aggregate);
    assert_eq!(
        Some(ExpressionTree::Arithmetic {
            operator: ArithmeticOperator::Multiply,
            left: Box::new(ExpressionTree::ScopedColumnAccess(ColumnScope::AggregationValue, "$value".to_owned())),
            right: Box::new(ExpressionTree::Value(Value::Int(2)))
        }),
        statement.aggregates[0].transform
    );
}

#[test]
fn test_transform_aggregate_statement2() {
    let tree = parse_str("SELECT 2 * MAX(x) FROM test").unwrap();

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.aggregates.len());
    assert_eq!("max0", statement.aggregates[0].name);
    assert_eq!(Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[0].aggregate);
    assert_eq!(
        Some(ExpressionTree::Arithmetic {
            operator: ArithmeticOperator::Multiply,
            left: Box::new(ExpressionTree::Value(Value::Int(2))),
            right: Box::new(ExpressionTree::ScopedColumnAccess(ColumnScope::AggregationValue, "$value".to_owned())),
        }),
        statement.aggregates[0].transform
    );
}

#[test]
fn test_transform_aggregate_statement3() {
    let tree = parse_str("SELECT SQRT(MAX(x)) FROM test").unwrap();

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.aggregates.len());
    assert_eq!("max0", statement.aggregates[0].name);
    assert_eq!(Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[0].aggregate);
    assert_eq!(
        Some(ExpressionTree::Function {
            function: Function::Sqrt,
            arguments: vec![ExpressionTree::ScopedColumnAccess(ColumnScope::AggregationValue, "$value".to_owned())]
        }),
        statement.aggregates[0].transform
    );
}

#[test]
fn test_transform_aggregate_statement4() {
    let tree = parse_str("SELECT GREATEST(MAX(x), MAX(x)) FROM test").unwrap();

    let statement = transform_statement(tree);
    assert_eq!(
        Some(ConvertParserTreeErrorType::TooManyAggregates.with_location(TokenLocation::new(0, 15))),
        statement.err()
    );
}

#[test]
fn test_transform_aggregate_statement5() {
    let tree = parse_str("SELECT MAX(x)::real FROM test").unwrap();

    let statement = transform_statement(tree);
    assert!(statement.is_ok());
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(1, statement.aggregates.len());
    assert_eq!("max0", statement.aggregates[0].name);
    assert_eq!(Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())), statement.aggregates[0].aggregate);
    assert_eq!(
        Some(ExpressionTree::TypeConversion {
            operand: Box::new(ExpressionTree::ScopedColumnAccess(ColumnScope::AggregationValue, "$value".to_owned())),
            convert_to_type: ValueType::Float,
        }),
        statement.aggregates[0].transform
    );
}

#[test]
fn test_create_table_statement1() {
    let tree = parse_str("CREATE TABLE test(line = 'A: ([0-9]+)', line[1] => x INT);").unwrap();

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