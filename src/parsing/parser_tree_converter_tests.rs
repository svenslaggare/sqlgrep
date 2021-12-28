use crate::data_model::{ColumnParsing, RegexMode, RegexResultReference};
use crate::model::{Aggregate, ArithmeticOperator, BooleanOperator, CompareOperator, ExpressionTree, Function, JoinClause, UnaryArithmeticOperator, Value, ValueType};
use crate::parsing::operator::Operator;
use crate::parsing::parser::{ParserColumnDefinition, ParserExpressionTreeData, ParserJoinClause, ParserOperationTree};
use crate::parsing::parser_tree_converter::{ConvertParserTreeErrorType, transform_statement};

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
                    ],
                    distinct: None
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
                    arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())],
                    distinct: None
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
                    arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())],
                    distinct: None
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
            (None, ParserExpressionTreeData::Call { name: "COUNT".to_owned(), arguments: vec![], distinct: None }.with_location(Default::default()) ),
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
    assert_eq!(Aggregate::Count(None, false), statement.aggregates[1].1);

    assert_eq!("test", statement.from);
    assert_eq!(Some(vec!["x".to_owned()]), statement.group_by);
}

#[test]
fn test_aggregate_statement4() {
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![
            (None, ParserExpressionTreeData::Call { name: "COUNT".to_owned(), arguments: vec![], distinct: None }.with_location(Default::default()) ),
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
    assert_eq!(Aggregate::Count(None, false), statement.aggregates[0].1);
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
                    arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())],
                    distinct: None
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
                    arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())],
                    distinct: None
                }.with_location(Default::default())
            ),
        ],
        from: ("test".to_string(), None),
        filter: None,
        group_by: None,
        having: Some(
            ParserExpressionTreeData::BooleanOperation {
                operator: BooleanOperator::And,
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
                            arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())],
                            distinct: None
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
            ExpressionTree::BooleanOperation {
                operator: BooleanOperator::And,
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
                            arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())],
                            distinct: None
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
                        arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())],
                        distinct: None
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
                            arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())],
                            distinct: None
                        }.with_location(Default::default())
                    ],
                    distinct: None
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
fn test_aggregate_statement10() {
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![
            (
                None,
                ParserExpressionTreeData::Call {
                    name: "GREATEST".to_owned(),
                    arguments: vec![
                        ParserExpressionTreeData::Call {
                            name: "MAX".to_owned(),
                            arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())],
                            distinct: None
                        }.with_location(Default::default()),
                        ParserExpressionTreeData::Call {
                            name: "MAX".to_owned(),
                            arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())],
                            distinct: None
                        }.with_location(Default::default())
                    ],
                    distinct: None
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
    assert_eq!(Some(ConvertParserTreeErrorType::TooManyAggregates.with_location(Default::default())), statement.err());
}

#[test]
fn test_aggregate_statement11() {
    let tree = ParserOperationTree::Select {
        location: Default::default(),
        projections: vec![
            (None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())),
            (
                None,
                ParserExpressionTreeData::Call {
                    name: "COUNT".to_owned(),
                    arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())],
                    distinct: Some(true)
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
    let statement = statement.unwrap();

    let statement = statement.extract_aggregate();
    assert!(statement.is_some());
    let statement = statement.unwrap();

    assert_eq!(2, statement.aggregates.len());
    assert_eq!("x", statement.aggregates[0].0);
    assert_eq!(Aggregate::GroupKey("x".to_owned()), statement.aggregates[0].1);

    assert_eq!("count1", statement.aggregates[1].0);
    assert_eq!(Aggregate::Count(Some("x".to_owned()), true), statement.aggregates[1].1);

    assert_eq!("test", statement.from);
    assert_eq!(Some(vec!["x".to_owned()]), statement.group_by);
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