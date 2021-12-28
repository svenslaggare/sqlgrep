use crate::data_model::{ColumnParsing, JsonAccess, RegexMode, RegexResultReference};
use crate::model::{BooleanOperator, NullableCompareOperator, Value, ValueType};
use crate::parsing::operator::{BinaryOperators, Operator, UnaryOperators};
use crate::parsing::parser::{parse_str, Parser, ParserColumnDefinition, ParserExpressionTreeData, ParserJoinClause, ParserOperationTree};
use crate::parsing::tokenizer::{Keyword, ParserError, ParserErrorType, Token, TokenLocation};

#[test]
fn test_advance_parser() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Identifier("a".to_string()),
            Token::Operator(Operator::Single('+')),
            Token::Int(4)
        ]
    );

    assert_eq!(&Token::Identifier("a".to_string()), parser.next().unwrap());
    assert_eq!(&Token::Operator(Operator::Single('+')), parser.next().unwrap());
    assert_eq!(&Token::Int(4), parser.next().unwrap());
    assert_eq!(Err(ParserErrorType::ReachedEndOfTokens), parser.next().map_err(|err| err.error));
    assert_eq!(Err(ParserErrorType::ReachedEndOfTokens), parser.next().map_err(|err| err.error));
}

#[test]
fn test_parse_values() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let tree = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Int(4711),
            Token::End
        ]
    ).parse_expression().unwrap().tree;

    assert_eq!(
        ParserExpressionTreeData::Value(Value::Int(4711)),
        tree
    );

    let tree = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Null,
            Token::End
        ]
    ).parse_expression().unwrap().tree;

    assert_eq!(
        ParserExpressionTreeData::Value(Value::Null),
        tree
    );

    let tree = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::True,
            Token::End
        ]
    ).parse_expression().unwrap().tree;

    assert_eq!(
        ParserExpressionTreeData::Value(Value::Bool(true)),
        tree
    );

    let tree = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::False,
            Token::End
        ]
    ).parse_expression().unwrap().tree;

    assert_eq!(
        ParserExpressionTreeData::Value(Value::Bool(false)),
        tree
    );

    let tree = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::String("hello world!".to_owned()),
            Token::End
        ]
    ).parse_expression().unwrap().tree;

    assert_eq!(
        ParserExpressionTreeData::Value(Value::String("hello world!".to_owned())),
        tree
    );
}

#[test]
fn test_parse_expression1() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Identifier("a".to_string()),
            Token::Operator(Operator::Single('+')),
            Token::Int(4),
            Token::End
        ]
    );

    let tree = parser.parse_expression().unwrap();
    assert_eq!(
        ParserExpressionTreeData::BinaryOperator {
            operator: Operator::Single('+'),
            left: Box::new(ParserExpressionTreeData::ColumnAccess("a".to_string()).with_location(Default::default())),
            right: Box::new(ParserExpressionTreeData::Value(Value::Int(4)).with_location(Default::default()))
        },
        tree.tree
    );
}

#[test]
fn test_parse_expression2() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::LeftParentheses,
            Token::Identifier("a".to_string()),
            Token::Operator(Operator::Single('+')),
            Token::Int(4),
            Token::RightParentheses,
            Token::Operator(Operator::Single('*')),
            Token::Identifier("b".to_string()),
            Token::End
        ]
    );

    let tree = parser.parse_expression().unwrap();
    assert_eq!(
        ParserExpressionTreeData::BinaryOperator {
            operator: Operator::Single('*'),
            left: Box::new(
                ParserExpressionTreeData::BinaryOperator {
                    operator: Operator::Single('+'),
                    left: Box::new(ParserExpressionTreeData::ColumnAccess("a".to_string()).with_location(Default::default())),
                    right: Box::new(ParserExpressionTreeData::Value(Value::Int(4)).with_location(Default::default()))
                }.with_location(Default::default())
            ),
            right: Box::new(ParserExpressionTreeData::ColumnAccess("b".to_string()).with_location(Default::default()))
        },
        tree.tree
    );
}

#[test]
fn test_parse_expression3() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Operator(Operator::Single('-')),
            Token::Int(4),
            Token::End
        ]
    );

    let tree = parser.parse_expression().unwrap();
    assert_eq!(
        ParserExpressionTreeData::UnaryOperator {
            operator: Operator::Single('-'),
            operand: Box::new(ParserExpressionTreeData::Value(Value::Int(4)).with_location(Default::default()))
        },
        tree.tree
    );
}

#[test]
fn test_parse_expression4() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Identifier("f".to_string()),
            Token::LeftParentheses,
            Token::Int(4),
            Token::Comma,
            Token::Identifier("a".to_string()),
            Token::RightParentheses,
            Token::End
        ]
    );

    let tree = parser.parse_expression().unwrap();
    assert_eq!(
        ParserExpressionTreeData::Call {
            name: "f".to_string(),
            arguments: vec![
                ParserExpressionTreeData::Value(Value::Int(4)).with_location(Default::default()),
                ParserExpressionTreeData::ColumnAccess("a".to_string()).with_location(Default::default()),
            ],
            distinct: None
        },
        tree.tree
    );
}

#[test]
fn test_parse_expression5() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::True,
            Token::Keyword(Keyword::And),
            Token::False,
            Token::End
        ]
    );

    let tree = parser.parse_expression().unwrap();
    assert_eq!(
        ParserExpressionTreeData::BooleanOperation {
            operator: BooleanOperator::And,
            left: Box::new(ParserExpressionTreeData::Value(Value::Bool(true)).with_location(Default::default())),
            right: Box::new(ParserExpressionTreeData::Value(Value::Bool(false)).with_location(Default::default()))
        },
        tree.tree
    );
}

#[test]
fn test_parse_expression6() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Operator(Operator::Single('-')),
            Token::Int(4),
            Token::End
        ]
    );

    let tree = parser.parse_expression().unwrap();
    assert_eq!(
        ParserExpressionTreeData::UnaryOperator {
            operator: Operator::Single('-'),
            operand: Box::new(ParserExpressionTreeData::Value(Value::Int(4)).with_location(Default::default()))
        },
        tree.tree
    );
}

#[test]
fn test_parse_expression7() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Not),
            Token::True,
            Token::End
        ]
    );

    let tree = parser.parse_expression().unwrap();
    assert_eq!(
        ParserExpressionTreeData::Invert {
            operand: Box::new(ParserExpressionTreeData::Value(Value::Bool(true)).with_location(Default::default()))
        },
        tree.tree
    );
}

#[test]
fn test_parse_expression8() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::True,
            Token::Keyword(Keyword::Is),
            Token::Null,
            Token::End
        ]
    );

    let tree = parser.parse_expression().unwrap();
    assert_eq!(
        ParserExpressionTreeData::NullableCompare {
            operator: NullableCompareOperator::Equal,
            left: Box::new(ParserExpressionTreeData::Value(Value::Bool(true)).with_location(Default::default())),
            right: Box::new(ParserExpressionTreeData::Value(Value::Null).with_location(Default::default()))
        },
        tree.tree
    );

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::True,
            Token::Keyword(Keyword::IsNot),
            Token::Null,
            Token::End
        ]
    );

    let tree = parser.parse_expression().unwrap();
    assert_eq!(
        ParserExpressionTreeData::NullableCompare {
            operator: NullableCompareOperator::NotEqual,
            left: Box::new(ParserExpressionTreeData::Value(Value::Bool(true)).with_location(Default::default())),
            right: Box::new(ParserExpressionTreeData::Value(Value::Null).with_location(Default::default()))
        },
        tree.tree
    );
}

#[test]
fn test_parse_expression9() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Identifier("a".to_string()),
            Token::Operator(Operator::Single('.')),
            Token::Identifier("b".to_string()),
            Token::End
        ]
    );

    let tree = parser.parse_expression().unwrap();
    assert_eq!(
        ParserExpressionTreeData::ColumnAccess("a.b".to_owned()),
        tree.tree
    );
}

#[test]
fn test_parse_expression10() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Identifier("a".to_string()),
            Token::LeftSquareParentheses,
            Token::Int(11),
            Token::RightSquareParentheses,
            Token::End
        ]
    );

    let tree = parser.parse_expression().unwrap();
    assert_eq!(
        ParserExpressionTreeData::ArrayElementAccess {
            array: Box::new(ParserExpressionTreeData::ColumnAccess("a".to_owned()).with_location(Default::default())),
            index: Box::new(ParserExpressionTreeData::Value(Value::Int(11)).with_location(Default::default()))
        },
        tree.tree
    );
}

#[test]
fn test_parse_expression11() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Identifier("a".to_string()),
            Token::LeftSquareParentheses,
            Token::Int(11),
            Token::Operator(Operator::Single('+')),
            Token::Identifier("a".to_string()),
            Token::RightSquareParentheses,
            Token::End
        ]
    );

    let tree = parser.parse_expression().unwrap();
    assert_eq!(
        ParserExpressionTreeData::ArrayElementAccess {
            array: Box::new(ParserExpressionTreeData::ColumnAccess("a".to_owned()).with_location(Default::default())),
            index: Box::new(ParserExpressionTreeData::BinaryOperator {
                operator: Operator::Single('+'),
                left: Box::new(ParserExpressionTreeData::Value(Value::Int(11)).with_location(Default::default())),
                right: Box::new(ParserExpressionTreeData::ColumnAccess("a".to_owned()).with_location(Default::default()))
            }.with_location(Default::default()))
        },
        tree.tree
    );
}

#[test]
fn test_parse_expression12() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Extract),
            Token::LeftParentheses,
            Token::Identifier("EPOCH".to_string()),
            Token::Keyword(Keyword::From),
            Token::Identifier("timestamp".to_string()),
            Token::RightParentheses,
            Token::End
        ]
    );

    let tree = parser.parse_expression().unwrap();
    assert_eq!(
        ParserExpressionTreeData::Call {
            name: "timestamp_extract_epoch".to_string(),
            arguments: vec![ParserExpressionTreeData::ColumnAccess("timestamp".to_owned()).with_location(Default::default())],
            distinct: None
        },
        tree.tree
    );
}

#[test]
fn test_parse_expression13() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Identifier("COUNT".to_owned()),
            Token::LeftParentheses,
            Token::Keyword(Keyword::Distinct),
            Token::Identifier("timestamp".to_string()),
            Token::RightParentheses,
            Token::End
        ]
    );

    let tree = parser.parse_expression().unwrap();
    assert_eq!(
        ParserExpressionTreeData::Call {
            name: "COUNT".to_string(),
            arguments: vec![ParserExpressionTreeData::ColumnAccess("timestamp".to_owned()).with_location(Default::default())],
            distinct: Some(true)
        },
        tree.tree
    );
}

#[test]
fn test_parse_expression14() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();


    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Identifier("array".to_string()),
            Token::LeftSquareParentheses,
            Token::Int(1337),
            Token::Comma,
            Token::Int(4711),
            Token::RightSquareParentheses,
            Token::End
        ]
    );

    let tree = parser.parse_expression().unwrap();
    assert_eq!(
        ParserExpressionTreeData::Call {
            name: "create_array".to_string(),
            arguments: vec![
                ParserExpressionTreeData::Value(Value::Int(1337)).with_location(Default::default()),
                ParserExpressionTreeData::Value(Value::Int(4711)).with_location(Default::default())
            ],
            distinct: None
        },
        tree.tree
    );
}

#[test]
fn test_parse_select1() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier("x".to_string()),
            Token::Keyword(Keyword::From),
            Token::Identifier("test".to_string()),
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::Select {
            location: Default::default(),
            projections: vec![(None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default()))],
            from: ("test".to_string(), None),
            filter: None,
            group_by: None,
            having: None,
            join: None
        },
        tree
    );
}

#[test]
fn test_parse_select2() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Operator(Operator::Single('*')),
            Token::Keyword(Keyword::From),
            Token::Identifier("test".to_string()),
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::Select {
            location: Default::default(),
            projections: vec![(None, ParserExpressionTreeData::Wildcard.with_location(Default::default()))],
            from: ("test".to_string(), None),
            filter: None,
            group_by: None,
            having: None,
            join: None
        },
        tree
    );
}

#[test]
fn test_parse_select_and_filter1() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier("x".to_string()),
            Token::Keyword(Keyword::From),
            Token::Identifier("test".to_string()),
            Token::Keyword(Keyword::Where),
            Token::Identifier("x".to_string()),
            Token::Operator(Operator::Single('>')),
            Token::Int(4),
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::Select {
            location: Default::default(),
            projections: vec![(None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default()))],
            from: ("test".to_string(), None),
            filter: Some(
                ParserExpressionTreeData::BinaryOperator {
                    operator: Operator::Single('>'),
                    left: Box::new(ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())),
                    right: Box::new(ParserExpressionTreeData::Value(Value::Int(4)).with_location(Default::default()))
                }.with_location(Default::default())
            ),
            group_by: None,
            having: None,
            join: None,
        },
        tree
    );
}

#[test]
fn test_parse_select_and_filter2() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier("x".to_string()),
            Token::Keyword(Keyword::As),
            Token::Identifier("xxx".to_string()),
            Token::Keyword(Keyword::From),
            Token::Identifier("test".to_string()),
            Token::Keyword(Keyword::Where),
            Token::Identifier("x".to_string()),
            Token::Operator(Operator::Single('>')),
            Token::Int(4),
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::Select {
            location: Default::default(),
            projections: vec![
                (
                    Some("xxx".to_owned()),
                    ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())
                )
            ],
            from: ("test".to_string(), None),
            filter: Some(
                ParserExpressionTreeData::BinaryOperator {
                    operator: Operator::Single('>'),
                    left: Box::new(ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())),
                    right: Box::new(ParserExpressionTreeData::Value(Value::Int(4)).with_location(Default::default()))
                }.with_location(Default::default())
            ),
            group_by: None,
            having: None,
            join: None
        },
        tree
    );
}

#[test]
fn test_parse_select_and_filter3() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier("MAX".to_owned()),
            Token::LeftParentheses,
            Token::Identifier("x".to_string()),
            Token::RightParentheses,
            Token::Keyword(Keyword::From),
            Token::Identifier("test".to_string()),
            Token::Keyword(Keyword::Where),
            Token::Identifier("x".to_string()),
            Token::Operator(Operator::Single('>')),
            Token::Int(4),
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::Select {
            location: Default::default(),
            projections: vec![(
                None,
                ParserExpressionTreeData::Call {
                    name: "MAX".to_owned(),
                    arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())],
                    distinct: None
                }.with_location(Default::default())
            )],
            from: ("test".to_string(), None),
            filter: Some(
                ParserExpressionTreeData::BinaryOperator {
                    operator: Operator::Single('>'),
                    left: Box::new(ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())),
                    right: Box::new(ParserExpressionTreeData::Value(Value::Int(4)).with_location(Default::default()))
                }.with_location(Default::default())
            ),
            group_by: None,
            having: None,
            join: None
        },
        tree
    );
}

#[test]
fn test_parse_select_and_filter4() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier("MAX".to_owned()),
            Token::LeftParentheses,
            Token::Identifier("x".to_string()),
            Token::RightParentheses,
            Token::Operator(Operator::Single('*')),
            Token::Int(2),
            Token::Keyword(Keyword::From),
            Token::Identifier("test".to_string()),
            Token::Keyword(Keyword::Where),
            Token::Identifier("x".to_string()),
            Token::Operator(Operator::Single('>')),
            Token::Int(4),
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::Select {
            location: Default::default(),
            projections: vec![
                (
                    None,
                    ParserExpressionTreeData::BinaryOperator {
                        operator: Operator::Single('*'),
                        left: Box::new(ParserExpressionTreeData::Call {
                            name: "MAX".to_owned(),
                            arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())],
                            distinct: None
                        }.with_location(Default::default())),
                        right: Box::new(ParserExpressionTreeData::Value(Value::Int(2)).with_location(Default::default()))
                    }.with_location(Default::default())
                )
            ],
            from: ("test".to_string(), None),
            filter: Some(
                ParserExpressionTreeData::BinaryOperator {
                    operator: Operator::Single('>'),
                    left: Box::new(ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())),
                    right: Box::new(ParserExpressionTreeData::Value(Value::Int(4)).with_location(Default::default()))
                }.with_location(Default::default())
            ),
            group_by: None,
            having: None,
            join: None
        },
        tree
    );
}

#[test]
fn test_parse_with_filename() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier("x".to_string()),
            Token::Keyword(Keyword::From),
            Token::Identifier("test".to_string()),
            Token::Colon,
            Token::Colon,
            Token::String("test.log".to_string()),
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::Select {
            location: Default::default(),
            projections: vec![(None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default()))],
            from: ("test".to_string(), Some("test.log".to_owned())),
            filter: None,
            group_by: None,
            having: None,
            join: None
        },
        tree
    );
}

#[test]
fn test_parse_select_group_by1() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier("x".to_string()),
            Token::Keyword(Keyword::From),
            Token::Identifier("test".to_string()),
            Token::Keyword(Keyword::Where),
            Token::Identifier("x".to_string()),
            Token::Operator(Operator::Single('>')),
            Token::Int(4),
            Token::Keyword(Keyword::Group),
            Token::Keyword(Keyword::By),
            Token::Identifier("x".to_string()),
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::Select {
            location: Default::default(),
            projections: vec![(None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default()))],
            from: ("test".to_string(), None),
            filter: Some(
                ParserExpressionTreeData::BinaryOperator {
                    operator: Operator::Single('>'),
                    left: Box::new(ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())),
                    right: Box::new(ParserExpressionTreeData::Value(Value::Int(4)).with_location(Default::default()))
                }.with_location(Default::default())
            ),
            group_by: Some(vec!["x".to_owned()]),
            having: None,
            join: None
        },
        tree
    );
}

#[test]
fn test_parse_select_group_by2() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier("x".to_string()),
            Token::Keyword(Keyword::From),
            Token::Identifier("test".to_string()),
            Token::Keyword(Keyword::Where),
            Token::Identifier("x".to_string()),
            Token::Operator(Operator::Single('>')),
            Token::Int(4),
            Token::Keyword(Keyword::Group),
            Token::Keyword(Keyword::By),
            Token::Identifier("x".to_string()),
            Token::Comma,
            Token::Identifier("y".to_string()),
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::Select {
            location: Default::default(),
            projections: vec![(None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default()))],
            from: ("test".to_string(), None),
            filter: Some(
                ParserExpressionTreeData::BinaryOperator {
                    operator: Operator::Single('>'),
                    left: Box::new(ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())),
                    right: Box::new(ParserExpressionTreeData::Value(Value::Int(4)).with_location(Default::default()))
                }.with_location(Default::default())
            ),
            group_by: Some(vec!["x".to_owned(), "y".to_owned()]),
            having: None,
            join: None
        },
        tree
    );
}

#[test]
fn test_parse_select_group_by3() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier("x".to_string()),
            Token::Keyword(Keyword::From),
            Token::Identifier("test".to_string()),
            Token::Keyword(Keyword::Where),
            Token::Identifier("x".to_string()),
            Token::Operator(Operator::Single('>')),
            Token::Int(4),
            Token::Keyword(Keyword::Group),
            Token::Keyword(Keyword::By),
            Token::Identifier("x".to_string()),
            Token::Keyword(Keyword::Group),
            Token::Keyword(Keyword::By),
            Token::Identifier("x".to_string()),
            Token::End
        ]
    );

    let tree = parser.parse();
    assert_eq!(
        Err(ParserError::new(TokenLocation::new(0, 0), ParserErrorType::AlreadyHaveGroupBy)),
        tree,
    );
}

#[test]
fn test_parse_select_having() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier("x".to_string()),
            Token::Keyword(Keyword::From),
            Token::Identifier("test".to_string()),
            Token::Keyword(Keyword::Where),
            Token::Identifier("x".to_string()),
            Token::Operator(Operator::Single('>')),
            Token::Int(4),
            Token::Keyword(Keyword::Having),
            Token::Identifier("y".to_string()),
            Token::Operator(Operator::Single('<')),
            Token::Int(4),
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::Select {
            location: Default::default(),
            projections: vec![(None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default()))],
            from: ("test".to_string(), None),
            filter: Some(
                ParserExpressionTreeData::BinaryOperator {
                    operator: Operator::Single('>'),
                    left: Box::new(ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())),
                    right: Box::new(ParserExpressionTreeData::Value(Value::Int(4)).with_location(Default::default()))
                }.with_location(Default::default())
            ),
            group_by: None,
            having: Some(
                ParserExpressionTreeData::BinaryOperator {
                    operator: Operator::Single('<'),
                    left: Box::new(ParserExpressionTreeData::ColumnAccess("y".to_owned()).with_location(Default::default())),
                    right: Box::new(ParserExpressionTreeData::Value(Value::Int(4)).with_location(Default::default()))
                }.with_location(Default::default())
            ),
            join: None,
        },
        tree
    );
}

#[test]
fn test_parse_inner_join1() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier("x".to_string()),
            Token::Keyword(Keyword::From),
            Token::Identifier("test".to_string()),
            Token::Keyword(Keyword::Where),
            Token::Identifier("x".to_string()),
            Token::Operator(Operator::Single('>')),
            Token::Int(4),
            Token::Keyword(Keyword::Inner),
            Token::Keyword(Keyword::Join),
            Token::Identifier("table1".to_string()),
            Token::Colon,
            Token::Colon,
            Token::String("file.log".to_string()),
            Token::Keyword(Keyword::On),
            Token::Identifier("table2".to_string()),
            Token::Operator(Operator::Single('.')),
            Token::Identifier("x".to_string()),
            Token::Operator(Operator::Single('=')),
            Token::Identifier("table1".to_string()),
            Token::Operator(Operator::Single('.')),
            Token::Identifier("y".to_string()),
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::Select {
            location: Default::default(),
            projections: vec![(None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default()))],
            from: ("test".to_string(), None),
            filter: Some(
                ParserExpressionTreeData::BinaryOperator {
                    operator: Operator::Single('>'),
                    left: Box::new(ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())),
                    right: Box::new(ParserExpressionTreeData::Value(Value::Int(4)).with_location(Default::default()))
                }.with_location(Default::default())
            ),
            group_by: None,
            having: None,
            join: Some(
                ParserJoinClause {
                    joiner_table: "table1".to_string(),
                    joiner_filename: "file.log".to_string(),
                    left_table: "table2".to_string(),
                    left_column: "x".to_string(),
                    right_table: "table1".to_string(),
                    right_column: "y".to_string(),
                    is_outer: false
                }
            ),
        },
        tree
    );
}

#[test]
fn test_parse_outer_join1() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier("x".to_string()),
            Token::Keyword(Keyword::From),
            Token::Identifier("test".to_string()),
            Token::Keyword(Keyword::Where),
            Token::Identifier("x".to_string()),
            Token::Operator(Operator::Single('>')),
            Token::Int(4),
            Token::Keyword(Keyword::Outer),
            Token::Keyword(Keyword::Join),
            Token::Identifier("table1".to_string()),
            Token::Colon,
            Token::Colon,
            Token::String("file.log".to_string()),
            Token::Keyword(Keyword::On),
            Token::Identifier("table2".to_string()),
            Token::Operator(Operator::Single('.')),
            Token::Identifier("x".to_string()),
            Token::Operator(Operator::Single('=')),
            Token::Identifier("table1".to_string()),
            Token::Operator(Operator::Single('.')),
            Token::Identifier("y".to_string()),
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::Select {
            location: Default::default(),
            projections: vec![(None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default()))],
            from: ("test".to_string(), None),
            filter: Some(
                ParserExpressionTreeData::BinaryOperator {
                    operator: Operator::Single('>'),
                    left: Box::new(ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(Default::default())),
                    right: Box::new(ParserExpressionTreeData::Value(Value::Int(4)).with_location(Default::default()))
                }.with_location(Default::default())
            ),
            group_by: None,
            having: None,
            join: Some(
                ParserJoinClause {
                    joiner_table: "table1".to_string(),
                    joiner_filename: "file.log".to_string(),
                    left_table: "table2".to_string(),
                    left_column: "x".to_string(),
                    right_table: "table1".to_string(),
                    right_column: "y".to_string(),
                    is_outer: true
                }
            ),
        },
        tree
    );
}

#[test]
fn test_parse_create_table1() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Create),
            Token::Keyword(Keyword::Table),
            Token::Identifier("test".to_string()),
            Token::LeftParentheses,

            Token::Identifier("line".to_string()),
            Token::Operator(Operator::Single('=')),
            Token::String("A: ([0-9]+)".to_owned()),
            Token::Comma,

            Token::Identifier("line".to_string()),
            Token::LeftSquareParentheses,
            Token::Int(1),
            Token::RightSquareParentheses,
            Token::RightArrow,
            Token::Identifier("x".to_owned()),
            Token::Identifier("INT".to_owned()),

            Token::RightParentheses,
            Token::SemiColon,
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: Default::default(),
            name: "test".to_string(),
            patterns: vec![("line".to_owned(), "A: ([0-9]+)".to_owned(), RegexMode::Captures)],
            columns: vec![ParserColumnDefinition::new(
                "line".to_string(),
                1,
                "x".to_string(),
                ValueType::Int
            )]
        },
        tree
    );
}

#[test]
fn test_parse_create_table2() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Create),
            Token::Keyword(Keyword::Table),
            Token::Identifier("test".to_string()),
            Token::LeftParentheses,

            Token::Identifier("line".to_string()),
            Token::Operator(Operator::Single('=')),
            Token::String("A: ([0-9]+), B: ([A-Z]+)".to_owned()),
            Token::Comma,

            Token::Identifier("line".to_string()),
            Token::LeftSquareParentheses,
            Token::Int(1),
            Token::RightSquareParentheses,
            Token::RightArrow,
            Token::Identifier("x".to_owned()),
            Token::Identifier("INT".to_owned()),
            Token::Comma,

            Token::Identifier("line".to_string()),
            Token::LeftSquareParentheses,
            Token::Int(2),
            Token::RightSquareParentheses,
            Token::RightArrow,
            Token::Identifier("y".to_owned()),
            Token::Identifier("TEXT".to_owned()),

            Token::RightParentheses,
            Token::SemiColon,
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: Default::default(),
            name: "test".to_string(),
            patterns: vec![("line".to_owned(), "A: ([0-9]+), B: ([A-Z]+)".to_owned(), RegexMode::Captures)],
            columns: vec![
                ParserColumnDefinition::new(
                    "line".to_string(),
                    1,
                    "x".to_string(),
                    ValueType::Int
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    2,
                    "y".to_string(),
                    ValueType::String
                )
            ]
        },
        tree
    );
}

#[test]
fn test_parse_create_table3() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Create),
            Token::Keyword(Keyword::Table),
            Token::Identifier("test1".to_string()),
            Token::LeftParentheses,

            Token::Identifier("line".to_string()),
            Token::Operator(Operator::Single('=')),
            Token::String("A: ([0-9]+)".to_owned()),
            Token::Comma,

            Token::Identifier("line".to_string()),
            Token::LeftSquareParentheses,
            Token::Int(1),
            Token::RightSquareParentheses,
            Token::RightArrow,
            Token::Identifier("x".to_owned()),
            Token::Identifier("INT".to_owned()),

            Token::RightParentheses,
            Token::SemiColon,

            Token::Keyword(Keyword::Create),
            Token::Keyword(Keyword::Table),
            Token::Identifier("test2".to_string()),
            Token::LeftParentheses,

            Token::Identifier("line".to_string()),
            Token::Operator(Operator::Single('=')),
            Token::String("A: ([0-9]+), B: ([A-Z]+)".to_owned()),
            Token::Comma,

            Token::Identifier("line".to_string()),
            Token::LeftSquareParentheses,
            Token::Int(1),
            Token::RightSquareParentheses,
            Token::RightArrow,
            Token::Identifier("x".to_owned()),
            Token::Identifier("INT".to_owned()),
            Token::Comma,

            Token::Identifier("line".to_string()),
            Token::LeftSquareParentheses,
            Token::Int(2),
            Token::RightSquareParentheses,
            Token::RightArrow,
            Token::Identifier("y".to_owned()),
            Token::Identifier("TEXT".to_owned()),

            Token::RightParentheses,
            Token::SemiColon,

            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::Multiple(vec![
            ParserOperationTree::CreateTable {
                location: Default::default(),
                name: "test1".to_string(),
                patterns: vec![("line".to_owned(), "A: ([0-9]+)".to_owned(), RegexMode::Captures)],
                columns: vec![
                    ParserColumnDefinition::new(
                        "line".to_string(),
                        1,
                        "x".to_string(),
                        ValueType::Int
                    )
                ]
            },
            ParserOperationTree::CreateTable {
                location: Default::default(),
                name: "test2".to_string(),
                patterns: vec![("line".to_owned(), "A: ([0-9]+), B: ([A-Z]+)".to_owned(), RegexMode::Captures)],
                columns: vec![
                    ParserColumnDefinition::new(
                        "line".to_string(),
                        1,
                        "x".to_string(),
                        ValueType::Int
                    ),
                    ParserColumnDefinition::new(
                        "line".to_string(),
                        2,
                        "y".to_string(),
                        ValueType::String
                    )
                ]
            },
        ]),
        tree
    );
}

#[test]
fn test_parse_create_table4() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Create),
            Token::Keyword(Keyword::Table),
            Token::Identifier("test".to_string()),
            Token::LeftParentheses,

            Token::String("A: ([0-9]+)".to_owned()),
            Token::RightArrow,
            Token::Identifier("x".to_owned()),
            Token::Identifier("INT".to_owned()),

            Token::RightParentheses,
            Token::SemiColon,
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: Default::default(),
            name: "test".to_string(),
            patterns: vec![("_pattern0".to_owned(), "A: ([0-9]+)".to_owned(), RegexMode::Captures)],
            columns: vec![
                ParserColumnDefinition::new(
                    "_pattern0".to_string(),
                    1,
                    "x".to_string(),
                    ValueType::Int
                )
            ]
        },
        tree
    );
}

#[test]
fn test_parse_create_table5() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Create),
            Token::Keyword(Keyword::Table),
            Token::Identifier("test".to_string()),
            Token::LeftParentheses,

            Token::Identifier("line".to_string()),
            Token::Operator(Operator::Single('=')),
            Token::String("A: ([0-9]+)".to_owned()),
            Token::Comma,

            Token::Identifier("line".to_string()),
            Token::LeftSquareParentheses,
            Token::Int(1),
            Token::RightSquareParentheses,
            Token::RightArrow,
            Token::Identifier("x".to_owned()),
            Token::Identifier("INT".to_owned()),
            Token::Keyword(Keyword::Not),
            Token::Null,

            Token::RightParentheses,
            Token::SemiColon,
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: Default::default(),
            name: "test".to_string(),
            patterns: vec![("line".to_owned(), "A: ([0-9]+)".to_owned(), RegexMode::Captures)],
            columns: vec![ParserColumnDefinition {
                parsing: ColumnParsing::Regex(RegexResultReference { pattern_name: "line".to_string(), group_index: 1 }),
                name: "x".to_string(),
                column_type: ValueType::Int,
                nullable: Some(false),
                trim: None,
                convert: None,
                default_value: None
            }]
        },
        tree
    );
}

#[test]
fn test_parse_create_table6() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Create),
            Token::Keyword(Keyword::Table),
            Token::Identifier("test".to_string()),
            Token::LeftParentheses,

            Token::Identifier("line".to_string()),
            Token::Operator(Operator::Single('=')),
            Token::String("A: ([0-9]+)".to_owned()),
            Token::Comma,

            Token::Identifier("line".to_string()),
            Token::LeftSquareParentheses,
            Token::Int(1),
            Token::RightSquareParentheses,
            Token::RightArrow,
            Token::Identifier("x".to_owned()),
            Token::Identifier("TEXT".to_owned()),
            Token::Identifier("TRIM".to_owned()),

            Token::RightParentheses,
            Token::SemiColon,
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: Default::default(),
            name: "test".to_string(),
            patterns: vec![("line".to_owned(), "A: ([0-9]+)".to_owned(), RegexMode::Captures)],
            columns: vec![ParserColumnDefinition {
                parsing: ColumnParsing::Regex(RegexResultReference { pattern_name: "line".to_string(), group_index: 1 }),
                name: "x".to_owned(),
                column_type: ValueType::String,
                nullable: None,
                trim: Some(true),
                convert: None,
                default_value: None
            }]
        },
        tree
    );
}

#[test]
fn test_parse_create_table7() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Create),
            Token::Keyword(Keyword::Table),
            Token::Identifier("test".to_string()),
            Token::LeftParentheses,

            Token::Identifier("line".to_string()),
            Token::Operator(Operator::Single('=')),
            Token::String("A: ([0-9]+), ([0-9]+), ([0-9]+)".to_owned()),
            Token::Comma,

            Token::Identifier("line".to_string()),
            Token::LeftSquareParentheses,
            Token::Int(1),
            Token::RightSquareParentheses,
            Token::Comma,
            Token::Identifier("line".to_string()),
            Token::LeftSquareParentheses,
            Token::Int(2),
            Token::RightSquareParentheses,
            Token::Comma,
            Token::Identifier("line".to_string()),
            Token::LeftSquareParentheses,
            Token::Int(3),
            Token::RightSquareParentheses,
            Token::RightArrow,
            Token::Identifier("x".to_owned()),
            Token::Identifier("INT[]".to_owned()),

            Token::RightParentheses,
            Token::SemiColon,
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: Default::default(),
            name: "test".to_string(),
            patterns: vec![("line".to_owned(), "A: ([0-9]+), ([0-9]+), ([0-9]+)".to_owned(), RegexMode::Captures)],
            columns: vec![
                ParserColumnDefinition {
                    parsing: ColumnParsing::MultiRegex(vec![
                        RegexResultReference::new("line".to_owned(), 1),
                        RegexResultReference::new("line".to_owned(), 2),
                        RegexResultReference::new("line".to_owned(), 3)
                    ]),
                    name: "x".to_string(),
                    column_type: ValueType::Array(Box::new(ValueType::Int)),
                    nullable: None,
                    trim: None,
                    convert: None,
                    default_value: None
                }
            ]
        },
        tree
    );
}

#[test]
fn test_parse_create_table8() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Create),
            Token::Keyword(Keyword::Table),
            Token::Identifier("test".to_string()),
            Token::LeftParentheses,

            Token::Identifier("line".to_string()),
            Token::Operator(Operator::Single('=')),
            Token::Identifier("split".to_owned()),
            Token::String("A: ([0-9]+)".to_owned()),
            Token::Comma,

            Token::Identifier("line".to_string()),
            Token::LeftSquareParentheses,
            Token::Int(1),
            Token::RightSquareParentheses,
            Token::RightArrow,
            Token::Identifier("x".to_owned()),
            Token::Identifier("INT".to_owned()),

            Token::RightParentheses,
            Token::SemiColon,
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: Default::default(),
            name: "test".to_string(),
            patterns: vec![("line".to_owned(), "A: ([0-9]+)".to_owned(), RegexMode::Split)],
            columns: vec![ParserColumnDefinition::new(
                "line".to_string(),
                1,
                "x".to_string(),
                ValueType::Int
            )]
        },
        tree
    );
}

#[test]
fn test_parse_create_table9() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::from_plain_tokens(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Keyword(Keyword::Create),
            Token::Keyword(Keyword::Table),
            Token::Identifier("test".to_string()),
            Token::LeftParentheses,

            Token::Identifier("line".to_string()),
            Token::Operator(Operator::Single('=')),
            Token::String("A: ([0-9]+)".to_owned()),
            Token::Comma,

            Token::Identifier("line".to_string()),
            Token::LeftSquareParentheses,
            Token::Int(1),
            Token::RightSquareParentheses,
            Token::RightArrow,
            Token::Identifier("x".to_owned()),
            Token::Identifier("INT".to_owned()),
            Token::Keyword(Keyword::Default),
            Token::Int(4711),

            Token::RightParentheses,
            Token::SemiColon,
            Token::End
        ]
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: Default::default(),
            name: "test".to_string(),
            patterns: vec![("line".to_owned(), "A: ([0-9]+)".to_owned(), RegexMode::Captures)],
            columns: vec![ParserColumnDefinition {
                parsing: ColumnParsing::Regex(RegexResultReference { pattern_name: "line".to_owned(), group_index: 1 }),
                name: "x".to_string(),
                column_type: ValueType::Int,
                nullable: None,
                trim: None,
                convert: None,
                default_value: Some(Value::Int(4711))
            }]
        },
        tree
    );
}

#[test]
fn test_parse_json_table1() {
    let tree = parse_str("CREATE TABLE connections({.test1.test2} => x INT);").unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: TokenLocation::new(0, 6),
            name: "connections".to_string(),
            patterns: vec![],
            columns: vec![
                ParserColumnDefinition {
                    parsing: ColumnParsing::Json(JsonAccess::Field { name: "test1".to_owned(), inner: Some(Box::new(JsonAccess::Field { name: "test2".to_string(), inner: None })) }),
                    name: "x".to_string(),
                    column_type: ValueType::Int,
                    nullable: None,
                    trim: None,
                    convert: None,
                    default_value: None
                }
            ]
        },
        tree
    );
}

#[test]
fn test_parse_json_table2() {
    let tree = parse_str("CREATE TABLE connections({.test1[3].test2.test3[4]} => x INT);").unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: TokenLocation::new(0, 6),
            name: "connections".to_string(),
            patterns: vec![],
            columns: vec![
                ParserColumnDefinition {
                    parsing: ColumnParsing::Json(JsonAccess::Field {
                        name: "test1".to_owned(),
                        inner: Some(
                            Box::new(JsonAccess::Array {
                                index: 3,
                                inner: Some(Box::new(JsonAccess::Field {
                                    name: "test2".to_owned(),
                                    inner: Some(Box::new(JsonAccess::Field {
                                        name: "test3".to_owned(),
                                        inner: Some(Box::new(JsonAccess::Array { index: 4, inner: None }))
                                    }))
                                }))
                            })
                        )
                    }),
                    name: "x".to_string(),
                    column_type: ValueType::Int,
                    nullable: None,
                    trim: None,
                    convert: None,
                    default_value: None
                }
            ]
        },
        tree
    );
}

#[test]
fn test_parse_str1() {
    let tree = parse_str("SELECT x, MAX(x) FROM test WHERE x >= 13 GROUP BY x").unwrap();

    assert_eq!(
        ParserOperationTree::Select {
            location: TokenLocation::new(0, 6),
            projections: vec![
                (None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(TokenLocation::new(0, 8))),
                (
                    None,
                    ParserExpressionTreeData::Call {
                        name: "MAX".to_owned(),
                        arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(TokenLocation::new(0, 15))],
                        distinct: None
                    }.with_location(TokenLocation::new(0, 13))
                )
            ],
            from: ("test".to_string(), None),
            filter: Some(
                ParserExpressionTreeData::BinaryOperator {
                    operator: Operator::Dual('>', '='),
                    left: Box::new(ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(TokenLocation::new(0, 34))),
                    right: Box::new(ParserExpressionTreeData::Value(Value::Int(13)).with_location(TokenLocation::new(0, 36)))
                }.with_location(TokenLocation::new(0, 34))
            ),
            group_by: Some(vec!["x".to_owned()]),
            having: None,
            join: None,
        },
        tree
    );
}

#[test]
fn test_parse_str2() {
    let tree = parse_str("SELECT x, MAX(x) FROM test::'/haha/test.log' WHERE x >= 13 GROUP BY x").unwrap();

    assert_eq!(
        ParserOperationTree::Select {
            location: TokenLocation::new(0, 6),
            projections: vec![
                (None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(TokenLocation::new(0, 8))),
                (
                    None,
                    ParserExpressionTreeData::Call {
                        name: "MAX".to_owned(),
                        arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(TokenLocation::new(0, 15))],
                        distinct: None
                    }.with_location(TokenLocation::new(0, 13))
                )
            ],
            from: ("test".to_string(), Some("/haha/test.log".to_owned())),
            filter: Some(
                ParserExpressionTreeData::BinaryOperator {
                    operator: Operator::Dual('>', '='),
                    left: Box::new(ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(TokenLocation::new(0, 52))),
                    right: Box::new(ParserExpressionTreeData::Value(Value::Int(13)).with_location(TokenLocation::new(0, 54)))
                }.with_location(TokenLocation::new(0, 52))
            ),
            group_by: Some(vec!["x".to_owned()]), having: None, join: None, },
        tree
    );
}

#[test]
fn test_parse_str3() {
    let tree = parse_str(r"
    CREATE TABLE connections(
        line = 'connection from ([0-9.]+) \\((.*)\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)',

        line[1] => ip TEXT,
        line[2] => hostname TEXT,
        line[9] => year INT,
        line[4] => month TEXT,
        line[5] => day INT,
        line[6] => hour INT,
        line[7] => minute INT,
        line[8] => second INT
    );").unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: TokenLocation::new(1, 10),
            name: "connections".to_string(),
            patterns: vec![
                ("line".to_owned(), "connection from ([0-9.]+) \\((.*)\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)".to_owned(), RegexMode::Captures)
            ],
            columns: vec![
                ParserColumnDefinition::new(
                    "line".to_string(),
                    1,
                    "ip".to_string(),
                    ValueType::String
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    2,
                    "hostname".to_string(),
                    ValueType::String
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    9,
                    "year".to_string(),
                    ValueType::Int
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    4,
                    "month".to_string(),
                    ValueType::String
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    5,
                    "day".to_string(),
                    ValueType::Int
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    6,
                    "hour".to_string(),
                    ValueType::Int
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    7,
                    "minute".to_string(),
                    ValueType::Int
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    8,
                    "second".to_string(),
                    ValueType::Int
                )
            ]
        },
        tree
    );
}

#[test]
fn test_parse_str4() {
    let tree = parse_str("SELECT x, MAX(x) FROM test WHERE x >= 13 GROUP BY x, y, z").unwrap();

    assert_eq!(
        ParserOperationTree::Select {
            location: TokenLocation::new(0, 6),
            projections: vec![
                (None, ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(TokenLocation::new(0, 8))),
                (
                    None,
                    ParserExpressionTreeData::Call {
                        name: "MAX".to_string(),
                        arguments: vec![ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(TokenLocation::new(0, 15))],
                        distinct: None
                    }.with_location(TokenLocation::new(0, 13))
                )
            ],
            from: ("test".to_string(), None),
            filter: Some(
                ParserExpressionTreeData::BinaryOperator {
                    operator: Operator::Dual('>', '='),
                    left: Box::new(ParserExpressionTreeData::ColumnAccess("x".to_owned()).with_location(TokenLocation::new(0, 34))),
                    right: Box::new(ParserExpressionTreeData::Value(Value::Int(13)).with_location(TokenLocation::new(0, 36)))
                }.with_location(TokenLocation::new(0, 34))
            ),
            group_by: Some(vec!["x".to_owned(), "y".to_owned(), "z".to_owned()]),
            having: None,
            join: None,
        },
        tree
    );
}

#[test]
fn test_parse_str5() {
    let tree = parse_str(r"
    CREATE TABLE test(
        line = 'testing (.*) (.*)',

        line[1] => ip TEXT,
        line[2] => hostname TEXT[]
    );").unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: TokenLocation::new(1, 10),
            name: "test".to_string(),
            patterns: vec![
                ("line".to_owned(), "testing (.*) (.*)".to_owned(), RegexMode::Captures)
            ],
            columns: vec![
                ParserColumnDefinition::new(
                    "line".to_string(),
                    1,
                    "ip".to_string(),
                    ValueType::String
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    2,
                    "hostname".to_string(),
                    ValueType::Array(Box::new(ValueType::String))
                )
            ]
        },
        tree
    );
}

#[test]
fn test_parse_str_from_file1() {
    let tree = parse_str(&std::fs::read_to_string("testdata/ftpd.txt").unwrap()).unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: TokenLocation::new(0, 6),
            name: "connections".to_string(),
            patterns: vec![
                ("line".to_owned(), "connection from ([0-9.]+) \\((.+)?\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)".to_owned(), RegexMode::Captures)
            ],
            columns: vec![
                ParserColumnDefinition::new(
                    "line".to_string(),
                    1,
                    "ip".to_string(),
                    ValueType::String
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    2,
                    "hostname".to_string(),
                    ValueType::String
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    9,
                    "year".to_string(),
                    ValueType::Int
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    4,
                    "month".to_string(),
                    ValueType::String
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    5,
                    "day".to_string(),
                    ValueType::Int
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    6,
                    "hour".to_string(),
                    ValueType::Int
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    7,
                    "minute".to_string(),
                    ValueType::Int
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    8,
                    "second".to_string(),
                    ValueType::Int
                )
            ]
        },
        tree
    );
}

#[test]
fn test_parse_str_from_file2() {
    let tree = parse_str(&std::fs::read_to_string("testdata/ftpd_csv.txt").unwrap()).unwrap();

    let mut year_column = ParserColumnDefinition::new(
        "line".to_string(),
        3,
        "year".to_string(),
        ValueType::Int
    );

    year_column.nullable = Some(false);

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: TokenLocation::new(0, 6),
            name: "connections".to_string(),
            patterns: vec![
                ("line".to_owned(), ";".to_owned(), RegexMode::Split)
            ],
            columns: vec![
                ParserColumnDefinition::new(
                    "line".to_string(),
                    1,
                    "ip".to_string(),
                    ValueType::String
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    2,
                    "hostname".to_string(),
                    ValueType::String
                ),
                year_column,
                ParserColumnDefinition::new(
                    "line".to_string(),
                    4,
                    "month".to_string(),
                    ValueType::String
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    5,
                    "day".to_string(),
                    ValueType::Int
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    6,
                    "hour".to_string(),
                    ValueType::Int
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    7,
                    "minute".to_string(),
                    ValueType::Int
                ),
                ParserColumnDefinition::new(
                    "line".to_string(),
                    8,
                    "second".to_string(),
                    ValueType::Int
                )
            ]
        },
        tree
    );
}