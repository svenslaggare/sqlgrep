use crate::data_model::{ColumnParsing, JsonAccess, RegexMode, RegexResultReference};
use crate::model::{Value, ValueType};
use crate::parsing::operator::{BinaryOperators, Operator, UnaryOperators};
use crate::parsing::parser::{parse_str, Parser, ParserColumnDefinition, ParserExpressionTree, ParserExpressionTreeData, ParserOperationTree, ParserResult};
use crate::parsing::tokenizer::{ParserError, ParserErrorType, Token, tokenize, TokenLocation};

fn parse_expression_str(text: &str) -> ParserResult<ParserExpressionTree> {
    let tokens = tokenize(text)?;

    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    Parser::new(&binary_operators, &unary_operators, tokens).parse_expression()
}

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
    let tree = parse_expression_str("a + 4").unwrap();
    assert_eq!("(a + 4)", tree.tree.to_string());
}

#[test]
fn test_parse_expression2() {
    let tree = parse_expression_str("(a + 4) * b").unwrap();
    assert_eq!("((a + 4) * b)", tree.tree.to_string());
}

#[test]
fn test_parse_expression3() {
    let tree = parse_expression_str("-4").unwrap();
    assert_eq!("-4", tree.tree.to_string());
}

#[test]
fn test_parse_expression4() {
    let tree = parse_expression_str("f(4, a)").unwrap();
    assert_eq!("f(4, a)", tree.tree.to_string());
}

#[test]
fn test_parse_expression5() {
    let tree = parse_expression_str("TRUE AND FALSE").unwrap();
    assert_eq!("(true AND false)", tree.tree.to_string());
}

#[test]
fn test_parse_expression6() {
    let tree = parse_expression_str("NOT TRUE").unwrap();
    assert_eq!("NOT true", tree.tree.to_string());
}

#[test]
fn test_parse_expression7() {
    let tree = parse_expression_str("TRUE IS NULL").unwrap();
    assert_eq!("(true IS NULL)", tree.tree.to_string());

    let tree = parse_expression_str("TRUE IS NOT NULL").unwrap();
    assert_eq!("(true IS NOT NULL)", tree.tree.to_string());
}

#[test]
fn test_parse_expression8() {
    let tree = parse_expression_str("a.b").unwrap();
    assert_eq!("a.b", tree.tree.to_string());
}

#[test]
fn test_parse_expression9() {
    let tree = parse_expression_str("a[11]").unwrap();
    assert_eq!("a[11]", tree.tree.to_string());
}

#[test]
fn test_parse_expression10() {
    let tree = parse_expression_str("a[11 + b]").unwrap();
    assert_eq!("a[(11 + b)]", tree.tree.to_string());
}

#[test]
fn test_parse_expression11() {
    let tree = parse_expression_str("EXTRACT (EPOCH FROM timestamp)").unwrap();
    assert_eq!("timestamp_extract_epoch(timestamp)", tree.tree.to_string());
}

#[test]
fn test_parse_expression12() {
    let tree = parse_expression_str("COUNT(DISTINCT timestamp)").unwrap();
    assert_eq!("COUNT(timestamp, distinct=true)", tree.tree.to_string());
}

#[test]
fn test_parse_expression13() {
    let tree = parse_expression_str("array[1337, 4711]").unwrap();
    assert_eq!("create_array(1337, 4711)", tree.tree.to_string());
}

#[test]
fn test_parse_type_convert1() {
    let tree = parse_expression_str("'2022-10-11 22:00:00'::timestamp").unwrap();
    assert_eq!("'2022-10-11 22:00:00'::timestamp", tree.tree.to_string());
}

#[test]
fn test_parse_select1() {
    let tree = parse_str("SELECT x FROM test").unwrap();
    assert_eq!("SELECT x FROM test", tree.to_string());
}

#[test]
fn test_parse_select2() {
    let tree = parse_str("SELECT * FROM test").unwrap();
    assert_eq!("SELECT * FROM test", tree.to_string());
}

#[test]
fn test_parse_select_and_filter1() {
    let tree = parse_str("SELECT x FROM test WHERE x > 4").unwrap();
    assert_eq!("SELECT x FROM test WHERE (x > 4)", tree.to_string());
}

#[test]
fn test_parse_select_and_filter2() {
    let tree = parse_str("SELECT x AS xxx FROM test WHERE x > 4").unwrap();
    assert_eq!("SELECT x AS xxx FROM test WHERE (x > 4)", tree.to_string());
}

#[test]
fn test_parse_select_and_filter3() {
    let tree = parse_str("SELECT MAX(x) FROM test WHERE x > 4").unwrap();
    assert_eq!("SELECT MAX(x) FROM test WHERE (x > 4)", tree.to_string());
}

#[test]
fn test_parse_select_and_filter4() {
    let tree = parse_str("SELECT MAX(x) * 2 FROM test WHERE x > 4").unwrap();
    assert_eq!("SELECT (MAX(x) * 2) FROM test WHERE (x > 4)", tree.to_string());
}

#[test]
fn test_parse_with_filename() {
    let tree = parse_str("SELECT x FROM test::'test.log'").unwrap();
    assert_eq!("SELECT x FROM test::'test.log'", tree.to_string());
}

#[test]
fn test_parse_select_group_by1() {
    let tree = parse_str("SELECT x FROM test WHERE x > 4 GROUP BY x").unwrap();
    assert_eq!("SELECT x FROM test WHERE (x > 4) GROUP BY x", tree.to_string());
}

#[test]
fn test_parse_select_group_by2() {
    let tree = parse_str("SELECT x FROM test WHERE x > 4 GROUP BY x, y").unwrap();
    assert_eq!("SELECT x FROM test WHERE (x > 4) GROUP BY x, y", tree.to_string());
}

#[test]
fn test_parse_select_group_by3() {
    let tree = parse_str("SELECT x FROM test WHERE x > 4 GROUP BY x GROUP BY y");
    assert_eq!(
        Err(ParserError::new(TokenLocation::new(0, 50), ParserErrorType::AlreadyHaveGroupBy)),
        tree,
    );
}

#[test]
fn test_parse_select_having() {
    let tree = parse_str("SELECT x FROM test WHERE x > 4 GROUP BY x HAVING y < 4").unwrap();
    assert_eq!("SELECT x FROM test WHERE (x > 4) GROUP BY x HAVING (y < 4)", tree.to_string());
}

#[test]
fn test_parse_select_many_features1() {
    let tree = parse_str("SELECT x, MAX(x) FROM test::'/haha/test.log' WHERE x >= 13 GROUP BY x").unwrap();
    assert_eq!("SELECT x, MAX(x) FROM test::'/haha/test.log' WHERE (x >= 13) GROUP BY x", tree.to_string());
}

#[test]
fn test_parse_inner_join1() {
    let tree = parse_str("SELECT x FROM test WHERE x > 4 INNER JOIN table1::'file.log' ON table2.x = table1.y").unwrap();
    assert_eq!("SELECT x FROM test WHERE (x > 4) INNER JOIN table1::'file.log' ON table2.x = table1.y", tree.to_string());
}

#[test]
fn test_parse_outer_join1() {
    let tree = parse_str("SELECT x FROM test WHERE x > 4 OUTER JOIN table1::'file.log' ON table2.x = table1.y").unwrap();
    assert_eq!("SELECT x FROM test WHERE (x > 4) OUTER JOIN table1::'file.log' ON table2.x = table1.y", tree.to_string());
}

#[test]
fn test_parse_create_table1() {
    let tree = parse_str("CREATE TABLE test(line = 'A: ([0-9]+)', line[1] => x INT);").unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: TokenLocation::new(0, 6),
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
    let tree = parse_str("CREATE TABLE test(line = 'A: ([0-9]+), B: ([A-Z]+)', line[1] => x INT, line[2] => y TEXT);").unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: TokenLocation::new(0, 6),
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
fn test_parse_create_table_multiple1() {
    let tree = parse_str("CREATE TABLE test1(line = 'A: ([0-9]+)', line[1] => x INT); CREATE TABLE test2(line = 'A: ([0-9]+), B: ([A-Z]+)', line[1] => x INT, line[2] => y TEXT);").unwrap();

    assert_eq!(
        ParserOperationTree::Multiple(vec![
            ParserOperationTree::CreateTable {
                location: TokenLocation::new(0, 6),
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
                location: TokenLocation::new(0, 66),
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
fn test_parse_create_table_inline1() {
    let tree = parse_str("CREATE TABLE test('A: ([0-9]+)' => x INT);").unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: TokenLocation::new(0, 6),
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
fn test_parse_create_table_create_array1() {
    let tree = parse_str("CREATE TABLE test(line = 'A: ([0-9]+), ([0-9]+), ([0-9]+)', line[1], line[2], line[3] => x INT[]);").unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: TokenLocation::new(0, 6),
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
                    microseconds: None,
                    default_value: None
                }
            ]
        },
        tree
    );
}

#[test]
fn test_parse_create_table_split_mode1() {
    let tree = parse_str("CREATE TABLE test(line = split 'A: ([0-9]+)', line[1] => x INT);").unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: TokenLocation::new(0, 6),
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
fn test_parse_create_table_modifiers1() {
    let tree = parse_str("CREATE TABLE test(line = 'A: ([0-9]+)', line[1] => x INT NOT NULL);").unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: TokenLocation::new(0, 6),
            name: "test".to_string(),
            patterns: vec![("line".to_owned(), "A: ([0-9]+)".to_owned(), RegexMode::Captures)],
            columns: vec![ParserColumnDefinition {
                parsing: ColumnParsing::Regex(RegexResultReference { pattern_name: "line".to_string(), group_index: 1 }),
                name: "x".to_string(),
                column_type: ValueType::Int,
                nullable: Some(false),
                trim: None,
                convert: None,
                microseconds: None,
                default_value: None
            }]
        },
        tree
    );
}

#[test]
fn test_parse_create_table_modifiers2() {
    let tree = parse_str("CREATE TABLE test(line = 'A: ([0-9]+)', line[1] => x TEXT TRIM);").unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: TokenLocation::new(0, 6),
            name: "test".to_string(),
            patterns: vec![("line".to_owned(), "A: ([0-9]+)".to_owned(), RegexMode::Captures)],
            columns: vec![ParserColumnDefinition {
                parsing: ColumnParsing::Regex(RegexResultReference { pattern_name: "line".to_string(), group_index: 1 }),
                name: "x".to_owned(),
                column_type: ValueType::String,
                nullable: None,
                trim: Some(true),
                convert: None,
                microseconds: None,
                default_value: None
            }]
        },
        tree
    );
}

#[test]
fn test_parse_create_table_modifiers3() {
    let tree = parse_str("CREATE TABLE test(line = 'A: ([0-9]+)', line[1] => x INT DEFAULT 4711);").unwrap();

    assert_eq!(
        ParserOperationTree::CreateTable {
            location: TokenLocation::new(0, 6),
            name: "test".to_string(),
            patterns: vec![("line".to_owned(), "A: ([0-9]+)".to_owned(), RegexMode::Captures)],
            columns: vec![ParserColumnDefinition {
                parsing: ColumnParsing::Regex(RegexResultReference { pattern_name: "line".to_owned(), group_index: 1 }),
                name: "x".to_string(),
                column_type: ValueType::Int,
                nullable: None,
                trim: None,
                convert: None,
                microseconds: None,
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
                    microseconds: None,
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
                    microseconds: None,
                    default_value: None
                }
            ]
        },
        tree
    );
}

#[test]
fn test_parse_create_table_multiline1() {
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
fn test_parse_create_table_multiline2() {
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