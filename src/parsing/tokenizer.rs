use std::str::FromStr;
use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::fmt::Formatter;

use lazy_static::lazy_static;

use crate::parsing::operator::Operator;
use crate::model::ValueType;

#[derive(Debug, PartialEq, Clone)]
pub enum Keyword {
    Select,
    From,
    Where,
    Group,
    By,
    As,
    And,
    Or,
    Create,
    Table,
    Not,
    Is,
    IsNot,
    Having,
    Inner,
    Outer,
    Join,
    On,
    Extract,
    Default
}

impl std::fmt::Display for Keyword {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    Int(i64),
    Float(f64),
    String(String),
    Null,
    True,
    False,
    Operator(Operator),
    Identifier(String),
    Keyword(Keyword),
    LeftParentheses,
    RightParentheses,
    LeftSquareParentheses,
    RightSquareParentheses,
    LeftCurlyParentheses,
    RightCurlyParentheses,
    Comma,
    SemiColon,
    Colon,
    RightArrow,
    End
}

#[derive(Debug, PartialEq, Clone)]
pub enum ParserError {
    Unknown,
    ReachedEndOfTokens,
    TooManyTokens,
    IntConvertError,
    FloatConvertError,
    AlreadyHasDot,
    ExpectedKeyword(Keyword),
    ExpectedAnyKeyword(Vec<Keyword>),
    ExpectedLeftParentheses,
    ExpectedRightParentheses,
    ExpectedLeftSquareParentheses,
    ExpectedRightSquareParentheses,
    ExpectedExpression,
    ExpectedArgumentListContinuation,
    ExpectedProjectionContinuation,
    ExpectedColumnDefinitionStart,
    ExpectedColumnDefinitionContinuation,
    ExpectedJsonColumnPartStart,
    ExpectedIdentifier,
    ExpectedString,
    ExpectedInt,
    ExpectedOperator,
    ExpectedSpecificOperator(Operator),
    ExpectedColon,
    ExpectedRightArrow,
    ExpectedSemiColon,
    ExpectedNull,
    ExpectedColumnAccess,
    NotDefinedBinaryOperator(Operator),
    NotDefinedUnaryOperator(Operator),
    NotDefinedType(String),
    TrimOnlyForString,
    ExpectedValueForDefaultValue,
    ExpectedDefaultValueOfType(ValueType)
}

impl std::fmt::Display for ParserError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ParserError::Unknown => { write!(f, "Unknown error") }
            ParserError::ReachedEndOfTokens => { write!(f, "Reached end of tokens") }
            ParserError::TooManyTokens => { write!(f, "Too many tokens") }
            ParserError::IntConvertError => { write!(f, "Failed to parse integer") }
            ParserError::FloatConvertError => { write!(f, "Failed to parse float") }
            ParserError::AlreadyHasDot => { write!(f, "The number already has a dot") }
            ParserError::ExpectedKeyword(keyword) => { write!(f, "Expected {} keyword", keyword) }
            ParserError::ExpectedAnyKeyword(keywords) => { write!(f, "Expected {} keyword", keywords.iter().map(|k| k.to_string()).collect::<Vec<_>>().join(" or ")) }
            ParserError::ExpectedLeftParentheses => { write!(f, "Expected '('") }
            ParserError::ExpectedRightParentheses => { write!(f, "Expected ')'") }
            ParserError::ExpectedLeftSquareParentheses => { write!(f, "Expected '['") }
            ParserError::ExpectedRightSquareParentheses => { write!(f, "Expected ']'") }
            ParserError::ExpectedExpression => { write!(f, "Expected an expression") }
            ParserError::ExpectedArgumentListContinuation => { write!(f, "Expected ',' or ')'") }
            ParserError::ExpectedProjectionContinuation => { write!(f, "Expected ','") }
            ParserError::ExpectedColumnDefinitionStart => { write!(f, "Expected start of column definition.")  }
            ParserError::ExpectedJsonColumnPartStart => { write!(f, "Expected '.', '[' or '}}'")  }
            ParserError::ExpectedColumnDefinitionContinuation => { write!(f, "Expected ',' or ')'")  }
            ParserError::ExpectedIdentifier => { write!(f, "Expected an identifier") }
            ParserError::ExpectedString => { write!(f, "Expected a string") }
            ParserError::ExpectedInt => { write!(f, "Expected an integer") }
            ParserError::ExpectedOperator => { write!(f, "Expected an operator") }
            ParserError::ExpectedSpecificOperator(operator) => { write!(f, "Expected '{}' operator", operator) }
            ParserError::ExpectedColon => { write!(f, "Expected ':'") }
            ParserError::ExpectedRightArrow => { write!(f, "Expected '=>'") }
            ParserError::ExpectedSemiColon => { write!(f, "Expected ';'") }
            ParserError::ExpectedNull => { write!(f, "Expected NULL") }
            ParserError::ExpectedColumnAccess => { write!(f, "Expected column access") }
            ParserError::NotDefinedBinaryOperator(operator) => { write!(f, "'{}' is not a valid binary operator", operator) }
            ParserError::NotDefinedUnaryOperator(operator) => { write!(f, "'{}' is not a valid unary operator", operator) }
            ParserError::NotDefinedType(value_type) => { write!(f, "'{}' is not a valid type", value_type) }
            ParserError::TrimOnlyForString => { write!(f, "The 'TRIM' modifier is only available for 'TEXT' columns") }
            ParserError::ExpectedValueForDefaultValue => { write!(f, "Expected a value expression for the default value") }
            ParserError::ExpectedDefaultValueOfType(value_type) => { write!(f, "Expected default value be of type '{}'", value_type) }
        }
    }
}

lazy_static! {
    static ref KEYWORDS: HashMap<String, Keyword> = HashMap::from_iter(
        vec![
            ("select".to_owned(), Keyword::Select),
            ("from".to_owned(), Keyword::From),
            ("where".to_owned(), Keyword::Where),
            ("group".to_owned(), Keyword::Group),
            ("by".to_owned(), Keyword::By),
            ("as".to_owned(), Keyword::As),
            ("and".to_owned(), Keyword::And),
            ("or".to_owned(), Keyword::Or),
            ("create".to_owned(), Keyword::Create),
            ("table".to_owned(), Keyword::Table),
            ("not".to_owned(), Keyword::Not),
            ("is".to_owned(), Keyword::Is),
            ("having".to_owned(), Keyword::Having),
            ("inner".to_owned(), Keyword::Inner),
            ("outer".to_owned(), Keyword::Outer),
            ("join".to_owned(), Keyword::Join),
            ("on".to_owned(), Keyword::On),
            ("extract".to_owned(), Keyword::Extract),
            ("default".to_owned(), Keyword::Default),
        ].into_iter()
    );

    static ref TWO_CHAR_OPERATORS: HashSet<char> = HashSet::from_iter(vec!['<', '>', '!', '=', '-'].into_iter());
}

pub fn tokenize(text: &str) -> Result<Vec<Token>, ParserError> {
    let mut tokens = Vec::new();
    let mut char_iterator = text.chars().peekable();

    let mut current_str: Option<String> = None;
    let mut is_escaped = false;
    let mut is_comment = false;
    while let Some(current) = char_iterator.next() {
        if let Some(Token::Operator(Operator::Dual('-', '-'))) = tokens.last() {
            is_comment = true;
            tokens.remove(tokens.len() - 1);
        }

        if is_comment {
            if current == '\n' {
                is_comment = false;
            }

            continue;
        }

        if current == '\\' && !is_escaped {
            is_escaped = true;
            continue;
        }

        let is_string_start = current == '\'' && !is_escaped;
        is_escaped = false;

        if is_string_start {
            if current_str.is_some() {
                tokens.push(Token::String(current_str.unwrap()));
                current_str = None;
            } else {
                current_str = Some(String::new());
            }

            continue;
        } else {
            if let Some(current_str) = current_str.as_mut() {
                current_str.push(current);
                continue;
            }
        }

        if current.is_alphabetic() {
            let mut identifier = String::new();
            identifier.push(current);

            loop {
                match char_iterator.peek() {
                    Some(next) if next.is_alphanumeric() || next == &'_' => {
                        identifier.push(char_iterator.next().unwrap());
                    }
                    _ => {
                        break
                    }
                };
            }

            if let Some(keyword) = KEYWORDS.get(&identifier.to_lowercase()) {
                if keyword == &Keyword::Not && tokens.last() == Some(Token::Keyword(Keyword::Is)).as_ref() {
                    *tokens.last_mut().unwrap() = Token::Keyword(Keyword::IsNot);
                } else {
                    tokens.push(Token::Keyword(keyword.clone()));
                }
            } else if identifier.to_lowercase() == "null" {
                tokens.push(Token::Null);
            } else if identifier.to_lowercase() == "true" {
                tokens.push(Token::True);
            } else if identifier.to_lowercase() == "false" {
                tokens.push(Token::False);
            } else {
                tokens.push(Token::Identifier(identifier));
            }
        } else if current.is_numeric() {
            let mut number = String::new();
            number.push(current);
            let mut has_dot = false;

            loop {
                match char_iterator.peek() {
                    Some(next) if next.is_numeric() => {
                        number.push(char_iterator.next().unwrap());
                    }
                    Some(next) if next == &'.' => {
                        if has_dot {
                            return Err(ParserError::AlreadyHasDot);
                        }

                        has_dot = true;
                        number.push(char_iterator.next().unwrap());
                    }
                    _ => {
                        break
                    }
                };
            }

            if has_dot {
                tokens.push(Token::Float(f64::from_str(&number).map_err(|_err| ParserError::FloatConvertError)?));
            } else {
                tokens.push(Token::Int(i64::from_str(&number).map_err(|_err| ParserError::IntConvertError)?));
            }
        } else if current == '(' {
            tokens.push(Token::LeftParentheses);
        } else if current == ')' {
            tokens.push(Token::RightParentheses);
        } else if current == '[' {
            tokens.push(Token::LeftSquareParentheses);
        } else if current == ']' {
            tokens.push(Token::RightSquareParentheses);
        } else if current == '{' {
            tokens.push(Token::LeftCurlyParentheses);
        } else if current == '}' {
            tokens.push(Token::RightCurlyParentheses);
        } else if current == ',' {
            tokens.push(Token::Comma);
        } else if current == ';' {
            tokens.push(Token::SemiColon);
        } else if current == ':' {
            tokens.push(Token::Colon);
        } else if current.is_whitespace() {
            // Skip
        } else {
            //If the previous token is an operator and the current one also is, upgrade to a two-op char
            let mut is_dual = false;
            if let Some(last) = tokens.last() {
                match last {
                    Token::Operator(Operator::Single('=')) if current == '>' => {
                        *tokens.last_mut().unwrap() = Token::RightArrow;
                        is_dual = true;
                    },
                    Token::Operator(Operator::Single(operator)) if TWO_CHAR_OPERATORS.contains(operator) => {
                        *tokens.last_mut().unwrap() = Token::Operator(Operator::Dual(*operator, current));
                        is_dual = true;
                    }
                    _ => {}
                }
            }

            if !is_dual {
                tokens.push(Token::Operator(Operator::Single(current)));
            }
        }
    }

    tokens.push(Token::End);
    Ok(tokens)
}


#[test]
fn test_tokenize1() {
    let tokens = tokenize("a1 + b + caba + 134 + 12");
    assert_eq!(
        vec![
            Token::Identifier("a1".to_string()),
            Token::Operator(Operator::Single('+')),
            Token::Identifier("b".to_string()),
            Token::Operator(Operator::Single('+')),
            Token::Identifier("caba".to_string()),
            Token::Operator(Operator::Single('+')),
            Token::Int(134),
            Token::Operator(Operator::Single('+')),
            Token::Int(12),
            Token::End
        ],
        tokens.unwrap()
    );
}

#[test]
fn test_tokenize2() {
    let tokens = tokenize("f(a, b, 4)");
    assert_eq!(
        vec![
            Token::Identifier("f".to_string()),
            Token::LeftParentheses,
            Token::Identifier("a".to_string()),
            Token::Comma,
            Token::Identifier("b".to_string()),
            Token::Comma,
            Token::Int(4),
            Token::RightParentheses,
            Token::End
        ],
        tokens.unwrap()
    );
}

#[test]
fn test_tokenize3() {
    let tokens = tokenize("a + 4");
    assert_eq!(
        vec![
            Token::Identifier("a".to_string()),
            Token::Operator(Operator::Single('+')),
            Token::Int(4),
            Token::End
        ],
        tokens.unwrap()
    );
}

#[test]
fn test_tokenize4() {
    let tokens = tokenize("SELECT x FROM test WHERE x > 4");
    assert_eq!(
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
        ],
        tokens.unwrap()
    );
}

#[test]
fn test_tokenize5() {
    let tokens = tokenize("a <= 4");
    assert_eq!(
        vec![
            Token::Identifier("a".to_string()),
            Token::Operator(Operator::Dual('<', '=')),
            Token::Int(4),
            Token::End
        ],
        tokens.unwrap()
    );
}

#[test]
fn test_tokenize6() {
    let tokens = tokenize("NULL true FALSE");
    assert_eq!(
        vec![
            Token::Null,
            Token::True,
            Token::False,
            Token::End
        ],
        tokens.unwrap()
    );
}

#[test]
fn test_tokenize7() {
    let tokens = tokenize("x + 'test 4711.1337' + y");
    assert_eq!(
        vec![
            Token::Identifier("x".to_owned()),
            Token::Operator(Operator::Single('+')),
            Token::String("test 4711.1337".to_owned()),
            Token::Operator(Operator::Single('+')),
            Token::Identifier("y".to_owned()),
            Token::End
        ],
        tokens.unwrap()
    );
}

#[test]
fn test_tokenize8() {
    let tokens = tokenize("x + 'test \\'4711\\'.1337' + y");
    assert_eq!(
        vec![
            Token::Identifier("x".to_owned()),
            Token::Operator(Operator::Single('+')),
            Token::String("test '4711'.1337".to_owned()),
            Token::Operator(Operator::Single('+')),
            Token::Identifier("y".to_owned()),
            Token::End
        ],
        tokens.unwrap()
    );
}

#[test]
fn test_tokenize9() {
    let tokens = tokenize("a => 4");
    assert_eq!(
        vec![
            Token::Identifier("a".to_string()),
            Token::RightArrow,
            Token::Int(4),
            Token::End
        ],
        tokens.unwrap()
    );
}

#[test]
fn test_tokenize10() {
    let tokens = tokenize("line => 'connection from ([0-9.]+) \\\\((.*)\\\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)'");
    assert_eq!(
        vec![
            Token::Identifier("line".to_owned()),
            Token::RightArrow,
            Token::String("connection from ([0-9.]+) \\((.*)\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)".to_owned()),
            Token::End
        ],
        tokens.unwrap()
    );
}

#[test]
fn test_tokenize11() {
    let tokens = tokenize("4.0");
    assert_eq!(
        vec![
            Token::Float(4.0),
            Token::End
        ],
        tokens.unwrap()
    );
}

#[test]
fn test_tokenize12() {
    let tokens = tokenize("x IS NOT NULL");
    assert_eq!(
        vec![
            Token::Identifier("x".to_owned()),
            Token::Keyword(Keyword::IsNot),
            Token::Null,
            Token::End
        ],
        tokens.unwrap()
    );
}

#[test]
fn test_tokenize13() {
    let tokens = tokenize("x IS NOT NULL--this is a comment\ny + x");
    assert_eq!(
        vec![
            Token::Identifier("x".to_owned()),
            Token::Keyword(Keyword::IsNot),
            Token::Null,
            Token::Identifier("y".to_owned()),
            Token::Operator(Operator::Single('+')),
            Token::Identifier("x".to_owned()),
            Token::End
        ],
        tokens.unwrap()
    );
}