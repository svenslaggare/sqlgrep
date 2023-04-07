use std::str::{FromStr, Chars};
use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::{FromIterator, Peekable};
use std::fmt::Formatter;

use lazy_static::lazy_static;

use crate::model::ValueType;
use crate::parsing::operator::Operator;

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
    Default,
    Distinct,
    Case,
    When,
    Then,
    Else,
    End
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
    DoubleColon,
    RightArrow,
    End
}

#[derive(Debug, PartialEq, Clone)]
pub struct TokenLocation {
    pub line: usize,
    pub column: usize,
}

impl TokenLocation {
    pub fn new(line: usize, column: usize) -> TokenLocation {
        TokenLocation {
            line,
            column
        }
    }

    pub fn extract_near(&self, text: &str) -> String {
        if let Some(line) = text.lines().nth(self.line) {
            let mut words = Vec::new();
            let mut word_start: usize = 0;
            let mut word_length: usize = 0;
            let mut index = 0;

            let line_chars = line.chars().collect::<Vec<_>>();
            for current in &line_chars {
                if current.is_whitespace() {
                    words.push((word_start, word_length));
                    word_length = 0;
                    word_start = index + 1;
                } else {
                    word_length += 1;
                }

                index += 1;
            }

            if word_length > 0 {
                words.push((word_start, word_length));
            }

            let get_substr = |(start, length): &(usize, usize)| String::from_iter(&line_chars[*start..(*start + *length)]);

            let mut near_text = String::new();
            for (word_index, &(word_start, word_length)) in words.iter().enumerate() {
                if word_start >= self.column || word_start + word_length >= self.column {
                    near_text = format!(
                        "{}{}{}",
                        words.get(word_index - 1).map(|w| get_substr(w) + " ").unwrap_or(String::new()),
                        get_substr(&(word_start, word_length)),
                        words.get(word_index + 1).map(|w| " ".to_owned() + &get_substr(w)).unwrap_or(String::new()),
                    );
                    break;
                }
            }

            near_text
        } else {
            String::new()
        }
    }
}

impl Default for TokenLocation {
    fn default() -> Self {
        TokenLocation::new(0, 0)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ParserToken {
    pub location: TokenLocation,
    pub token: Token
}

impl ParserToken {
    pub fn new(line: usize, column: usize, token: Token) -> ParserToken {
        ParserToken {
            location: TokenLocation { line, column },
            token
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ParserError {
    pub location: TokenLocation,
    pub error: ParserErrorType
}

impl ParserError {
    pub fn new(location: TokenLocation, error: ParserErrorType) -> ParserError {
        ParserError {
            location,
            error
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum ParserErrorType {
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
    ExpectedDoubleColon,
    ExpectedRightArrow,
    ExpectedSemiColon,
    ExpectedNull,
    ExpectedColumnAccess,
    NotDefinedBinaryOperator(Operator),
    NotDefinedUnaryOperator(Operator),
    NotDefinedType(String),
    TrimOnlyForString,
    ExpectedValueForDefaultValue,
    ExpectedDefaultValueOfType(ValueType),
    AlreadyHaveWhere,
    AlreadyHaveJoin,
    AlreadyHaveGroupBy,
    AlreadyHaveHaving
}

impl std::fmt::Display for ParserErrorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ParserErrorType::Unknown => { write!(f, "Unknown error") }
            ParserErrorType::ReachedEndOfTokens => { write!(f, "Reached end of tokens") }
            ParserErrorType::TooManyTokens => { write!(f, "Too many tokens") }
            ParserErrorType::IntConvertError => { write!(f, "Failed to parse integer") }
            ParserErrorType::FloatConvertError => { write!(f, "Failed to parse float") }
            ParserErrorType::AlreadyHasDot => { write!(f, "The number already has a dot") }
            ParserErrorType::ExpectedKeyword(keyword) => { write!(f, "Expected {} keyword", keyword) }
            ParserErrorType::ExpectedAnyKeyword(keywords) => { write!(f, "Expected {} keyword", keywords.iter().map(|k| k.to_string()).collect::<Vec<_>>().join(" or ")) }
            ParserErrorType::ExpectedLeftParentheses => { write!(f, "Expected '('") }
            ParserErrorType::ExpectedRightParentheses => { write!(f, "Expected ')'") }
            ParserErrorType::ExpectedLeftSquareParentheses => { write!(f, "Expected '['") }
            ParserErrorType::ExpectedRightSquareParentheses => { write!(f, "Expected ']'") }
            ParserErrorType::ExpectedExpression => { write!(f, "Expected an expression") }
            ParserErrorType::ExpectedArgumentListContinuation => { write!(f, "Expected ',' or ')'") }
            ParserErrorType::ExpectedProjectionContinuation => { write!(f, "Expected ','") }
            ParserErrorType::ExpectedColumnDefinitionStart => { write!(f, "Expected start of column definition.")  }
            ParserErrorType::ExpectedJsonColumnPartStart => { write!(f, "Expected '.', '[' or '}}'")  }
            ParserErrorType::ExpectedColumnDefinitionContinuation => { write!(f, "Expected ',' or ')'")  }
            ParserErrorType::ExpectedIdentifier => { write!(f, "Expected an identifier") }
            ParserErrorType::ExpectedString => { write!(f, "Expected a string") }
            ParserErrorType::ExpectedInt => { write!(f, "Expected an integer") }
            ParserErrorType::ExpectedOperator => { write!(f, "Expected an operator") }
            ParserErrorType::ExpectedSpecificOperator(operator) => { write!(f, "Expected '{}' operator", operator) }
            ParserErrorType::ExpectedColon => { write!(f, "Expected ':'") }
            ParserErrorType::ExpectedDoubleColon => { write!(f, "Expected '::'") }
            ParserErrorType::ExpectedRightArrow => { write!(f, "Expected '=>'") }
            ParserErrorType::ExpectedSemiColon => { write!(f, "Expected ';'") }
            ParserErrorType::ExpectedNull => { write!(f, "Expected NULL") }
            ParserErrorType::ExpectedColumnAccess => { write!(f, "Expected column access") }
            ParserErrorType::NotDefinedBinaryOperator(operator) => { write!(f, "'{}' is not a valid binary operator", operator) }
            ParserErrorType::NotDefinedUnaryOperator(operator) => { write!(f, "'{}' is not a valid unary operator", operator) }
            ParserErrorType::NotDefinedType(value_type) => { write!(f, "'{}' is not a valid type", value_type) }
            ParserErrorType::TrimOnlyForString => { write!(f, "The 'TRIM' modifier is only available for 'TEXT' columns") }
            ParserErrorType::ExpectedValueForDefaultValue => { write!(f, "Expected a value expression for the default value") }
            ParserErrorType::ExpectedDefaultValueOfType(value_type) => { write!(f, "Expected default value be of type '{}'", value_type) }
            ParserErrorType::AlreadyHaveWhere => { write!(f, "This statement already have a 'WHERE' clause") }
            ParserErrorType::AlreadyHaveJoin => { write!(f, "This statement already have a 'JOIN' clause") }
            ParserErrorType::AlreadyHaveGroupBy => { write!(f, "This statement already have a 'GROUP BY' clause") }
            ParserErrorType::AlreadyHaveHaving => { write!(f, "This statement already have a 'HAVING' clause") }
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
            ("distinct".to_owned(), Keyword::Distinct),
            ("case".to_owned(), Keyword::Case),
            ("when".to_owned(), Keyword::When),
            ("then".to_owned(), Keyword::Then),
            ("else".to_owned(), Keyword::Else),
            ("end".to_owned(), Keyword::End),
        ].into_iter()
    );

    static ref TWO_CHAR_OPERATORS: HashSet<char> = HashSet::from_iter(vec!['<', '>', '!', '=', '-'].into_iter());
}

pub fn tokenize_simple(text: &str) -> Result<Vec<Token>, ParserError> {
    Ok(tokenize(text)?.into_iter().map(|token| token.token).collect())
}

pub fn tokenize(text: &str) -> Result<Vec<ParserToken>, ParserError> {
    struct TokenizerState<'a> {
        char_iterator: Peekable<Chars<'a>>,
        tokens: Vec<ParserToken>,
        line: usize,
        column: usize,
        token_column_start: usize
    }

    impl<'a> TokenizerState<'a> {
        fn next_char(&mut self) -> Option<char> {
            let result = self.char_iterator.next();

            if result.is_some() {
                self.column += 1;
            }

            result
        }

        fn location(&self) -> TokenLocation {
            TokenLocation { line: self.line, column: self.column }
        }

        fn add(&mut self, token: Token) {
            self.tokens.push(ParserToken::new(self.line, self.token_column_start, token));
            self.token_column_start = self.column;
        }
    }

    let mut state = TokenizerState {
        char_iterator: text.chars().peekable(),
        tokens: Vec::new(),
        line: 0,
        column: 0,
        token_column_start: 0
    };

    let mut current_str: Option<String> = None;
    let mut is_escaped = false;
    let mut is_comment = false;
    while let Some(current) = state.next_char() {
        if current == '\n' {
            state.line += 1;
            state.column = 0;
            state.token_column_start = 0;
        }

        if let Some(Token::Operator(Operator::Dual('-', '-'))) = state.tokens.last().map(|t| &t.token) {
            is_comment = true;
            state.tokens.remove(state.tokens.len() - 1);
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
                state.add(Token::String(current_str.unwrap()));
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
                match state.char_iterator.peek() {
                    Some(next) if next.is_alphanumeric() || next == &'_' => {
                        identifier.push(state.next_char().unwrap());
                    }
                    _ => {
                        break
                    }
                };
            }

            if let Some(keyword) = KEYWORDS.get(&identifier.to_lowercase()) {
                if keyword == &Keyword::Not && state.tokens.last().map(|t| &t.token) == Some(Token::Keyword(Keyword::Is)).as_ref() {
                    state.tokens.last_mut().unwrap().token = Token::Keyword(Keyword::IsNot);
                } else {
                    state.add(Token::Keyword(keyword.clone()));
                }
            } else if identifier.to_lowercase() == "null" {
                state.add(Token::Null);
            } else if identifier.to_lowercase() == "true" {
                state.add(Token::True);
            } else if identifier.to_lowercase() == "false" {
                state.add(Token::False);
            } else {
                state.add(Token::Identifier(identifier));
            }
        } else if current.is_numeric() {
            let mut number = String::new();
            number.push(current);
            let mut has_dot = false;

            loop {
                match state.char_iterator.peek() {
                    Some(next) if next.is_numeric() => {
                        number.push(state.next_char().unwrap());
                    }
                    Some(next) if next == &'.' => {
                        if has_dot {
                            return Err(ParserError::new(state.location(), ParserErrorType::AlreadyHasDot));
                        }

                        has_dot = true;
                        number.push(state.next_char().unwrap());
                    }
                    _ => {
                        break
                    }
                };
            }

            if has_dot {
                state.add(Token::Float(f64::from_str(&number).map_err(|_err| ParserError::new(state.location(), ParserErrorType::FloatConvertError))?));
            } else {
                state.add(Token::Int(i64::from_str(&number).map_err(|_err| ParserError::new(state.location(), ParserErrorType::IntConvertError))?));
            }
        } else if current == '(' {
            state.add(Token::LeftParentheses);
        } else if current == ')' {
            state.add(Token::RightParentheses);
        } else if current == '[' {
            state.add(Token::LeftSquareParentheses);
        } else if current == ']' {
            state.add(Token::RightSquareParentheses);
        } else if current == '{' {
            state.add(Token::LeftCurlyParentheses);
        } else if current == '}' {
            state.add(Token::RightCurlyParentheses);
        } else if current == ',' {
            state.add(Token::Comma);
        } else if current == ';' {
            state.add(Token::SemiColon);
        } else if current == ':' {
            let mut is_dual = false;
            if let Some(last) = state.tokens.last().map(|t| &t.token) {
                match last {
                    Token::Colon => {
                        state.tokens.last_mut().unwrap().token = Token::DoubleColon;
                        is_dual = true;
                    },
                    _ => {}
                }
            }

            if !is_dual {
                state.add(Token::Colon);
            }
        } else if current.is_whitespace() {
            // Skip
        } else {
            //If the previous token is an operator and the current one also is, upgrade to a two-op char
            let mut is_dual = false;
            if let Some(last) = state.tokens.last().map(|t| &t.token) {
                match last {
                    Token::Operator(Operator::Single('=')) if current == '>' => {
                        state.tokens.last_mut().unwrap().token = Token::RightArrow;
                        is_dual = true;
                    },
                    Token::Operator(Operator::Single(operator)) if TWO_CHAR_OPERATORS.contains(operator) => {
                        state.tokens.last_mut().unwrap().token = Token::Operator(Operator::Dual(*operator, current));
                        is_dual = true;
                    }
                    _ => {}
                }
            }

            if !is_dual {
                state.add(Token::Operator(Operator::Single(current)));
            }
        }
    }

    state.add(Token::End);
    Ok(state.tokens)
}

pub fn keywords_list(uppercase: bool) -> Vec<String> {
    let keywords = KEYWORDS.keys();

    if uppercase {
        keywords.map(|keyword| keyword.to_uppercase()).collect()
    } else {
        keywords.cloned().collect()
    }
}

#[test]
fn test_tokenize1() {
    let tokens = tokenize_simple("a1 + b + caba + 134 + 12");
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
    let tokens = tokenize_simple("f(a, b, 4)");
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
    let tokens = tokenize_simple("a + 4");
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
    let tokens = tokenize_simple("SELECT x FROM test WHERE x > 4");
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
    let tokens = tokenize_simple("a <= 4");
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
    let tokens = tokenize_simple("NULL true FALSE");
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
    let tokens = tokenize_simple("x + 'test 4711.1337' + y");
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
    let tokens = tokenize_simple("x + 'test \\'4711\\'.1337' + y");
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
    let tokens = tokenize_simple("a => 4");
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
    let tokens = tokenize_simple("line => 'connection from ([0-9.]+) \\\\((.*)\\\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)'");
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
    let tokens = tokenize_simple("4.0");
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
    let tokens = tokenize_simple("x IS NOT NULL");
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
    let tokens = tokenize_simple("x IS NOT NULL--this is a comment\ny + x");
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