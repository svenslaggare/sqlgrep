use std::str::FromStr;
use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::fmt::Formatter;

use lazy_static::lazy_static;

use crate::model::Value;

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
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Operator {
    Single(char),
    Dual(char, char)
}

impl std::fmt::Display for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Operator::Single(x) => write!(f, "{}", x),
            Operator::Dual(x, y) => write!(f, "{}{}", x, y)
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    Int(i64),
    String(String),
    Null,
    True,
    False,
    Operator(Operator),
    Identifier(String),
    Keyword(Keyword),
    LeftParentheses,
    RightParentheses,
    Comma,
    SemiColon,
    Colon,
    End
}

#[derive(Debug, PartialEq, Clone)]
pub enum ParserError {
    IntConvertError,
    ExpectedKeyword(Keyword),
    ExpectedAnyKeyword(Vec<Keyword>),
    ExpectedRightParentheses,
    ExpectedExpression,
    ExpectedArgumentListContinuation,
    ExpectedProjectionContinuation,
    NotDefinedBinaryOperator(Operator),
    NotDefinedUnaryOperator(Operator),
    ExpectedIdentifier,
    ExpectedString,
    ExpectedOperator,
    ExpectedColon,
    ReachedEndOfTokens,
    TooManyTokens
}

impl std::fmt::Display for ParserError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum ParseExpressionTree {
    Value(Value),
    ColumnAccess(String),
    BinaryOperator { operator: Operator, left: Box<ParseExpressionTree>, right: Box<ParseExpressionTree> },
    UnaryOperator { operator: Operator, operand: Box<ParseExpressionTree>},
    AndExpression { left: Box<ParseExpressionTree>, right: Box<ParseExpressionTree> },
    OrExpression { left: Box<ParseExpressionTree>, right: Box<ParseExpressionTree> },
    Call(String, Vec<ParseExpressionTree>)
}

#[derive(PartialEq, Debug, Clone)]
pub enum ParseOperationTree {
    Select {
        projections: Vec<(Option<String>, ParseExpressionTree)>,
        from: (String, Option<String>),
        filter: Option<ParseExpressionTree>,
        group_by: Option<String>
    }
}

pub type ParserResult<T> = Result<T, ParserError>;

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
        ].into_iter()
    );

    static ref TWO_CHAR_OPERATORS: HashSet<char> = HashSet::from_iter(vec!['<', '>', '!', '='].into_iter());
}

pub fn tokenize(text: &str) -> Result<Vec<Token>, ParserError> {
    let mut tokens = Vec::new();
    let mut char_iterator = text.chars().peekable();

    let mut current_str: Option<String> = None;
    let mut is_escaped = false;
    while let Some(current) = char_iterator.next() {
        if current == '\\' {
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
                    Some(next) if next.is_alphanumeric() => {
                        identifier.push(char_iterator.next().unwrap());
                    }
                    _ => {
                        break
                    }
                };
            }

            if let Some(keyword) = KEYWORDS.get(&identifier.to_lowercase()) {
                tokens.push(Token::Keyword(keyword.clone()));
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

            loop {
                match char_iterator.peek() {
                    Some(next) if next.is_numeric() || next == &'.' => {
                        number.push(char_iterator.next().unwrap());
                    }
                    _ => {
                        break
                    }
                };
            }

            tokens.push(Token::Int(i64::from_str(&number).map_err(|_err| ParserError::IntConvertError)?));
        } else if current == '(' {
            tokens.push(Token::LeftParentheses);
        } else if current == ')' {
            tokens.push(Token::RightParentheses);
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
            if !tokens.is_empty() {
                match tokens.last().unwrap() {
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

pub struct BinaryOperator {
    pub precedence: i32
}

impl BinaryOperator {
    fn new(precedence: i32)-> BinaryOperator {
        BinaryOperator {
            precedence
        }
    }
}

pub struct BinaryOperators {
    operators: HashMap<Operator, BinaryOperator>
}

impl BinaryOperators {
    pub fn new() -> BinaryOperators {
        let mut operators = HashMap::new();

        operators.insert(Operator::Single('^'), BinaryOperator::new(5));
        operators.insert(Operator::Single('*'), BinaryOperator::new(5));
        operators.insert(Operator::Single('/'), BinaryOperator::new(5));
        operators.insert(Operator::Single('+'), BinaryOperator::new(4));
        operators.insert(Operator::Single('-'), BinaryOperator::new(4));
        operators.insert(Operator::Single('<'), BinaryOperator::new(3));
        operators.insert(Operator::Dual('<', '='), BinaryOperator::new(3));
        operators.insert(Operator::Single('>'), BinaryOperator::new(3));
        operators.insert(Operator::Dual('>', '='), BinaryOperator::new(3));
        operators.insert(Operator::Single('='), BinaryOperator::new(2));
        operators.insert(Operator::Dual('!', '='), BinaryOperator::new(2));

        BinaryOperators {
            operators
        }
    }

    pub fn get(&self, op: &Operator) -> Option<&BinaryOperator> {
        self.operators.get(op)
    }
}

pub struct UnaryOperators {
    operators: HashSet<Operator>
}

impl UnaryOperators {
    pub fn new() -> UnaryOperators {
        let mut operators = HashSet::<Operator>::new();

        operators.insert(Operator::Single('-'));

        UnaryOperators {
            operators
        }
    }

    pub fn exists(&self, op: &Operator) -> bool {
        self.operators.contains(op)
    }
}

pub struct Parser<'a> {
    tokens: Vec<Token>,
    index: isize,
    binary_operators: &'a BinaryOperators,
    unary_operators: &'a UnaryOperators
}

impl<'a> Parser<'a> {
    pub fn new(binary_operators: &'a BinaryOperators,
               unary_operators: &'a UnaryOperators,
               tokens: Vec<Token>) -> Parser<'a> {
        Parser {
            tokens,
            index: -1,
            binary_operators,
            unary_operators
        }
    }

    pub fn parse(&mut self) -> ParserResult<ParseOperationTree> {
        self.next()?;

        let operation = match self.current() {
            Token::Keyword(Keyword::Select) => {
                self.parse_select()
            }
            _=> { return Err(ParserError::ExpectedAnyKeyword(vec![Keyword::Select])); }
        };

        if self.current() == &Token::SemiColon {
            self.next()?;
        }

        if (self.index as usize) + 1 != self.tokens.len() {
            Err(ParserError::TooManyTokens)
        } else {
            operation
        }
    }

    fn parse_select(&mut self) -> ParserResult<ParseOperationTree> {
        let mut projections = Vec::new();
        self.next()?;

        loop {
            let mut projection_name = None;
            let projection = self.parse_expression_internal()?;
            match self.current() {
                Token::Keyword(Keyword::As) => {
                    self.next()?;
                    projection_name = Some(self.consume_identifier()?);
                }
                _ => {}
            }

            projections.push((projection_name, projection));

            match self.current() {
                Token::Comma => { self.next()?; }
                Token::Keyword(Keyword::From) => {
                    self.next()?;
                    break;
                }
                _ => { return Err(ParserError::ExpectedProjectionContinuation); }
            }
        }

        let table_name = self.consume_identifier()?;
        let mut filename = None;
        if self.current() == &Token::Colon {
            self.next()?;

            if self.current() != &Token::Colon {
                return Err(ParserError::ExpectedColon);
            }

            self.next()?;
            filename = Some(self.consume_string()?);
        }

        let mut filter = None;
        let mut group_by = None;

        if self.current() != &Token::End {
            loop {
                match self.current() {
                    Token::Keyword(Keyword::Where) => {
                        self.next()?;
                        filter = Some(self.parse_expression_internal()?);
                    }
                    Token::Keyword(Keyword::Group) => {
                        self.next()?;
                        if self.current() != &Token::Keyword(Keyword::By) {
                            return Err(ParserError::ExpectedKeyword(Keyword::By));
                        }

                        self.next()?;

                        group_by = Some(self.consume_identifier()?);
                    },
                    Token::SemiColon => {
                        self.next()?;
                        break;
                    }
                    _ => { return Err(ParserError::ExpectedAnyKeyword(vec![Keyword::Where, Keyword::Group])); }
                }

                if self.current() == &Token::End {
                    break;
                }
            }
        }


        Ok(
            ParseOperationTree::Select {
                projections,
                from: (table_name, filename),
                filter,
                group_by
            }
        )
    }

    pub fn parse_expression(&mut self) -> ParserResult<ParseExpressionTree> {
        self.next()?;
        self.parse_expression_internal()
    }

    fn parse_expression_internal(&mut self) -> ParserResult<ParseExpressionTree> {
        let lhs = self.parse_unary_operator()?;
        self.parse_binary_operator_rhs(0, lhs)
    }

    fn parse_binary_operator_rhs(&mut self, precedence: i32, lhs: ParseExpressionTree) -> ParserResult<ParseExpressionTree> {
        let mut lhs = lhs;
        loop {
            let token_precedence = self.get_token_precedence()?;

            if token_precedence < precedence {
                return Ok(lhs);
            }

            let op = self.current().clone();
            self.next()?;

            let mut rhs = self.parse_unary_operator()?;
            if token_precedence < self.get_token_precedence()? {
                rhs = self.parse_binary_operator_rhs(token_precedence + 1, rhs)?;
            }

            match op {
                Token::Operator(op) => {
                    lhs = ParseExpressionTree::BinaryOperator { operator: op, left: Box::new(lhs), right: Box::new(rhs) };
                }
                Token::Keyword(Keyword::And) => {
                    lhs = ParseExpressionTree::AndExpression { left: Box::new(lhs), right: Box::new(rhs) };
                }
                Token::Keyword(Keyword::Or) => {
                    lhs = ParseExpressionTree::OrExpression { left: Box::new(lhs), right: Box::new(rhs) };
                }
                _ => { return Err(ParserError::ExpectedOperator); }
            }
        }
    }

    fn parse_primary_expression(&mut self) -> ParserResult<ParseExpressionTree> {
        match self.current() {
            Token::Int(value) => {
                let value = *value;
                self.next()?;
                Ok(ParseExpressionTree::Value(Value::Int(value)))
            }
            Token::String(value) => {
                let value_copy = value.clone();
                self.next()?;
                Ok(ParseExpressionTree::Value(Value::String(value_copy)))
            }
            Token::Null => {
                self.next()?;
                Ok(ParseExpressionTree::Value(Value::Null))
            }
            Token::True => {
                self.next()?;
                Ok(ParseExpressionTree::Value(Value::Bool(true)))
            }
            Token::False => {
                self.next()?;
                Ok(ParseExpressionTree::Value(Value::Bool(false)))
            }
            Token::Identifier(identifier) => self.parse_identifier_expression(identifier.clone()),
            Token::LeftParentheses => {
                self.next()?;
                let expression = self.parse_expression_internal();

                match self.current() {
                    Token::RightParentheses => {},
                    _ => return Err(ParserError::ExpectedRightParentheses)
                }

                self.next()?;
                expression
            }
            _ => Err(ParserError::ExpectedExpression)
        }
    }

    fn parse_identifier_expression(&mut self, identifier: String) -> ParserResult<ParseExpressionTree> {
        self.next()?;

        match self.current() {
            Token::LeftParentheses => (),
            _ => return Ok(ParseExpressionTree::ColumnAccess(identifier))
        }

        self.next()?;
        let mut arguments = Vec::<ParseExpressionTree>::new();

        match self.current() {
            Token::RightParentheses => (),
            _ => {
                loop {
                    arguments.push(self.parse_expression_internal()?);
                    match self.current() {
                        Token::RightParentheses => { break; }
                        Token::Comma => {}
                        _ => return Err(ParserError::ExpectedArgumentListContinuation)
                    }

                    self.next()?;
                }
            }
        }

        self.next()?;
        return Ok(ParseExpressionTree::Call(identifier, arguments));
    }

    fn parse_unary_operator(&mut self) -> ParserResult<ParseExpressionTree> {
        match self.current() {
            Token::Operator(_) => {},
            _ => return self.parse_primary_expression()
        }

        let op = self.current_to_op()?;
        self.next()?;

        let operand = self.parse_unary_operator()?;
        if !self.unary_operators.exists(&op) {
            return Err(ParserError::NotDefinedUnaryOperator(op));
        }

        Ok(ParseExpressionTree::UnaryOperator { operator: op, operand: Box::new(operand) })
    }

    fn consume_identifier(&mut self) -> ParserResult<String> {
        if let Token::Identifier(identifier) = self.current() {
            let identifier = identifier.clone();
            self.next()?;
            Ok(identifier)
        } else {
            Err(ParserError::ExpectedIdentifier)
        }
    }

    fn consume_string(&mut self) -> ParserResult<String> {
        if let Token::String(string) = self.current() {
            let string = string.clone();
            self.next()?;
            Ok(string)
        } else {
            Err(ParserError::ExpectedString)
        }
    }

    fn current(&self) -> &Token {
        &self.tokens[self.index as usize]
    }

    fn next(&mut self) -> Result<&Token, ParserError> {
        self.index += 1;
        if self.index >= self.tokens.len() as isize {
            return Err(ParserError::ReachedEndOfTokens);
        }

        Ok(&self.tokens[self.index as usize])
    }

    fn current_to_op(&self) -> Result<Operator, ParserError> {
        if let Token::Operator(op) = self.current() {
            Ok(*op)
        } else {
            Err(ParserError::ExpectedOperator)
        }
    }

    fn get_token_precedence(&self) -> Result<i32, ParserError> {
        match self.current() {
            Token::Operator(op) => {
                match self.binary_operators.get(op) {
                    Some(bin_op) => Ok(bin_op.precedence),
                    None => Err(ParserError::NotDefinedBinaryOperator(op.clone()))
                }
            }
            Token::Keyword(Keyword::And) => Ok(1),
            Token::Keyword(Keyword::Or) => Ok(1),
            _ => Ok(-1)
        }
    }
}

#[test]
fn test_advance_parser() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::new(
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
    assert_eq!(Err(ParserError::ReachedEndOfTokens), parser.next());
    assert_eq!(Err(ParserError::ReachedEndOfTokens), parser.next());
}

#[test]
fn test_parse_values() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let tree = Parser::new(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Int(4711),
            Token::End
        ]
    ).parse_expression().unwrap();

    assert_eq!(
        ParseExpressionTree::Value(Value::Int(4711)),
        tree
    );

    let tree = Parser::new(
        &binary_operators,
        &unary_operators,
        vec![
            Token::Null,
            Token::End
        ]
    ).parse_expression().unwrap();

    assert_eq!(
        ParseExpressionTree::Value(Value::Null),
        tree
    );

    let tree = Parser::new(
        &binary_operators,
        &unary_operators,
        vec![
            Token::True,
            Token::End
        ]
    ).parse_expression().unwrap();

    assert_eq!(
        ParseExpressionTree::Value(Value::Bool(true)),
        tree
    );

    let tree = Parser::new(
        &binary_operators,
        &unary_operators,
        vec![
            Token::False,
            Token::End
        ]
    ).parse_expression().unwrap();

    assert_eq!(
        ParseExpressionTree::Value(Value::Bool(false)),
        tree
    );

    let tree = Parser::new(
        &binary_operators,
        &unary_operators,
        vec![
            Token::String("hello world!".to_owned()),
            Token::End
        ]
    ).parse_expression().unwrap();

    assert_eq!(
        ParseExpressionTree::Value(Value::String("hello world!".to_owned())),
        tree
    );
}

#[test]
fn test_parse_expression1() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::new(
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
        ParseExpressionTree::BinaryOperator {
            operator: Operator::Single('+'),
            left: Box::new(ParseExpressionTree::ColumnAccess("a".to_string())),
            right: Box::new(ParseExpressionTree::Value(Value::Int(4)))
        },
        tree
    );
}

#[test]
fn test_parse_expression2() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::new(
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
        ParseExpressionTree::BinaryOperator {
            operator: Operator::Single('*'),
            left: Box::new(
                ParseExpressionTree::BinaryOperator {
                    operator: Operator::Single('+'),
                    left: Box::new(ParseExpressionTree::ColumnAccess("a".to_string())),
                    right: Box::new(ParseExpressionTree::Value(Value::Int(4)))
                }
            ),
            right: Box::new(ParseExpressionTree::ColumnAccess("b".to_string()))
        },
        tree
    );
}

#[test]
fn test_parse_expression3() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::new(
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
        ParseExpressionTree::UnaryOperator {
            operator: Operator::Single('-'),
            operand: Box::new(ParseExpressionTree::Value(Value::Int(4)))
        },
        tree
    );
}

#[test]
fn test_parse_expression4() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::new(
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
        ParseExpressionTree::Call(
            "f".to_string(),
            vec![
                ParseExpressionTree::Value(Value::Int(4)),
                ParseExpressionTree::ColumnAccess("a".to_string()),
            ]
        ),
        tree
    );
}

#[test]
fn test_parse_expression5() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::new(
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
        ParseExpressionTree::AndExpression {
            left: Box::new(ParseExpressionTree::Value(Value::Bool(true))),
            right: Box::new(ParseExpressionTree::Value(Value::Bool(false)))
        },
        tree
    );
}

#[test]
fn test_parse_select1() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::new(
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
        ParseOperationTree::Select {
            projections: vec![(None, ParseExpressionTree::ColumnAccess("x".to_owned()))],
            from: ("test".to_string(), None),
            filter: None,
            group_by: None
        },
        tree
    );
}

#[test]
fn test_parse_select_and_filter1() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::new(
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
        ParseOperationTree::Select {
            projections: vec![(None, ParseExpressionTree::ColumnAccess("x".to_owned()))],
            from: ("test".to_string(), None),
            filter: Some(
                ParseExpressionTree::BinaryOperator {
                    operator: Operator::Single('>'),
                    left: Box::new(ParseExpressionTree::ColumnAccess("x".to_owned())),
                    right: Box::new(ParseExpressionTree::Value(Value::Int(4)))
                }
            ),
            group_by: None
        },
        tree
    );
}

#[test]
fn test_parse_select_and_filter2() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::new(
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
        ParseOperationTree::Select {
            projections: vec![(Some("xxx".to_owned()), ParseExpressionTree::ColumnAccess("x".to_owned()))],
            from: ("test".to_string(), None),
            filter: Some(
                ParseExpressionTree::BinaryOperator {
                    operator: Operator::Single('>'),
                    left: Box::new(ParseExpressionTree::ColumnAccess("x".to_owned())),
                    right: Box::new(ParseExpressionTree::Value(Value::Int(4)))
                }
            ),
            group_by: None
        },
        tree
    );
}

#[test]
fn test_parse_select_and_filter3() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::new(
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
        ParseOperationTree::Select {
            projections: vec![(None, ParseExpressionTree::Call("MAX".to_owned(), vec![ParseExpressionTree::ColumnAccess("x".to_owned())]))],
            from: ("test".to_string(), None),
            filter: Some(
                ParseExpressionTree::BinaryOperator {
                    operator: Operator::Single('>'),
                    left: Box::new(ParseExpressionTree::ColumnAccess("x".to_owned())),
                    right: Box::new(ParseExpressionTree::Value(Value::Int(4)))
                }
            ),
            group_by: None
        },
        tree
    );
}

#[test]
fn test_parse_with_filename() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::new(
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
        ParseOperationTree::Select {
            projections: vec![(None, ParseExpressionTree::ColumnAccess("x".to_owned()))],
            from: ("test".to_string(), Some("test.log".to_owned())),
            filter: None,
            group_by: None
        },
        tree
    );
}

#[test]
fn test_parse_select_group_by1() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::new(
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
        ParseOperationTree::Select {
            projections: vec![(None, ParseExpressionTree::ColumnAccess("x".to_owned()))],
            from: ("test".to_string(), None),
            filter: Some(
                ParseExpressionTree::BinaryOperator {
                    operator: Operator::Single('>'),
                    left: Box::new(ParseExpressionTree::ColumnAccess("x".to_owned())),
                    right: Box::new(ParseExpressionTree::Value(Value::Int(4)))
                }
            ),
            group_by: Some("x".to_owned())
        },
        tree
    );
}

#[test]
fn test_parse_str1() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let tokens = tokenize("SELECT x, MAX(x) FROM test WHERE x >= 13 GROUP BY x");
    assert!(tokens.is_ok());

    let tokens = tokens.unwrap();

    let mut parser = Parser::new(
        &binary_operators,
        &unary_operators,
        tokens
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParseOperationTree::Select {
            projections: vec![
                (None, ParseExpressionTree::ColumnAccess("x".to_owned())),
                (None, ParseExpressionTree::Call("MAX".to_owned(), vec![ParseExpressionTree::ColumnAccess("x".to_owned())]))
            ],
            from: ("test".to_string(), None),
            filter: Some(
                ParseExpressionTree::BinaryOperator {
                    operator: Operator::Dual('>', '='),
                    left: Box::new(ParseExpressionTree::ColumnAccess("x".to_owned())),
                    right: Box::new(ParseExpressionTree::Value(Value::Int(13)))
                }
            ),
            group_by: Some("x".to_owned())
        },
        tree
    );
}

#[test]
fn test_parse_str2() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let tokens = tokenize("SELECT x, MAX(x) FROM test::'/haha/test.log' WHERE x >= 13 GROUP BY x");
    assert!(tokens.is_ok());

    let tokens = tokens.unwrap();

    let mut parser = Parser::new(
        &binary_operators,
        &unary_operators,
        tokens
    );

    let tree = parser.parse().unwrap();

    assert_eq!(
        ParseOperationTree::Select {
            projections: vec![
                (None, ParseExpressionTree::ColumnAccess("x".to_owned())),
                (None, ParseExpressionTree::Call("MAX".to_owned(), vec![ParseExpressionTree::ColumnAccess("x".to_owned())]))
            ],
            from: ("test".to_string(), Some("/haha/test.log".to_owned())),
            filter: Some(
                ParseExpressionTree::BinaryOperator {
                    operator: Operator::Dual('>', '='),
                    left: Box::new(ParseExpressionTree::ColumnAccess("x".to_owned())),
                    right: Box::new(ParseExpressionTree::Value(Value::Int(13)))
                }
            ),
            group_by: Some("x".to_owned())
        },
        tree
    );
}