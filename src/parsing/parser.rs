use std::collections::{HashMap, HashSet};

use crate::model::{Value, ValueType, Float};
use crate::data_model::{JsonAccess, ColumnParsing, RegexResultReference, RegexMode};
use crate::parsing::tokenizer::{ParserError, Token, Keyword, tokenize};
use crate::parsing::operator::{BinaryOperators, UnaryOperators, Operator};

pub fn parse_str(text: &str) -> ParserResult<ParseOperationTree> {
    let tokens = tokenize(text)?;

    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    Parser::new(
        &binary_operators,
        &unary_operators,
        tokens
    ).parse()
}

#[derive(PartialEq, Debug, Clone)]
pub enum ParseExpressionTree {
    Value(Value),
    ColumnAccess(String),
    Wildcard,
    BinaryOperator { operator: Operator, left: Box<ParseExpressionTree>, right: Box<ParseExpressionTree> },
    UnaryOperator { operator: Operator, operand: Box<ParseExpressionTree>},
    Invert { operand: Box<ParseExpressionTree> },
    Is { left: Box<ParseExpressionTree>, right: Box<ParseExpressionTree> },
    IsNot { left: Box<ParseExpressionTree>, right: Box<ParseExpressionTree> },
    And { left: Box<ParseExpressionTree>, right: Box<ParseExpressionTree> },
    Or { left: Box<ParseExpressionTree>, right: Box<ParseExpressionTree> },
    Call { name: String, arguments: Vec<ParseExpressionTree> },
    ArrayElementAccess { array: Box<ParseExpressionTree>, index: Box<ParseExpressionTree> }
}

#[derive(PartialEq, Debug, Clone)]
pub struct ParseColumnDefinition {
    pub parsing: ColumnParsing,
    pub name: String,
    pub column_type: ValueType,
    pub nullable: Option<bool>,
    pub trim: Option<bool>,
    pub convert: Option<bool>,
    pub default_value: Option<Value>
}

impl ParseColumnDefinition {
    pub fn new(pattern_name: String,
               pattern_index: usize,
               name: String,
               column_type: ValueType) -> ParseColumnDefinition {
        ParseColumnDefinition {
            parsing: ColumnParsing::Regex(RegexResultReference { pattern_name, group_index: pattern_index }),
            name,
            column_type,
            nullable: None,
            trim: None,
            convert: None,
            default_value: None
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct ParseJoinClause {
    pub joiner_table: String,
    pub joiner_filename: String,
    pub left_table: String,
    pub left_column: String,
    pub right_table: String,
    pub right_column: String,
    pub is_outer: bool
}

#[derive(PartialEq, Debug, Clone)]
pub enum ParseOperationTree {
    Select {
        projections: Vec<(Option<String>, ParseExpressionTree)>,
        from: (String, Option<String>),
        filter: Option<ParseExpressionTree>,
        group_by: Option<Vec<String>>,
        having: Option<ParseExpressionTree>,
        join: Option<ParseJoinClause>
    },
    CreateTable {
        name: String,
        patterns: Vec<(String, String, RegexMode)>,
        columns: Vec<ParseColumnDefinition>
    },
    Multiple(Vec<ParseOperationTree>)
}

pub type ParserResult<T> = Result<T, ParserError>;

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
            Token::Keyword(Keyword::Create) => {
                self.parse_multiple_create_table()
            }
            _=> { return Err(ParserError::ExpectedAnyKeyword(vec![Keyword::Select, Keyword::Create])); }
        };

        if self.current() == &Token::SemiColon {
            self.next()?;
        }

        match operation {
            Ok(operation) => {
                if (self.index as usize) + 1 == self.tokens.len() {
                    Ok(operation)
                } else {
                    Err(ParserError::TooManyTokens)
                }
            }
            Err(err) => Err(err)
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

            self.expect_and_consume_token(
                Token::Colon,
                ParserError::ExpectedColon
            )?;

            filename = Some(self.consume_string()?);
        }

        let mut filter = None;
        let mut group_by = None;
        let mut having = None;
        let mut join = None;

        if self.current() != &Token::End {
            loop {
                match self.current() {
                    Token::Keyword(Keyword::Where) => {
                        self.next()?;
                        filter = Some(self.parse_expression_internal()?);
                    }
                    Token::Keyword(Keyword::Inner) => {
                        join = Some(self.parse_join(false)?);
                    }
                    Token::Keyword(Keyword::Outer) => {
                        join = Some(self.parse_join(true)?);
                    }
                    Token::Keyword(Keyword::Group) => {
                        self.next()?;

                        self.expect_and_consume_token(
                            Token::Keyword(Keyword::By),
                            ParserError::ExpectedKeyword(Keyword::By)
                        )?;

                        let mut group_by_keys = Vec::new();
                        group_by_keys.push(self.consume_identifier()?);
                        while let Token::Comma = self.current() {
                            self.next()?;
                            group_by_keys.push(self.consume_identifier()?);
                        }

                        group_by = Some(group_by_keys);
                    }
                    Token::Keyword(Keyword::Having) => {
                        self.next()?;
                        having = Some(self.parse_expression_internal()?);
                    }
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
                group_by,
                having,
                join
            }
        )
    }

    fn parse_join(&mut self, is_outer: bool) -> ParserResult<ParseJoinClause> {
        self.next()?;

        self.expect_and_consume_token(
            Token::Keyword(Keyword::Join),
            ParserError::ExpectedKeyword(Keyword::Join)
        )?;

        let joiner_table = self.consume_identifier()?;
        self.expect_and_consume_token(
            Token::Colon,
            ParserError::ExpectedColon
        )?;

        self.expect_and_consume_token(
            Token::Colon,
            ParserError::ExpectedColon
        )?;
        let joiner_filename = self.consume_string()?;

        self.expect_and_consume_token(
            Token::Keyword(Keyword::On),
            ParserError::ExpectedKeyword(Keyword::On)
        )?;

        let left_table = self.consume_identifier()?;
        self.expect_and_consume_operator(Operator::Single('.'))?;
        let left_column = self.consume_identifier()?;

        self.expect_and_consume_operator(Operator::Single('='))?;

        let right_table = self.consume_identifier()?;
        self.expect_and_consume_operator(Operator::Single('.'))?;
        let right_column = self.consume_identifier()?;

        Ok(
            ParseJoinClause {
                joiner_table,
                joiner_filename,
                left_table,
                left_column,
                right_table,
                right_column,
                is_outer
            }
        )
    }

    fn parse_multiple_create_table(&mut self) -> ParserResult<ParseOperationTree> {
        let mut operations = Vec::new();

        loop {
            operations.push(self.parse_create_table()?);

            if self.current() != &Token::Keyword(Keyword::Create) {
                break;
            }
        }

        if operations.len() == 1 {
            Ok(operations.remove(0))
        } else {
            Ok(ParseOperationTree::Multiple(operations))
        }
    }

    fn parse_create_table(&mut self) -> ParserResult<ParseOperationTree> {
        self.next()?;

        self.expect_and_consume_token(
            Token::Keyword(Keyword::Table),
            ParserError::ExpectedKeyword(Keyword::Table)
        )?;

        let table_name = self.consume_identifier()?;

        self.expect_and_consume_token(
            Token::LeftParentheses,
            ParserError::ExpectedLeftParentheses
        )?;

        let mut patterns = Vec::new();
        let mut columns = Vec::new();

        loop {
            match self.current() {
                Token::Identifier(pattern_name) => {
                    let pattern_name = pattern_name.clone();
                    self.next()?;

                    match self.current() {
                        Token::Operator(Operator::Single('=')) => {
                            self.next()?;
                            let regex_mode = self.parse_regex_mode()?;
                            let pattern = self.consume_string()?;
                            patterns.push((pattern_name, pattern, regex_mode));
                        }
                        Token::LeftSquareParentheses => {
                            self.next()?;
                            let group_index = self.consume_int()? as usize;

                            self.expect_and_consume_token(
                                Token::RightSquareParentheses,
                                ParserError::ExpectedRightSquareParentheses
                            )?;

                            let mut pattern_references = vec![
                                RegexResultReference { pattern_name, group_index }
                            ];

                            if self.current() == &Token::Comma {
                                loop {
                                    self.next()?;
                                    let pattern_name = self.consume_identifier()?;

                                    self.expect_and_consume_token(
                                        Token::LeftSquareParentheses,
                                        ParserError::ExpectedLeftSquareParentheses
                                    )?;

                                    let group_index = self.consume_int()? as usize;

                                    self.expect_and_consume_token(
                                        Token::RightSquareParentheses,
                                        ParserError::ExpectedRightSquareParentheses
                                    )?;

                                    pattern_references.push(RegexResultReference::new(pattern_name, group_index));
                                    match self.current() {
                                        Token::RightArrow => { break; },
                                        Token::Comma => {},
                                        _ => {
                                            return Err(ParserError::ExpectedRightArrow);
                                        }
                                    }
                                }
                            }

                            self.expect_and_consume_token(
                                Token::RightArrow,
                                ParserError::ExpectedRightArrow
                            )?;

                            if pattern_references.len() == 1 {
                                columns.push(self.parse_define_column(ColumnParsing::Regex(pattern_references.remove(0)))?);
                            } else {
                                columns.push(self.parse_define_column(ColumnParsing::MultiRegex(pattern_references))?);
                            }
                        }
                        _ => { return Err(ParserError::ExpectedColumnDefinitionStart) }
                    }
                }
                Token::String(pattern) => {
                    let pattern = pattern.clone();
                    self.next()?;

                    self.expect_and_consume_token(
                        Token::RightArrow,
                        ParserError::ExpectedRightArrow
                    )?;

                    let pattern_name = format!("_pattern{}", patterns.len());
                    patterns.push((pattern_name.clone(), pattern.clone(), RegexMode::Captures));

                    columns.push(self.parse_define_column(ColumnParsing::Regex(RegexResultReference { pattern_name, group_index: 1 }))?);
                }
                Token::LeftCurlyParentheses => {
                    self.next()?;
                    let mut json_access_parts = Vec::new();

                    loop {
                        match self.current() {
                            Token::Operator(Operator::Single('.')) => {
                                self.next()?;
                                let identifier = self.consume_identifier()?;
                                json_access_parts.push(JsonAccess::Field { name: identifier, inner: None });
                            }
                            Token::LeftSquareParentheses => {
                                self.next()?;
                                let index = self.consume_int()? as usize;
                                self.expect_and_consume_token(Token::RightSquareParentheses, ParserError::ExpectedRightSquareParentheses)?;
                                json_access_parts.push(JsonAccess::Array { index, inner: None });
                            },
                            Token::RightCurlyParentheses => {
                                self.next()?;
                                break;
                            }
                            _ => { return Err(ParserError::ExpectedJsonColumnPartStart) }
                        }
                    }

                    self.expect_and_consume_token(
                        Token::RightArrow,
                        ParserError::ExpectedRightArrow
                    )?;

                    let json_access = JsonAccess::from_linear(json_access_parts);
                    columns.push(self.parse_define_column(ColumnParsing::Json(json_access))?);
                }
                _ => { return Err(ParserError::ExpectedColumnDefinitionStart) }
            }

            match self.current() {
                Token::Comma => { self.next()?; }
                Token::RightParentheses => {
                    self.next()?;
                    break;
                }
                _ => { return Err(ParserError::ExpectedColumnDefinitionContinuation); }
            }
        }

        self.expect_and_consume_token(
            Token::SemiColon,
            ParserError::ExpectedSemiColon
        )?;

        Ok(
            ParseOperationTree::CreateTable {
                name: table_name,
                patterns,
                columns
            }
        )
    }

    fn parse_regex_mode(&mut self) -> ParserResult<RegexMode> {
        let mut regex_mode = RegexMode::Captures;
        match self.current() {
            Token::Identifier(identifier) if identifier == "split" => {
                regex_mode = RegexMode::Split;
                self.next()?;
            }
            Token::Identifier(identifier) if identifier == "match" => {
                regex_mode = RegexMode::Captures;
                self.next()?;
            }
            _ => {}
        }

        Ok(regex_mode)
    }

    fn parse_define_column(&mut self, parsing: ColumnParsing) -> ParserResult<ParseColumnDefinition> {
        let column_name = self.consume_identifier()?;
        let column_type = self.parse_type()?;

        let mut nullable = None;
        let mut trim = None;
        let mut convert = None;
        let mut default_value = None;

        match self.current().clone() {
            Token::Keyword(Keyword::Not) => {
                self.next()?;
                self.expect_and_consume_token(
                    Token::Null,
                    ParserError::ExpectedNull
                )?;

                nullable = Some(false);
            }
            Token::Identifier(identifier) if identifier.to_lowercase() == "trim" => {
                if column_type != ValueType::String {
                    return Err(ParserError::TrimOnlyForString);
                }

                self.next()?;
                trim = Some(true);
            }
            Token::Identifier(identifier) if identifier.to_lowercase() == "convert" => {
                self.next()?;
                convert = Some(true);
            }
            Token::Keyword(Keyword::Default) => {
                self.next()?;
                match self.parse_primary_expression()? {
                    ParseExpressionTree::Value(value) => {
                        if let Some(value_type) = value.value_type() {
                            if value_type != column_type {
                                return Err(ParserError::ExpectedDefaultValueOfType(column_type));
                            }
                        }

                        default_value = Some(value);
                    }
                    _ => { return Err(ParserError::ExpectedValueForDefaultValue); }
                }
            }
            _ => {}
        }

        Ok(
            ParseColumnDefinition {
                parsing,
                name: column_name,
                column_type,
                nullable,
                trim,
                convert,
                default_value
            }
        )
    }

    fn parse_type(&mut self) -> ParserResult<ValueType> {
        let mut type_value = self.consume_identifier()?;
        while self.current() == &Token::LeftSquareParentheses {
            self.next()?;
            self.expect_and_consume_token(Token::RightSquareParentheses, ParserError::ExpectedRightParentheses)?;
            type_value += "[]";
        }

        ValueType::from_str(&type_value.to_lowercase()).ok_or(ParserError::NotDefinedType(type_value))
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
                Token::Operator(Operator::Single('.')) => {
                    match (lhs, rhs) {
                        (ParseExpressionTree::ColumnAccess(left), ParseExpressionTree::ColumnAccess(right)) => {
                            lhs = ParseExpressionTree::ColumnAccess(format!("{}.{}", left, right));
                        }
                        _ => { return Err(ParserError::ExpectedColumnAccess); }
                    }
                }
                Token::Operator(op) => {
                    lhs = ParseExpressionTree::BinaryOperator { operator: op, left: Box::new(lhs), right: Box::new(rhs) };
                }
                Token::Keyword(Keyword::Is) => {
                    lhs = ParseExpressionTree::Is { left: Box::new(lhs), right: Box::new(rhs) };
                }
                Token::Keyword(Keyword::IsNot) => {
                    lhs = ParseExpressionTree::IsNot { left: Box::new(lhs), right: Box::new(rhs) };
                }
                Token::Keyword(Keyword::And) => {
                    lhs = ParseExpressionTree::And { left: Box::new(lhs), right: Box::new(rhs) };
                }
                Token::Keyword(Keyword::Or) => {
                    lhs = ParseExpressionTree::Or { left: Box::new(lhs), right: Box::new(rhs) };
                }
                Token::LeftSquareParentheses => {
                    lhs = ParseExpressionTree::ArrayElementAccess { array: Box::new(lhs), index: Box::new(rhs) };
                    self.expect_and_consume_token(Token::RightSquareParentheses, ParserError::ExpectedRightSquareParentheses)?;
                }
                _ => { return Err(ParserError::ExpectedOperator); }
            }
        }
    }

    fn get_token_precedence(&self) -> ParserResult<i32> {
        match self.current() {
            Token::Operator(op) => {
                match self.binary_operators.get(op) {
                    Some(bin_op) => Ok(bin_op.precedence),
                    None => Err(ParserError::NotDefinedBinaryOperator(op.clone()))
                }
            }
            Token::Keyword(Keyword::Is) => Ok(2),
            Token::Keyword(Keyword::IsNot) => Ok(2),
            Token::Keyword(Keyword::And) => Ok(1),
            Token::Keyword(Keyword::Or) => Ok(1),
            Token::LeftSquareParentheses => Ok(1),
            _ => Ok(-1)
        }
    }

    fn parse_primary_expression(&mut self) -> ParserResult<ParseExpressionTree> {
        match self.current().clone() {
            Token::Int(value) => {
                self.next()?;
                Ok(ParseExpressionTree::Value(Value::Int(value)))
            }
            Token::Float(value) => {
                self.next()?;
                Ok(ParseExpressionTree::Value(Value::Float(Float(value))))
            }
            Token::String(value) => {
                self.next()?;
                Ok(ParseExpressionTree::Value(Value::String(value)))
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

                self.expect_and_consume_token(
                    Token::RightParentheses,
                    ParserError::ExpectedRightParentheses
                )?;

                expression
            }
            Token::Keyword(Keyword::Extract) => {
                self.parse_extract_expression()
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
        return Ok(ParseExpressionTree::Call {name: identifier, arguments });
    }

    fn parse_unary_operator(&mut self) -> ParserResult<ParseExpressionTree> {
        match self.current() {
            Token::Operator(_) | Token::Keyword(Keyword::Not) => {},
            _ => return self.parse_primary_expression()
        }

        let op_token = self.current().clone();
        self.next()?;

        match op_token {
            Token::Operator(Operator::Single('*')) => {
                return Ok(ParseExpressionTree::Wildcard);
            }
            _ => {}
        };

        let operand = self.parse_unary_operator()?;
        match op_token {
            Token::Operator(op) => {
                if !self.unary_operators.exists(&op) {
                    return Err(ParserError::NotDefinedUnaryOperator(op));
                }

                Ok(ParseExpressionTree::UnaryOperator { operator: op, operand: Box::new(operand) })
            }
            Token::Keyword(Keyword::Not) => {
                Ok(ParseExpressionTree::Invert { operand: Box::new(operand) })
            }
            _ => Err(ParserError::Unknown)
        }
    }

    fn parse_extract_expression(&mut self) -> ParserResult<ParseExpressionTree> {
        self.next()?;

        self.expect_and_consume_token(Token::LeftParentheses, ParserError::ExpectedLeftParentheses)?;

        let identifier = self.consume_identifier()?;
        self.expect_and_consume_token(Token::Keyword(Keyword::From), ParserError::ExpectedKeyword(Keyword::From))?;
        let from_expression = self.parse_expression_internal()?;

        self.expect_and_consume_token(Token::RightParentheses, ParserError::ExpectedRightParentheses)?;

        Ok(ParseExpressionTree::Call {
            name: format!("timestamp_extract_{}", identifier.to_lowercase()),
            arguments: vec![from_expression]
        })
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

    fn consume_int(&mut self) -> ParserResult<i64> {
        if let Token::Int(value) = self.current() {
            let value = *value;
            self.next()?;
            Ok(value)
        } else {
            Err(ParserError::ExpectedInt)
        }
    }

    fn expect_and_consume_token(&mut self, token: Token, error: ParserError) -> ParserResult<()> {
        self.expect_token(token, error)?;
        self.next()?;
        Ok(())
    }

    fn expect_and_consume_operator(&mut self, operator: Operator) -> ParserResult<()> {
        self.expect_token(Token::Operator(operator), ParserError::ExpectedSpecificOperator(operator))?;
        self.next()?;
        Ok(())
    }

    fn expect_token(&self, token: Token, error: ParserError) -> ParserResult<()> {
        if self.current() != &token {
            return Err(error);
        }

        Ok(())
    }

    fn current(&self) -> &Token {
        &self.tokens[self.index as usize]
    }

    fn next(&mut self) -> ParserResult<&Token> {
        self.index += 1;
        if self.index >= self.tokens.len() as isize {
            return Err(ParserError::ReachedEndOfTokens);
        }

        Ok(&self.tokens[self.index as usize])
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
        ParseExpressionTree::Call {
            name: "f".to_string(),
            arguments: vec![
                ParseExpressionTree::Value(Value::Int(4)),
                ParseExpressionTree::ColumnAccess("a".to_string()),
            ]
        },
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
        ParseExpressionTree::And {
            left: Box::new(ParseExpressionTree::Value(Value::Bool(true))),
            right: Box::new(ParseExpressionTree::Value(Value::Bool(false)))
        },
        tree
    );
}

#[test]
fn test_parse_expression6() {
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
fn test_parse_expression7() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::new(
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
        ParseExpressionTree::Invert {
            operand: Box::new(ParseExpressionTree::Value(Value::Bool(true)))
        },
        tree
    );
}

#[test]
fn test_parse_expression8() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::new(
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
        ParseExpressionTree::Is {
            left: Box::new(ParseExpressionTree::Value(Value::Bool(true))),
            right: Box::new(ParseExpressionTree::Value(Value::Null))
        },
        tree
    );

    let mut parser = Parser::new(
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
        ParseExpressionTree::IsNot {
            left: Box::new(ParseExpressionTree::Value(Value::Bool(true))),
            right: Box::new(ParseExpressionTree::Value(Value::Null))
        },
        tree
    );
}

#[test]
fn test_parse_expression9() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::new(
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
        ParseExpressionTree::ColumnAccess("a.b".to_owned()),
        tree
    );
}

#[test]
fn test_parse_expression10() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::new(
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
        ParseExpressionTree::ArrayElementAccess {
            array: Box::new(ParseExpressionTree::ColumnAccess("a".to_owned())),
            index: Box::new(ParseExpressionTree::Value(Value::Int(11)))
        },
        tree
    );
}

#[test]
fn test_parse_expression11() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::new(
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
        ParseExpressionTree::ArrayElementAccess {
            array: Box::new(ParseExpressionTree::ColumnAccess("a".to_owned())),
            index: Box::new(ParseExpressionTree::BinaryOperator {
                operator: Operator::Single('+'),
                left: Box::new(ParseExpressionTree::Value(Value::Int(11))),
                right: Box::new(ParseExpressionTree::ColumnAccess("a".to_owned()))
            })
        },
        tree
    );
}

#[test]
fn test_parse_expression12() {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::new(
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
        ParseExpressionTree::Call {
            name: "timestamp_extract_epoch".to_string(),
            arguments: vec![ParseExpressionTree::ColumnAccess("timestamp".to_owned())]
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

    let mut parser = Parser::new(
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
        ParseOperationTree::Select {
            projections: vec![(None, ParseExpressionTree::Wildcard)],
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
            projections: vec![(None, ParseExpressionTree::Call { name: "MAX".to_owned(), arguments: vec![ParseExpressionTree::ColumnAccess("x".to_owned())] })],
            from: ("test".to_string(), None),
            filter: Some(
                ParseExpressionTree::BinaryOperator {
                    operator: Operator::Single('>'),
                    left: Box::new(ParseExpressionTree::ColumnAccess("x".to_owned())),
                    right: Box::new(ParseExpressionTree::Value(Value::Int(4)))
                }
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
            Token::Comma,
            Token::Identifier("y".to_string()),
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
            group_by: Some(vec!["x".to_owned(), "y".to_owned()]),
            having: None,
            join: None
        },
        tree
    );
}

#[test]
fn test_parse_select_having() {
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
            Token::Keyword(Keyword::Having),
            Token::Identifier("y".to_string()),
            Token::Operator(Operator::Single('<')),
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
            group_by: None,
            having: Some(
                ParseExpressionTree::BinaryOperator {
                    operator: Operator::Single('<'),
                    left: Box::new(ParseExpressionTree::ColumnAccess("y".to_owned())),
                    right: Box::new(ParseExpressionTree::Value(Value::Int(4)))
                }
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
            group_by: None,
            having: None,
            join: Some(
                ParseJoinClause {
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
            group_by: None,
            having: None,
            join: Some(
                ParseJoinClause {
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

    let mut parser = Parser::new(
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
        ParseOperationTree::CreateTable {
            name: "test".to_string(),
            patterns: vec![("line".to_owned(), "A: ([0-9]+)".to_owned(), RegexMode::Captures)],
            columns: vec![ParseColumnDefinition::new(
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

    let mut parser = Parser::new(
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
        ParseOperationTree::CreateTable {
            name: "test".to_string(),
            patterns: vec![("line".to_owned(), "A: ([0-9]+), B: ([A-Z]+)".to_owned(), RegexMode::Captures)],
            columns: vec![
                ParseColumnDefinition::new(
                    "line".to_string(),
                    1,
                    "x".to_string(),
                    ValueType::Int
                ),
                ParseColumnDefinition::new(
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

    let mut parser = Parser::new(
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
        ParseOperationTree::Multiple(vec![
            ParseOperationTree::CreateTable {
                name: "test1".to_string(),
                patterns: vec![("line".to_owned(), "A: ([0-9]+)".to_owned(), RegexMode::Captures)],
                columns: vec![
                    ParseColumnDefinition::new(
                        "line".to_string(),
                        1,
                        "x".to_string(),
                        ValueType::Int
                    )
                ]
            },
            ParseOperationTree::CreateTable {
                name: "test2".to_string(),
                patterns: vec![("line".to_owned(), "A: ([0-9]+), B: ([A-Z]+)".to_owned(), RegexMode::Captures)],
                columns: vec![
                    ParseColumnDefinition::new(
                        "line".to_string(),
                        1,
                        "x".to_string(),
                        ValueType::Int
                    ),
                    ParseColumnDefinition::new(
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

    let mut parser = Parser::new(
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
        ParseOperationTree::CreateTable {
            name: "test".to_string(),
            patterns: vec![("_pattern0".to_owned(), "A: ([0-9]+)".to_owned(), RegexMode::Captures)],
            columns: vec![
                ParseColumnDefinition::new(
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

    let mut parser = Parser::new(
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
        ParseOperationTree::CreateTable {
            name: "test".to_string(),
            patterns: vec![("line".to_owned(), "A: ([0-9]+)".to_owned(), RegexMode::Captures)],
            columns: vec![ParseColumnDefinition {
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

    let mut parser = Parser::new(
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
        ParseOperationTree::CreateTable {
            name: "test".to_string(),
            patterns: vec![("line".to_owned(), "A: ([0-9]+)".to_owned(), RegexMode::Captures)],
            columns: vec![ParseColumnDefinition {
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

    let mut parser = Parser::new(
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
        ParseOperationTree::CreateTable {
            name: "test".to_string(),
            patterns: vec![("line".to_owned(), "A: ([0-9]+), ([0-9]+), ([0-9]+)".to_owned(), RegexMode::Captures)],
            columns: vec![
                ParseColumnDefinition {
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

    let mut parser = Parser::new(
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
        ParseOperationTree::CreateTable {
            name: "test".to_string(),
            patterns: vec![("line".to_owned(), "A: ([0-9]+)".to_owned(), RegexMode::Split)],
            columns: vec![ParseColumnDefinition::new(
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

    let mut parser = Parser::new(
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
        ParseOperationTree::CreateTable {
            name: "test".to_string(),
            patterns: vec![("line".to_owned(), "A: ([0-9]+)".to_owned(), RegexMode::Captures)],
            columns: vec![ParseColumnDefinition {
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
        ParseOperationTree::CreateTable {
            name: "connections".to_string(),
            patterns: vec![],
            columns: vec![
                ParseColumnDefinition {
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
        ParseOperationTree::CreateTable {
            name: "connections".to_string(),
            patterns: vec![],
            columns: vec![
                ParseColumnDefinition {
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
        ParseOperationTree::Select {
            projections: vec![
                (None, ParseExpressionTree::ColumnAccess("x".to_owned())),
                (None, ParseExpressionTree::Call { name: "MAX".to_owned(), arguments: vec![ParseExpressionTree::ColumnAccess("x".to_owned())] })
            ],
            from: ("test".to_string(), None),
            filter: Some(
                ParseExpressionTree::BinaryOperator {
                    operator: Operator::Dual('>', '='),
                    left: Box::new(ParseExpressionTree::ColumnAccess("x".to_owned())),
                    right: Box::new(ParseExpressionTree::Value(Value::Int(13)))
                }
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
        ParseOperationTree::Select { projections: vec![
                (None, ParseExpressionTree::ColumnAccess("x".to_owned())),
                (None, ParseExpressionTree::Call { name: "MAX".to_owned(), arguments: vec![ParseExpressionTree::ColumnAccess("x".to_owned())] })
            ], from: ("test".to_string(), Some("/haha/test.log".to_owned())), filter: Some(
                ParseExpressionTree::BinaryOperator {
                    operator: Operator::Dual('>', '='),
                    left: Box::new(ParseExpressionTree::ColumnAccess("x".to_owned())),
                    right: Box::new(ParseExpressionTree::Value(Value::Int(13)))
                }
            ), group_by: Some(vec!["x".to_owned()]), having: None, join: None, },
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
        ParseOperationTree::CreateTable {
            name: "connections".to_string(),
            patterns: vec![
                ("line".to_owned(), "connection from ([0-9.]+) \\((.*)\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)".to_owned(), RegexMode::Captures)
            ],
            columns: vec![
                ParseColumnDefinition::new(
                    "line".to_string(),
                    1,
                    "ip".to_string(),
                    ValueType::String
                ),
                ParseColumnDefinition::new(
                    "line".to_string(),
                    2,
                    "hostname".to_string(),
                    ValueType::String
                ),
                ParseColumnDefinition::new(
                    "line".to_string(),
                    9,
                    "year".to_string(),
                    ValueType::Int
                ),
                ParseColumnDefinition::new(
                    "line".to_string(),
                    4,
                    "month".to_string(),
                    ValueType::String
                ),
                ParseColumnDefinition::new(
                    "line".to_string(),
                    5,
                    "day".to_string(),
                    ValueType::Int
                ),
                ParseColumnDefinition::new(
                    "line".to_string(),
                    6,
                    "hour".to_string(),
                    ValueType::Int
                ),
                ParseColumnDefinition::new(
                    "line".to_string(),
                    7,
                    "minute".to_string(),
                    ValueType::Int
                ),
                ParseColumnDefinition::new(
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
        ParseOperationTree::Select {
            projections: vec![
                (None, ParseExpressionTree::ColumnAccess("x".to_owned())),
                (None, ParseExpressionTree::Call { name: "MAX".to_string(), arguments: vec![ParseExpressionTree::ColumnAccess("x".to_owned())] })
            ],
            from: ("test".to_string(), None),
            filter: Some(
                ParseExpressionTree::BinaryOperator {
                    operator: Operator::Dual('>', '='),
                    left: Box::new(ParseExpressionTree::ColumnAccess("x".to_owned())),
                    right: Box::new(ParseExpressionTree::Value(Value::Int(13)))
                }
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
        ParseOperationTree::CreateTable {
            name: "test".to_string(),
            patterns: vec![
                ("line".to_owned(), "testing (.*) (.*)".to_owned(), RegexMode::Captures)
            ],
            columns: vec![
                ParseColumnDefinition::new(
                    "line".to_string(),
                    1,
                    "ip".to_string(),
                    ValueType::String
                ),
                ParseColumnDefinition::new(
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
        ParseOperationTree::CreateTable {
            name: "connections".to_string(),
            patterns: vec![
                ("line".to_owned(), "connection from ([0-9.]+) \\((.+)?\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)".to_owned(), RegexMode::Captures)
            ],
            columns: vec![
                ParseColumnDefinition::new(
                    "line".to_string(),
                    1,
                    "ip".to_string(),
                    ValueType::String
                ),
                ParseColumnDefinition::new(
                    "line".to_string(),
                    2,
                    "hostname".to_string(),
                    ValueType::String
                ),
                ParseColumnDefinition::new(
                    "line".to_string(),
                    9,
                    "year".to_string(),
                    ValueType::Int
                ),
                ParseColumnDefinition::new(
                    "line".to_string(),
                    4,
                    "month".to_string(),
                    ValueType::String
                ),
                ParseColumnDefinition::new(
                    "line".to_string(),
                    5,
                    "day".to_string(),
                    ValueType::Int
                ),
                ParseColumnDefinition::new(
                    "line".to_string(),
                    6,
                    "hour".to_string(),
                    ValueType::Int
                ),
                ParseColumnDefinition::new(
                    "line".to_string(),
                    7,
                    "minute".to_string(),
                    ValueType::Int
                ),
                ParseColumnDefinition::new(
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

    let mut year_column = ParseColumnDefinition::new(
        "line".to_string(),
        3,
        "year".to_string(),
        ValueType::Int
    );

    year_column.nullable = Some(false);

    assert_eq!(
        ParseOperationTree::CreateTable {
            name: "connections".to_string(),
            patterns: vec![
                ("line".to_owned(), ";".to_owned(), RegexMode::Split)
            ],
            columns: vec![
                ParseColumnDefinition::new(
                    "line".to_string(),
                    1,
                    "ip".to_string(),
                    ValueType::String
                ),
                ParseColumnDefinition::new(
                    "line".to_string(),
                    2,
                    "hostname".to_string(),
                    ValueType::String
                ),
                year_column,
                ParseColumnDefinition::new(
                    "line".to_string(),
                    4,
                    "month".to_string(),
                    ValueType::String
                ),
                ParseColumnDefinition::new(
                    "line".to_string(),
                    5,
                    "day".to_string(),
                    ValueType::Int
                ),
                ParseColumnDefinition::new(
                    "line".to_string(),
                    6,
                    "hour".to_string(),
                    ValueType::Int
                ),
                ParseColumnDefinition::new(
                    "line".to_string(),
                    7,
                    "minute".to_string(),
                    ValueType::Int
                ),
                ParseColumnDefinition::new(
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