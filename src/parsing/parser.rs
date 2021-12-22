use std::collections::{HashMap, HashSet};

use crate::model::{Value, ValueType, Float, NullableCompareOperator, BooleanOperator};
use crate::data_model::{JsonAccess, ColumnParsing, RegexResultReference, RegexMode};
use crate::parsing::tokenizer::{ParserErrorType, Token, Keyword, tokenize_simple, ParserToken, tokenize, ParserError, TokenLocation};
use crate::parsing::operator::{BinaryOperators, UnaryOperators, Operator};

pub fn parse_str(text: &str) -> ParserResult<ParserOperationTree> {
    let tokens = tokenize(text)?;

    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    Parser::new(&binary_operators, &unary_operators, tokens).parse()
}

#[derive(PartialEq, Debug, Clone)]
pub struct ParserExpressionTree {
    pub location: TokenLocation,
    pub tree: ParserExpressionTreeData
}

impl ParserExpressionTree {
    pub fn new(location: TokenLocation, tree: ParserExpressionTreeData) -> ParserExpressionTree {
        ParserExpressionTree {
            location,
            tree
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum ParserExpressionTreeData {
    Value(Value),
    ColumnAccess(String),
    Wildcard,
    BinaryOperator { operator: Operator, left: Box<ParserExpressionTree>, right: Box<ParserExpressionTree> },
    BooleanOperation { operator: BooleanOperator, left: Box<ParserExpressionTree>, right: Box<ParserExpressionTree> },
    UnaryOperator { operator: Operator, operand: Box<ParserExpressionTree>},
    Invert { operand: Box<ParserExpressionTree> },
    NullableCompare { operator: NullableCompareOperator, left: Box<ParserExpressionTree>, right: Box<ParserExpressionTree> },
    Call { name: String, arguments: Vec<ParserExpressionTree>, distinct: Option<bool> },
    ArrayElementAccess { array: Box<ParserExpressionTree>, index: Box<ParserExpressionTree> }
}

impl ParserExpressionTreeData {
    pub fn with_location(self, location: TokenLocation) -> ParserExpressionTree {
        ParserExpressionTree {
            location,
            tree: self
        }
    }

    pub fn visit<'a, E, F: FnMut(&'a ParserExpressionTreeData) -> Result<(), E>>(&'a self, f: &mut F) -> Result<(), E> {
        match self {
            ParserExpressionTreeData::Value(_) => {}
            ParserExpressionTreeData::ColumnAccess(_) => {}
            ParserExpressionTreeData::Wildcard => {}
            ParserExpressionTreeData::BinaryOperator { left, right, .. } => {
                left.tree.visit(f)?;
                right.tree.visit(f)?;
            }
            ParserExpressionTreeData::BooleanOperation { left, right, .. } => {
                left.tree.visit(f)?;
                right.tree.visit(f)?;
            }
            ParserExpressionTreeData::UnaryOperator { operand, .. } => {
                operand.tree.visit(f)?;
            }
            ParserExpressionTreeData::Invert { operand } => {
                operand.tree.visit(f)?;
            }
            ParserExpressionTreeData::NullableCompare { left, right, .. } => {
                left.tree.visit(f)?;
                right.tree.visit(f)?;
            }
            ParserExpressionTreeData::Call { arguments, .. } => {
                for arg in arguments {
                    arg.tree.visit(f)?;
                }
            }
            ParserExpressionTreeData::ArrayElementAccess { array, index } => {
                array.tree.visit(f)?;
                index.tree.visit(f)?;
            }
        }

        f(self)?;
        Ok(())
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct ParserColumnDefinition {
    pub parsing: ColumnParsing,
    pub name: String,
    pub column_type: ValueType,
    pub nullable: Option<bool>,
    pub trim: Option<bool>,
    pub convert: Option<bool>,
    pub default_value: Option<Value>
}

impl ParserColumnDefinition {
    pub fn new(pattern_name: String,
               pattern_index: usize,
               name: String,
               column_type: ValueType) -> ParserColumnDefinition {
        ParserColumnDefinition {
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
pub struct ParserJoinClause {
    pub joiner_table: String,
    pub joiner_filename: String,
    pub left_table: String,
    pub left_column: String,
    pub right_table: String,
    pub right_column: String,
    pub is_outer: bool
}

#[derive(PartialEq, Debug, Clone)]
pub enum ParserOperationTree {
    Select {
        location: TokenLocation,
        projections: Vec<(Option<String>, ParserExpressionTree)>,
        from: (String, Option<String>),
        filter: Option<ParserExpressionTree>,
        group_by: Option<Vec<String>>,
        having: Option<ParserExpressionTree>,
        join: Option<ParserJoinClause>
    },
    CreateTable {
        location: TokenLocation,
        name: String,
        patterns: Vec<(String, String, RegexMode)>,
        columns: Vec<ParserColumnDefinition>
    },
    Multiple(Vec<ParserOperationTree>)
}

pub type ParserResult<T> = Result<T, ParserError>;

pub struct Parser<'a> {
    tokens: Vec<ParserToken>,
    index: isize,
    binary_operators: &'a BinaryOperators,
    unary_operators: &'a UnaryOperators
}

impl<'a> Parser<'a> {
    pub fn new(binary_operators: &'a BinaryOperators,
               unary_operators: &'a UnaryOperators,
               tokens: Vec<ParserToken>) -> Parser<'a> {
        Parser {
            tokens,
            index: -1,
            binary_operators,
            unary_operators
        }
    }

    pub fn from_plain_tokens(binary_operators: &'a BinaryOperators,
                             unary_operators: &'a UnaryOperators,
                             tokens: Vec<Token>) -> Parser<'a> {
        Parser {
            tokens: tokens.into_iter().map(|token| ParserToken::new(0, 0, token)).collect(),
            index: -1,
            binary_operators,
            unary_operators
        }
    }

    pub fn parse(&mut self) -> ParserResult<ParserOperationTree> {
        self.next()?;

        let operation = match self.current() {
            Token::Keyword(Keyword::Select) => {
                self.parse_select()
            }
            Token::Keyword(Keyword::Create) => {
                self.parse_multiple_create_table()
            }
            _=> { return Err(self.create_error(ParserErrorType::ExpectedAnyKeyword(vec![Keyword::Select, Keyword::Create]))); }
        };

        if self.current() == &Token::SemiColon {
            self.next()?;
        }

        match operation {
            Ok(operation) => {
                if (self.index as usize) + 1 == self.tokens.len() {
                    Ok(operation)
                } else {
                    Err(self.create_error(ParserErrorType::TooManyTokens))
                }
            }
            Err(err) => Err(err)
        }
    }

    fn parse_select(&mut self) -> ParserResult<ParserOperationTree> {
        let mut projections = Vec::new();
        self.next()?;

        let location = self.current_location();
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
                _ => { return Err(self.create_error(ParserErrorType::ExpectedProjectionContinuation)); }
            }
        }

        let table_name = self.consume_identifier()?;
        let mut filename = None;
        if self.current() == &Token::Colon {
            self.next()?;

            self.expect_and_consume_token(
                Token::Colon,
                ParserErrorType::ExpectedColon
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
                            ParserErrorType::ExpectedKeyword(Keyword::By)
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
                    _ => { return Err(self.create_error(ParserErrorType::ExpectedAnyKeyword(vec![Keyword::Where, Keyword::Group]))); }
                }

                if self.current() == &Token::End {
                    break;
                }
            }
        }

        Ok(
            ParserOperationTree::Select {
                location,
                projections,
                from: (table_name, filename),
                filter,
                group_by,
                having,
                join
            }
        )
    }

    fn parse_join(&mut self, is_outer: bool) -> ParserResult<ParserJoinClause> {
        self.next()?;

        self.expect_and_consume_token(
            Token::Keyword(Keyword::Join),
            ParserErrorType::ExpectedKeyword(Keyword::Join)
        )?;

        let joiner_table = self.consume_identifier()?;
        self.expect_and_consume_token(
            Token::Colon,
            ParserErrorType::ExpectedColon
        )?;

        self.expect_and_consume_token(
            Token::Colon,
            ParserErrorType::ExpectedColon
        )?;
        let joiner_filename = self.consume_string()?;

        self.expect_and_consume_token(
            Token::Keyword(Keyword::On),
            ParserErrorType::ExpectedKeyword(Keyword::On)
        )?;

        let left_table = self.consume_identifier()?;
        self.expect_and_consume_operator(Operator::Single('.'))?;
        let left_column = self.consume_identifier()?;

        self.expect_and_consume_operator(Operator::Single('='))?;

        let right_table = self.consume_identifier()?;
        self.expect_and_consume_operator(Operator::Single('.'))?;
        let right_column = self.consume_identifier()?;

        Ok(
            ParserJoinClause {
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

    fn parse_multiple_create_table(&mut self) -> ParserResult<ParserOperationTree> {
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
            Ok(ParserOperationTree::Multiple(operations))
        }
    }

    fn parse_create_table(&mut self) -> ParserResult<ParserOperationTree> {
        self.next()?;

        let location = self.current_location();
        self.expect_and_consume_token(
            Token::Keyword(Keyword::Table),
            ParserErrorType::ExpectedKeyword(Keyword::Table)
        )?;

        let table_name = self.consume_identifier()?;

        self.expect_and_consume_token(
            Token::LeftParentheses,
            ParserErrorType::ExpectedLeftParentheses
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
                                ParserErrorType::ExpectedRightSquareParentheses
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
                                        ParserErrorType::ExpectedLeftSquareParentheses
                                    )?;

                                    let group_index = self.consume_int()? as usize;

                                    self.expect_and_consume_token(
                                        Token::RightSquareParentheses,
                                        ParserErrorType::ExpectedRightSquareParentheses
                                    )?;

                                    pattern_references.push(RegexResultReference::new(pattern_name, group_index));
                                    match self.current() {
                                        Token::RightArrow => { break; },
                                        Token::Comma => {},
                                        _ => {
                                            return Err(self.create_error(ParserErrorType::ExpectedRightArrow));
                                        }
                                    }
                                }
                            }

                            self.expect_and_consume_token(
                                Token::RightArrow,
                                ParserErrorType::ExpectedRightArrow
                            )?;

                            if pattern_references.len() == 1 {
                                columns.push(self.parse_define_column(ColumnParsing::Regex(pattern_references.remove(0)))?);
                            } else {
                                columns.push(self.parse_define_column(ColumnParsing::MultiRegex(pattern_references))?);
                            }
                        }
                        _ => { return Err(self.create_error(ParserErrorType::ExpectedColumnDefinitionStart)) }
                    }
                }
                Token::String(pattern) => {
                    let pattern = pattern.clone();
                    self.next()?;

                    self.expect_and_consume_token(
                        Token::RightArrow,
                        ParserErrorType::ExpectedRightArrow
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
                                self.expect_and_consume_token(Token::RightSquareParentheses, ParserErrorType::ExpectedRightSquareParentheses)?;
                                json_access_parts.push(JsonAccess::Array { index, inner: None });
                            },
                            Token::RightCurlyParentheses => {
                                self.next()?;
                                break;
                            }
                            _ => { return Err(self.create_error(ParserErrorType::ExpectedJsonColumnPartStart)) }
                        }
                    }

                    self.expect_and_consume_token(
                        Token::RightArrow,
                        ParserErrorType::ExpectedRightArrow
                    )?;

                    let json_access = JsonAccess::from_linear(json_access_parts);
                    columns.push(self.parse_define_column(ColumnParsing::Json(json_access))?);
                }
                _ => { return Err(self.create_error(ParserErrorType::ExpectedColumnDefinitionStart)) }
            }

            match self.current() {
                Token::Comma => { self.next()?; }
                Token::RightParentheses => {
                    self.next()?;
                    break;
                }
                _ => { return Err(self.create_error(ParserErrorType::ExpectedColumnDefinitionContinuation)); }
            }
        }

        self.expect_and_consume_token(
            Token::SemiColon,
            ParserErrorType::ExpectedSemiColon
        )?;

        Ok(
            ParserOperationTree::CreateTable {
                location,
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

    fn parse_define_column(&mut self, parsing: ColumnParsing) -> ParserResult<ParserColumnDefinition> {
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
                    ParserErrorType::ExpectedNull
                )?;

                nullable = Some(false);
            }
            Token::Identifier(identifier) if identifier.to_lowercase() == "trim" => {
                if column_type != ValueType::String {
                    return Err(self.create_error(ParserErrorType::TrimOnlyForString));
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
                match self.parse_primary_expression()?.tree {
                    ParserExpressionTreeData::Value(value) => {
                        if let Some(value_type) = value.value_type() {
                            if value_type != column_type {
                                return Err(self.create_error(ParserErrorType::ExpectedDefaultValueOfType(column_type)));
                            }
                        }

                        default_value = Some(value);
                    }
                    _ => { return Err(self.create_error(ParserErrorType::ExpectedValueForDefaultValue)); }
                }
            }
            _ => {}
        }

        Ok(
            ParserColumnDefinition {
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
        let location = self.current_location();
        let mut type_value = self.consume_identifier()?;
        while self.current() == &Token::LeftSquareParentheses {
            self.next()?;
            self.expect_and_consume_token(Token::RightSquareParentheses, ParserErrorType::ExpectedRightParentheses)?;
            type_value += "[]";
        }

        ValueType::from_str(&type_value.to_lowercase()).ok_or(ParserError::new(location, ParserErrorType::NotDefinedType(type_value)))
    }

    pub fn parse_expression(&mut self) -> ParserResult<ParserExpressionTree> {
        self.next()?;
        self.parse_expression_internal()
    }

    fn parse_expression_internal(&mut self) -> ParserResult<ParserExpressionTree> {
        let lhs = self.parse_unary_operator()?;
        self.parse_binary_operator_rhs(0, lhs)
    }

    fn parse_binary_operator_rhs(&mut self, precedence: i32, lhs: ParserExpressionTree) -> ParserResult<ParserExpressionTree> {
        let mut lhs = lhs;
        loop {
            let token_precedence = self.get_token_precedence()?;

            if token_precedence < precedence {
                return Ok(lhs);
            }

            let op_location = self.current_location();
            let op = self.current().clone();
            self.next()?;

            let mut rhs = self.parse_unary_operator()?;
            if token_precedence < self.get_token_precedence()? {
                rhs = self.parse_binary_operator_rhs(token_precedence + 1, rhs)?;
            }

            match op {
                Token::Operator(Operator::Single('.')) => {
                    match (lhs.tree, rhs.tree) {
                        (ParserExpressionTreeData::ColumnAccess(left), ParserExpressionTreeData::ColumnAccess(right)) => {
                            lhs = ParserExpressionTree::new(op_location, ParserExpressionTreeData::ColumnAccess(format!("{}.{}", left, right)));
                        }
                        _ => { return Err(ParserError::new(op_location, ParserErrorType::ExpectedColumnAccess)); }
                    }
                }
                Token::Operator(op) => {
                    lhs = ParserExpressionTree::new(op_location, ParserExpressionTreeData::BinaryOperator { operator: op, left: Box::new(lhs), right: Box::new(rhs) });
                }
                Token::Keyword(Keyword::Is) => {
                    lhs = ParserExpressionTree::new(
                        op_location,
                        ParserExpressionTreeData::NullableCompare { operator: NullableCompareOperator::Equal, left: Box::new(lhs), right: Box::new(rhs) }
                    );
                }
                Token::Keyword(Keyword::IsNot) => {
                    lhs = ParserExpressionTree::new(
                        op_location,
                        ParserExpressionTreeData::NullableCompare { operator: NullableCompareOperator::NotEqual, left: Box::new(lhs), right: Box::new(rhs) }
                    );
                }
                Token::Keyword(Keyword::And) => {
                    lhs = ParserExpressionTree::new(
                        op_location,
                        ParserExpressionTreeData::BooleanOperation { operator: BooleanOperator::And, left: Box::new(lhs), right: Box::new(rhs) }
                    );
                }
                Token::Keyword(Keyword::Or) => {
                    lhs = ParserExpressionTree::new(
                        op_location,
                        ParserExpressionTreeData::BooleanOperation { operator: BooleanOperator::Or, left: Box::new(lhs), right: Box::new(rhs) }
                    );
                }
                Token::LeftSquareParentheses => {
                    lhs = ParserExpressionTree::new(op_location, ParserExpressionTreeData::ArrayElementAccess { array: Box::new(lhs), index: Box::new(rhs) });
                    self.expect_and_consume_token(Token::RightSquareParentheses, ParserErrorType::ExpectedRightSquareParentheses)?;
                }
                _ => { return Err(ParserError::new(op_location, ParserErrorType::ExpectedOperator)); }
            }
        }
    }

    fn get_token_precedence(&self) -> ParserResult<i32> {
        match self.current() {
            Token::Operator(op) => {
                match self.binary_operators.get(op) {
                    Some(bin_op) => Ok(bin_op.precedence),
                    None => Err(self.create_error(ParserErrorType::NotDefinedBinaryOperator(op.clone())))
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

    fn parse_primary_expression(&mut self) -> ParserResult<ParserExpressionTree> {
        let token_location = self.current_location();
        match self.current().clone() {
            Token::Int(value) => {
                self.next()?;
                Ok(ParserExpressionTree::new(token_location, ParserExpressionTreeData::Value(Value::Int(value))))
            }
            Token::Float(value) => {
                self.next()?;
                Ok(ParserExpressionTree::new(token_location, ParserExpressionTreeData::Value(Value::Float(Float(value)))))
            }
            Token::String(value) => {
                self.next()?;
                Ok(ParserExpressionTree::new(token_location, ParserExpressionTreeData::Value(Value::String(value))))
            }
            Token::Null => {
                self.next()?;
                Ok(ParserExpressionTree::new(token_location, ParserExpressionTreeData::Value(Value::Null)))
            }
            Token::True => {
                self.next()?;
                Ok(ParserExpressionTree::new(token_location, ParserExpressionTreeData::Value(Value::Bool(true))))
            }
            Token::False => {
                self.next()?;
                Ok(ParserExpressionTree::new(token_location, ParserExpressionTreeData::Value(Value::Bool(false))))
            }
            Token::Identifier(identifier) => self.parse_identifier_expression(identifier.clone()),
            Token::LeftParentheses => {
                self.next()?;
                let expression = self.parse_expression_internal();

                self.expect_and_consume_token(
                    Token::RightParentheses,
                    ParserErrorType::ExpectedRightParentheses
                )?;

                expression
            }
            Token::Keyword(Keyword::Extract) => {
                self.parse_extract_expression()
            }
            _ => Err(self.create_error(ParserErrorType::ExpectedExpression))
        }
    }

    fn parse_identifier_expression(&mut self, identifier: String) -> ParserResult<ParserExpressionTree> {
        self.next()?;

        let token_location = self.current_location();
        match self.current() {
            Token::LeftParentheses => (),
            _ => return Ok(ParserExpressionTree::new(token_location, ParserExpressionTreeData::ColumnAccess(identifier)))
        }

        self.next()?;

        let mut distinct = None;
        if identifier.to_lowercase() == "count" {
            distinct = Some(false);
            if let Token::Keyword(Keyword::Distinct) = self.current() {
                distinct = Some(true);
                self.next()?;
            }
        }

        let mut arguments = Vec::<ParserExpressionTree>::new();

        match self.current() {
            Token::RightParentheses => (),
            _ => {
                loop {
                    arguments.push(self.parse_expression_internal()?);
                    match self.current() {
                        Token::RightParentheses => { break; }
                        Token::Comma => {}
                        _ => return Err(self.create_error(ParserErrorType::ExpectedArgumentListContinuation))
                    }

                    self.next()?;
                }
            }
        }

        self.next()?;

        Ok(
            ParserExpressionTree::new(
                token_location,
                ParserExpressionTreeData::Call { name: identifier, arguments, distinct }
            )
        )
    }

    fn parse_unary_operator(&mut self) -> ParserResult<ParserExpressionTree> {
        match self.current() {
            Token::Operator(_) | Token::Keyword(Keyword::Not) => {},
            _ => return self.parse_primary_expression()
        }

        let op_location = self.current_location();
        let op_token = self.current().clone();
        self.next()?;

        match op_token {
            Token::Operator(Operator::Single('*')) => {
                return Ok(ParserExpressionTree::new(op_location, ParserExpressionTreeData::Wildcard));
            }
            _ => {}
        };

        let operand = self.parse_unary_operator()?;
        match op_token {
            Token::Operator(op) => {
                if !self.unary_operators.exists(&op) {
                    return Err(ParserError::new(op_location, ParserErrorType::NotDefinedUnaryOperator(op)));
                }

                Ok(ParserExpressionTree::new(op_location,ParserExpressionTreeData::UnaryOperator { operator: op, operand: Box::new(operand) }))
            }
            Token::Keyword(Keyword::Not) => {
                Ok(ParserExpressionTree::new(op_location,ParserExpressionTreeData::Invert { operand: Box::new(operand) }))
            }
            _ => Err(ParserError::new(op_location, ParserErrorType::Unknown))
        }
    }

    fn parse_extract_expression(&mut self) -> ParserResult<ParserExpressionTree> {
        self.next()?;

        self.expect_and_consume_token(Token::LeftParentheses, ParserErrorType::ExpectedLeftParentheses)?;

        let token_location = self.current_location();
        let identifier = self.consume_identifier()?;
        self.expect_and_consume_token(Token::Keyword(Keyword::From), ParserErrorType::ExpectedKeyword(Keyword::From))?;
        let from_expression = self.parse_expression_internal()?;

        self.expect_and_consume_token(Token::RightParentheses, ParserErrorType::ExpectedRightParentheses)?;

        Ok(
            ParserExpressionTree::new(
                token_location,
                ParserExpressionTreeData::Call {
                    name: format!("timestamp_extract_{}", identifier.to_lowercase()),
                    arguments: vec![from_expression],
                    distinct: None
                }
            )
        )
    }

    fn consume_identifier(&mut self) -> ParserResult<String> {
        if let Token::Identifier(identifier) = self.current() {
            let identifier = identifier.clone();
            self.next()?;
            Ok(identifier)
        } else {
            Err(self.create_error(ParserErrorType::ExpectedIdentifier))
        }
    }

    fn consume_string(&mut self) -> ParserResult<String> {
        if let Token::String(string) = self.current() {
            let string = string.clone();
            self.next()?;
            Ok(string)
        } else {
            Err(self.create_error(ParserErrorType::ExpectedString))
        }
    }

    fn consume_int(&mut self) -> ParserResult<i64> {
        if let Token::Int(value) = self.current() {
            let value = *value;
            self.next()?;
            Ok(value)
        } else {
            Err(self.create_error(ParserErrorType::ExpectedInt))
        }
    }

    fn expect_and_consume_token(&mut self, token: Token, error: ParserErrorType) -> ParserResult<()> {
        self.expect_token(token, error)?;
        self.next()?;
        Ok(())
    }

    fn expect_and_consume_operator(&mut self, operator: Operator) -> ParserResult<()> {
        self.expect_token(Token::Operator(operator), ParserErrorType::ExpectedSpecificOperator(operator))?;
        self.next()?;
        Ok(())
    }

    fn expect_token(&self, token: Token, error: ParserErrorType) -> ParserResult<()> {
        if self.current() != &token {
            return Err(self.create_error(error));
        }

        Ok(())
    }

    fn current(&self) -> &Token {
        &self.tokens[self.index as usize].token
    }

    fn current_location(&self) -> TokenLocation {
        self.tokens[self.index as usize].location.clone()
    }

    fn create_error(&self, error: ParserErrorType) -> ParserError {
        ParserError::new(self.current_location(), error)
    }

    fn next(&mut self) -> ParserResult<&Token> {
        let next_index = self.index + 1;
        if next_index >= self.tokens.len() as isize {
            return Err(self.create_error(ParserErrorType::ReachedEndOfTokens));
        }

        self.index = next_index;
        Ok(&self.tokens[self.index as usize].token)
    }
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