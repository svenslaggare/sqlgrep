use crate::model::{Value, ValueType, Float, NullableCompareOperator, BooleanOperator};
use crate::data_model::{JsonAccess, ColumnParsing, RegexResultReference, RegexMode};
use crate::execution::ColumnScope;
use crate::parsing::tokenizer::{ParserErrorType, Token, Keyword, ParserToken, tokenize, ParserError, TokenLocation};
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
    ScopedColumnAccess(ColumnScope, String),
    Wildcard,
    BinaryOperator { operator: Operator, left: Box<ParserExpressionTree>, right: Box<ParserExpressionTree> },
    BooleanOperation { operator: BooleanOperator, left: Box<ParserExpressionTree>, right: Box<ParserExpressionTree> },
    UnaryOperator { operator: Operator, operand: Box<ParserExpressionTree>},
    Invert { operand: Box<ParserExpressionTree> },
    NullableCompare { operator: NullableCompareOperator, left: Box<ParserExpressionTree>, right: Box<ParserExpressionTree> },
    Call { name: String, arguments: Vec<ParserExpressionTree>, distinct: Option<bool> },
    ArrayElementAccess { array: Box<ParserExpressionTree>, index: Box<ParserExpressionTree> },
    TypeConversion { operand: Box<ParserExpressionTree>, convert_to_type: ValueType },
    Case { clauses: Vec<(ParserExpressionTree, ParserExpressionTree)>, else_clause: Box<ParserExpressionTree> }
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
            ParserExpressionTreeData::ScopedColumnAccess(_, _) => {}
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
            ParserExpressionTreeData::TypeConversion { operand, .. } => {
                operand.tree.visit(f)?;
            }
            ParserExpressionTreeData::Case { clauses, else_clause } => {
                for clause in clauses {
                    clause.0.tree.visit(f)?;
                    clause.1.tree.visit(f)?;
                }

                else_clause.tree.visit(f)?;
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
    pub microseconds: Option<bool>,
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
            microseconds: None,
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
        group_by: Option<Vec<ParserExpressionTree>>,
        having: Option<ParserExpressionTree>,
        join: Option<ParserJoinClause>,
        limit: Option<usize>,
        distinct: bool
    },
    CreateTable {
        location: TokenLocation,
        end_location: TokenLocation,
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
        self.next()?;

        let mut projections = Vec::new();
        let mut distinct = false;

        if self.current() == &Token::Keyword(Keyword::Distinct) {
            self.next()?;
            distinct = true;
        }

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
        if self.current() == &Token::DoubleColon {
            self.next()?;
            filename = Some(self.consume_string()?);
        }

        let mut filter = None;
        let mut group_by = None;
        let mut having = None;
        let mut join = None;
        let mut limit = None;

        if self.current() != &Token::End {
            loop {
                match self.current() {
                    Token::Keyword(Keyword::Where) => {
                        self.next()?;
                        if filter.is_some() {
                            return Err(self.create_error(ParserErrorType::AlreadyHaveWhere));
                        }

                        filter = Some(self.parse_expression_internal()?);
                    }
                    Token::Keyword(Keyword::Inner) => {
                        if join.is_some() {
                            return Err(self.create_error(ParserErrorType::AlreadyHaveJoin));
                        }

                        join = Some(self.parse_join(false)?);
                    }
                    Token::Keyword(Keyword::Outer) => {
                        if join.is_some() {
                            return Err(self.create_error(ParserErrorType::AlreadyHaveJoin));
                        }

                        join = Some(self.parse_join(true)?);
                    }
                    Token::Keyword(Keyword::Group) => {
                        self.next()?;

                        self.expect_and_consume_token(
                            Token::Keyword(Keyword::By),
                            ParserErrorType::ExpectedKeyword(Keyword::By)
                        )?;

                        if group_by.is_some() {
                            return Err(self.create_error(ParserErrorType::AlreadyHaveGroupBy));
                        }

                        let mut group_by_keys = Vec::new();
                        group_by_keys.push(self.parse_expression_internal()?);
                        while let Token::Comma = self.current() {
                            self.next()?;
                            group_by_keys.push(self.parse_expression_internal()?);
                        }

                        group_by = Some(group_by_keys);
                    }
                    Token::Keyword(Keyword::Having) => {
                        if having.is_some() {
                            return Err(self.create_error(ParserErrorType::AlreadyHaveHaving));
                        }

                        self.next()?;
                        having = Some(self.parse_expression_internal()?);
                    }
                    Token::Keyword(Keyword::Limit) => {
                        if limit.is_some() {
                            return Err(self.create_error(ParserErrorType::AlreadyHaveLimit));
                        }

                        self.next()?;
                        limit = Some(self.consume_int()? as usize);
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
                join,
                limit,
                distinct
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
            Token::DoubleColon,
            ParserErrorType::ExpectedDoubleColon
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
        let location = self.current_location();

        self.next()?;

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
                Token::RightParentheses => {
                    self.next()?;
                    break;
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
                end_location: self.current_location(),
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
        let mut microseconds = None;

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
            Token::Identifier(identifier) if identifier.to_lowercase() == "microseconds" => {
                self.next()?;
                microseconds = Some(true);
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
                microseconds,
                default_value,
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
                Token::DoubleColon => {
                    let convert_to_type = match rhs.tree {
                        ParserExpressionTreeData::ColumnAccess(typename) => {
                            ValueType::from_str(&typename).ok_or_else(|| ParserError::new(op_location.clone(), ParserErrorType::NotDefinedType(typename)))?
                        }
                        _ => { return Err(ParserError::new(op_location, ParserErrorType::ExpectedIdentifier)); }
                    };

                    lhs = ParserExpressionTree::new(op_location, ParserExpressionTreeData::TypeConversion { operand: Box::new(lhs), convert_to_type });
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
            Token::DoubleColon => Ok(7),
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
            Token::Keyword(Keyword::Case) => {
                self.parse_case_expression()
            }
            _ => Err(self.create_error(ParserErrorType::ExpectedExpression))
        }
    }

    fn parse_identifier_expression(&mut self, identifier: String) -> ParserResult<ParserExpressionTree> {
        self.next()?;

        let token_location = self.current_location();
        let mut is_create_array = false;
        match self.current() {
            Token::LeftParentheses => (),
            Token::LeftSquareParentheses if identifier.to_lowercase() == "array" => { is_create_array = true; },
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

        if !is_create_array {
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
        } else {
            match self.current() {
                Token::RightSquareParentheses => (),
                _ => {
                    loop {
                        arguments.push(self.parse_expression_internal()?);
                        match self.current() {
                            Token::RightSquareParentheses => { break; }
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
                    ParserExpressionTreeData::Call { name: "create_array".to_owned(), arguments, distinct }
                )
            )
        }
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

    fn parse_case_expression(&mut self) -> ParserResult<ParserExpressionTree> {
        let token_location = self.current_location();
        self.next()?;

        let mut clauses = Vec::new();
        loop {
            self.expect_and_consume_token(Token::Keyword(Keyword::When), ParserErrorType::ExpectedKeyword(Keyword::When))?;
            let condition = self.parse_expression_internal()?;
            self.expect_and_consume_token(Token::Keyword(Keyword::Then), ParserErrorType::ExpectedKeyword(Keyword::Then))?;
            let result = self.parse_expression_internal()?;

            clauses.push((condition, result));

            if self.current() == &Token::Keyword(Keyword::Else) {
                self.next()?;
                let else_result = self.parse_expression_internal()?;
                self.expect_and_consume_token(Token::Keyword(Keyword::End), ParserErrorType::ExpectedKeyword(Keyword::End))?;

                return Ok(
                    ParserExpressionTree::new(
                        token_location,
                        ParserExpressionTreeData::Case {
                            clauses,
                            else_clause: Box::new(else_result)
                        }
                    )
                );
            }
        }
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

    pub fn next(&mut self) -> ParserResult<&Token> {
        let next_index = self.index + 1;
        if next_index >= self.tokens.len() as isize {
            return Err(self.create_error(ParserErrorType::ReachedEndOfTokens));
        }

        self.index = next_index;
        Ok(&self.tokens[self.index as usize].token)
    }
}

impl std::fmt::Display for ParserOperationTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParserOperationTree::Select { location: _location, projections, from, filter, group_by, having, join, limit, distinct } => {
                let projection_str = projections
                    .iter()
                    .map(|(name, expression)| {
                        if let Some(name) = name {
                            format!("{} AS {}", expression.tree, name)
                        } else {
                            expression.tree.to_string()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");

                let distinct_str = if *distinct {
                    "DISTINCT "
                } else {
                    ""
                };

                write!(f, "SELECT {}{} FROM {}", distinct_str, projection_str, from.0)?;
                if let Some(from_file) = &from.1 {
                    write!(f, "::'{}'", from_file)?;
                }

                if let Some(join) = join {
                    if join.is_outer {
                        write!(f, " OUTER JOIN ")?;
                    } else {
                        write!(f, " INNER JOIN ")?;
                    }

                    write!(f, "{}::'{}'", join.joiner_table, join.joiner_filename)?;
                    write!(f, " ON {}.{} = {}.{}", join.left_table, join.left_column, join.right_table, join.right_column)?;
                }

                if let Some(filter) = filter {
                    write!(f, " WHERE {}", filter.tree)?;
                }

                if let Some(group_by) = group_by {
                    let group_by = group_by.iter().map(|part| part.tree.to_string()).collect::<Vec<_>>();
                    write!(f, " GROUP BY {}", group_by.join(", "))?;
                }

                if let Some(having) = having {
                    write!(f, " HAVING {}", having.tree)?;
                }

                if let Some(limit) = limit {
                    write!(f, " LIMIT {}", limit)?;
                }

                Ok(())
            }
            ParserOperationTree::CreateTable { .. } => {
                unimplemented!();
            }
            ParserOperationTree::Multiple(_) => {
                unimplemented!();
            }
        }
    }
}


impl std::fmt::Display for ParserExpressionTreeData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        ParserExpressionTreeDataVisualizer {}.fmt(self, f)
    }
}

struct ParserExpressionTreeDataVisualizer {

}

impl ParserExpressionTreeDataVisualizer {
    fn fmt(&self, value: &ParserExpressionTreeData, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match value {
            ParserExpressionTreeData::Value(value) => {
                write!(f, "{}", value)
            }
            ParserExpressionTreeData::ColumnAccess(name) => {
                write!(f, "{}", name)
            }
            ParserExpressionTreeData::ScopedColumnAccess(scope, name) => {
                match scope {
                    ColumnScope::Table => write!(f, "{}", name),
                    ColumnScope::AggregationValue => write!(f, "AggregationValue({})", name),
                    ColumnScope::GroupKey => write!(f, "GroupKey({})", name),
                    ColumnScope::GroupValue => write!(f, "GroupValue({})", name)
                }
            }
            ParserExpressionTreeData::Wildcard => {
                write!(f, "*")
            }
            ParserExpressionTreeData::BinaryOperator { operator, left, right } => {
                write!(f, "(")?;
                self.fmt(&left.tree, f)?;
                write!(f, " {} ", operator)?;
                self.fmt(&right.tree, f)?;
                write!(f, ")")?;
                Ok(())
            }
            ParserExpressionTreeData::BooleanOperation { operator, left, right } => {
                write!(f, "(")?;
                self.fmt(&left.tree, f)?;
                write!(f, " {} ", operator)?;
                self.fmt(&right.tree, f)?;
                write!(f, ")")?;
                Ok(())
            }
            ParserExpressionTreeData::UnaryOperator { operator, operand } => {
                write!(f, "{}", operator)?;
                self.fmt(&operand.tree, f)?;
                Ok(())
            }
            ParserExpressionTreeData::Invert { operand } => {
                write!(f, "NOT ")?;
                self.fmt(&operand.tree, f)?;
                Ok(())
            }
            ParserExpressionTreeData::NullableCompare { operator, left, right } => {
                write!(f, "(")?;
                self.fmt(&left.tree, f)?;
                write!(f, " {} ", operator)?;
                self.fmt(&right.tree, f)?;
                write!(f, ")")?;
                Ok(())
            }
            ParserExpressionTreeData::Call { name, arguments, distinct } => {
                write!(f, "{}", name)?;
                write!(f, "(")?;
                let mut is_first = true;
                for argument in arguments {
                    if !is_first {
                        write!(f, ", ")?;
                    } else {
                        is_first = false;
                    }

                    self.fmt(&argument.tree, f)?;
                }

                if let Some(distinct) = distinct {
                    if !is_first {
                        write!(f, ", ")?;
                    }

                    write!(f, "distinct={}", distinct)?;
                }

                write!(f, ")")?;
                Ok(())
            }
            ParserExpressionTreeData::ArrayElementAccess { array, index } => {
                self.fmt(&array.tree, f)?;
                write!(f, "[")?;
                self.fmt(&index.tree, f)?;
                write!(f, "]")?;
                Ok(())
            }
            ParserExpressionTreeData::TypeConversion { operand, convert_to_type } => {
                self.fmt(&operand.tree, f)?;
                write!(f, "::{}", convert_to_type)?;
                Ok(())
            }
            ParserExpressionTreeData::Case { clauses, else_clause } => {
                write!(f, "(CASE ")?;
                for clause in clauses {
                    write!(f, "WHEN ")?;
                    self.fmt(&clause.0.tree, f)?;
                    write!(f, " THEN ")?;
                    self.fmt(&clause.1.tree, f)?;
                    write!(f, " ")?;
                }
                write!(f, "ELSE ")?;
                self.fmt(&else_clause.tree, f)?;
                write!(f, " END)")?;
                Ok(())
            }
        }
    }
}