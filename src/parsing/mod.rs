pub mod tokenizer;
pub mod operator;
pub mod parser;
pub mod parser_tree_converter;

#[cfg(test)]
pub mod parser_tests;

#[cfg(test)]
pub mod parser_tree_converter_tests;

use crate::model::Statement;
use crate::parsing::parser_tree_converter::{ConvertParserTreeError};
use crate::parsing::tokenizer::{ParserError, TokenLocation};

#[derive(Debug)]
pub enum CommonParserError {
    ParserError(ParserError),
    ConvertParserTreeError(ConvertParserTreeError)
}

impl CommonParserError {
    pub fn location(&self) -> &TokenLocation {
        match self {
            CommonParserError::ParserError(err) => &err.location,
            CommonParserError::ConvertParserTreeError(err) => &err.location
        }
    }
}

impl std::fmt::Display for CommonParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommonParserError::ParserError(err) => write!(f, "{}", err.error),
            CommonParserError::ConvertParserTreeError(err) => write!(f, "{}", err.error),
        }
    }
}

pub fn parse(line: &str) -> Result<Statement, CommonParserError> {
    let parse_tree = parser::parse_str(&line).map_err(|err| CommonParserError::ParserError(err))?;
    let statement = parser_tree_converter::transform_statement(parse_tree).map_err(|err| CommonParserError::ConvertParserTreeError(err))?;
    Ok(statement)
}