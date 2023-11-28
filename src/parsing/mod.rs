mod parser;

mod operator;
mod tokenizer;
mod parser_tree_converter;

#[cfg(test)]
mod parser_tests;

#[cfg(test)]
mod parser_tree_converter_tests;

pub use crate::parsing::tokenizer::TokenLocation;
pub use crate::parsing::parser::{ParserOperationTree, ParserResult};

use crate::model::Statement;
use crate::parsing::parser_tree_converter::{ConvertParserTreeError};
use crate::parsing::tokenizer::{ParserError};

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

pub fn parse(text: &str) -> Result<Statement, CommonParserError> {
    let parse_tree = parser::parse_str(text).map_err(|err| CommonParserError::ParserError(err))?;
    let statement = parser_tree_converter::transform_statement(parse_tree).map_err(|err| CommonParserError::ConvertParserTreeError(err))?;
    Ok(statement)
}

pub fn parse_into_tree(text: &str) -> ParserResult<ParserOperationTree> {
    parser::parse_str(text)
}

pub fn completion_words() -> Vec<String> {
    let mut completion_words = tokenizer::keywords_list(true);
    completion_words.extend(parser_tree_converter::completion_words());
    completion_words
}