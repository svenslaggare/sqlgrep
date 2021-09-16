pub mod tokenizer;
pub mod operator;
pub mod parser;
pub mod parse_tree_converter;

use crate::model::Statement;
use crate::parsing::parse_tree_converter::{ConvertParseTreeErrorType, ConvertParseTreeError};
use crate::parsing::tokenizer::{ParserErrorType, ParserError};

#[derive(Debug)]
pub enum CommonParserError {
    ParserError(ParserError),
    ConvertParseTreeError(ConvertParseTreeError)
}

impl std::fmt::Display for CommonParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommonParserError::ParserError(err) => write!(f, "{} ({}:{})", err.error, err.location.line + 1, err.location.column + 1),
            CommonParserError::ConvertParseTreeError(err) => write!(f, "{} ({}:{})", err.error, err.location.line + 1, err.location.column + 1),
        }
    }
}

pub fn parse(line: &str) -> Result<Statement, CommonParserError> {
    let parse_tree = parser::parse_str(&line).map_err(|err| CommonParserError::ParserError(err))?;
    let statement = parse_tree_converter::transform_statement(parse_tree).map_err(|err| CommonParserError::ConvertParseTreeError(err))?;
    Ok(statement)
}