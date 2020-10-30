mod model;
mod execution_model;
mod expression_execution;
mod data_model;
mod select_execution;
mod aggregate_execution;
mod parser;
mod parse_tree_converter;
mod process_engine;
mod ingest;

use std::io::Write;

use crate::data_model::{TableDefinition, ColumnDefinition, Tables};
use crate::model::ValueType;
use crate::ingest::FileIngester;
use crate::process_engine::ProcessEngine;
use crate::parser::{ParserError, tokenize, Parser, BinaryOperators, UnaryOperators, ParserResult, ParseOperationTree};

struct ReadLinePrompt {
    prompt: String
}

impl ReadLinePrompt {
    fn new(prompt: &str) -> ReadLinePrompt {
        ReadLinePrompt {
            prompt: prompt.to_string()
        }
    }
}

impl std::iter::Iterator for ReadLinePrompt {
    type Item = String;

    fn next(&mut self) -> Option<String> {
        let mut line = String::new();

        print!("{}", self.prompt);
        std::io::stdout().flush().unwrap();

        match std::io::stdin().read_line(&mut line) {
            Ok(_) => Some(line.trim().to_string()),
            Err(_) => None
        }
    }
}

fn parse_statement(line: String) -> ParserResult<ParseOperationTree> {
    let binary_operators = BinaryOperators::new();
    let unary_operators = UnaryOperators::new();

    let mut parser = Parser::new(
        &binary_operators,
        &unary_operators,
        tokenize(&line)?
    );

    parser.parse()
}

fn main() {
    let table_definition = TableDefinition::new(
        "connections",
        vec![
            ("line", "connection from ([0-9.]+) \\((.+)?\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)")
        ],
        vec![
            ColumnDefinition::new("line", 1, "ip", ValueType::String),
            ColumnDefinition::new("line", 2, "hostname", ValueType::String),
            ColumnDefinition::new("line", 9, "year", ValueType::Int),
            ColumnDefinition::new("line", 4, "month", ValueType::String),
            ColumnDefinition::new("line", 5, "day", ValueType::Int),
            ColumnDefinition::new("line", 6, "hour", ValueType::Int),
            ColumnDefinition::new("line", 7, "minute", ValueType::Int),
            ColumnDefinition::new("line", 8, "second", ValueType::Int),
        ]
    ).unwrap();

    let mut tables = Tables::new();
    tables.add_table("connections", table_definition);

    for line in ReadLinePrompt::new("> ") {
        let parse_tree = parse_statement(line);
        if let Err(err) = parse_tree {
            println!("{}", err);
            continue;
        }

        let parse_tree = parse_tree.unwrap();

        let statement = parse_tree_converter::transform_statement(parse_tree);
        if let Err(err) = statement {
            println!("{}", err);
            continue;
        }

        let statement = statement.unwrap();
        let mut ingester = FileIngester::new("testdata/test1.log", ProcessEngine::new(&tables)).unwrap();
        if let Err(result) = ingester.process(statement) {
            println!("{}", result);
        }
    }
}
