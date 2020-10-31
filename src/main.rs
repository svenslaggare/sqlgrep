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
use crate::parser::{tokenize, Parser, BinaryOperators, UnaryOperators, ParserResult, ParseOperationTree};

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

fn define_table(text: String) -> Option<TableDefinition> {
    let table_definition_tree = parser::parse_str(&text);
    if let Err(err) = table_definition_tree {
        println!("Failed to parse data definition: {}", err);
        return None;
    }
    let table_definition_tree = table_definition_tree.unwrap();

    let create_table_statement = parse_tree_converter::transform_statement(table_definition_tree);
    if let Err(err) = create_table_statement {
        println!("Failed to create table: {}", err);
        return None;
    }
    let create_table_statement = create_table_statement.unwrap();

    let table_definition = create_table_statement.extract_create_table();
    if table_definition.is_none() {
        println!("Expected create table statement.");
        return None;
    }

     table_definition
}

fn main() {
    let mut tables = Tables::new();

    let default_input_filename = "data/test1.log";
    let table_definition_filename = "data/definition1.txt";

    // let default_input_filename = "testdata/test1.log";
    // let table_definition_filename = "testdata/definition1.txt";

    let table_definition_str = std::fs::read_to_string(table_definition_filename).unwrap();
    let table_definition = define_table(table_definition_str);
    if table_definition.is_none() {
        return;
    }

    let table_definition = table_definition.unwrap();

    let table_name = table_definition.name.clone();
    tables.add_table(&table_name, table_definition);

    for line in ReadLinePrompt::new("> ") {
        let parse_tree = parser::parse_str(&line);
        if let Err(err) = parse_tree {
            println!("Failed parsing input: {}", err);
            continue;
        }

        let parse_tree = parse_tree.unwrap();

        let statement = parse_tree_converter::transform_statement(parse_tree);
        if let Err(err) = statement {
            println!("Failed parsing input: {}", err);
            continue;
        }

        let statement = statement.unwrap();
        let filename = statement.filename().unwrap_or(default_input_filename);

        let mut ingester = FileIngester::new(&filename, ProcessEngine::new(&tables)).unwrap();
        if let Err(result) = ingester.process(statement) {
            println!("Execution error: {}", result);
        }
    }
}
