mod model;
mod execution_model;
mod expression_execution;
mod data_model;
mod select_execution;
mod aggregate_execution;
mod parser;
mod parse_tree_converter;
mod execution_engine;
mod ingest;

use std::io::Write;

use structopt::StructOpt;

use crate::data_model::{TableDefinition, ColumnDefinition, Tables};
use crate::model::{ValueType, Statement};
use crate::ingest::{FileIngester, FollowFileIngester};
use crate::execution_engine::ExecutionEngine;
use crate::parser::{tokenize, Parser, BinaryOperators, UnaryOperators, ParserResult, ParseOperationTree};

#[derive(Debug, StructOpt)]
#[structopt(name="sqlgrep", about="sqlgrep")]
struct CommandLineInput {
    #[structopt(name="input_filename", help="The input file")]
    input_file: Option<String>,
    #[structopt(short, long("data-file"), help="The data definition file")]
    data_definition_file: Option<String>,
    #[structopt(short("f"), long("follow"), help="Follows the input file")]
    follow: bool,
    #[structopt(long, help="Starts following the file from the start instead of the end")]
    head: bool
}

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

fn define_table(tables: &mut Tables, text: String) -> bool {
    let table_definition_tree = parser::parse_str(&text);
    if let Err(err) = table_definition_tree {
        println!("Failed to parse data definition: {}", err);
        return false;
    }
    let table_definition_tree = table_definition_tree.unwrap();

    let create_table_statement = parse_tree_converter::transform_statement(table_definition_tree);
    if let Err(err) = create_table_statement {
        println!("Failed to create table: {}", err);
        return false;
    }
    let create_table_statement = create_table_statement.unwrap();

    let table_definition = create_table_statement.extract_create_table();
    if table_definition.is_none() {
        println!("Expected create table statement.");
        return false;
    }

    let table_definition = table_definition.unwrap();

    let table_name = table_definition.name.clone();
    tables.add_table(&table_name, table_definition);
    true
}

fn parse_statement(line: &str) -> Option<Statement> {
    let parse_tree = parser::parse_str(&line);
    if let Err(err) = parse_tree {
        println!("Failed parsing input: {}", err);
        return None;
    }

    let parse_tree = parse_tree.unwrap();

    let statement = parse_tree_converter::transform_statement(parse_tree);
    if let Err(err) = statement {
        println!("Failed parsing input: {}", err);
        return None;
    }

    statement.ok()
}

fn main() {
    let command_line_input: CommandLineInput = CommandLineInput::from_args();

    let mut tables = Tables::new();
    if let Some(table_definition_filename) = command_line_input.data_definition_file {
        if !define_table(&mut tables, std::fs::read_to_string(table_definition_filename).unwrap()) {
            return;
        }
    }

    let default_input_filename = command_line_input.input_file;

    for line in ReadLinePrompt::new("> ") {
        match parse_statement(&line) {
            Some(statement) => {
                let filename = statement.filename().map(|x| x.to_owned()).or(default_input_filename.clone()).unwrap();

                let result = if command_line_input.follow {
                    let mut ingester = FollowFileIngester::new(
                        &filename,
                        command_line_input.head,
                        ExecutionEngine::new(&tables)
                    ).unwrap();
                    ingester.process(statement)
                } else {
                    let mut ingester = FileIngester::new(&filename, ExecutionEngine::new(&tables)).unwrap();
                    ingester.process(statement)
                };

                if let Err(err) = result {
                    println!("Execution error: {}", err);
                }
            }
            _ => {}
        }
    }
}
