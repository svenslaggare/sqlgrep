use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use structopt::StructOpt;

use rustyline::Editor;

use sqlgrep::data_model::{Tables};
use sqlgrep::model::{Statement};
use sqlgrep::ingest::{FileIngester, FollowFileIngester};
use sqlgrep::execution_engine::ExecutionEngine;
use sqlgrep::parser;
use sqlgrep::parse_tree_converter;

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
    head: bool,
    #[structopt(short, long, help="Executes the given query")]
    command: Option<String>,
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

    match create_table_statement {
        Statement::CreateTable(table_definition) => {
            let table_name = table_definition.name.clone();
            tables.add_table(&table_name, table_definition);
        }
        Statement::Multiple(table_definitions) => {
            for table_definition in table_definitions {
                match table_definition {
                    Statement::CreateTable(table_definition) => {
                        let table_name = table_definition.name.clone();
                        tables.add_table(&table_name, table_definition);
                    }
                    _ => {
                        println!("Expected create table statement.");
                        return false;
                    }
                }
            }
        }
        _ => {
            println!("Expected create table statement.");
            return false;
        }
    }

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

fn execute(command_line_input: &CommandLineInput, tables: &Tables, running: Arc<AtomicBool>, query_line: String, single_result: bool) -> bool {
    if query_line == "exit" {
        return true;
    }

    running.store(true, Ordering::SeqCst);

    match parse_statement(&query_line) {
        Some(statement) => {
            let filename = statement.filename().map(|x| x.to_owned()).or(command_line_input.input_file.clone());
            if filename.is_none() {
                println!("The input filename must be defined.");
                return false;
            }
            let filename = filename.unwrap();

            let result = if command_line_input.follow {
                let ingester = FollowFileIngester::new(
                    running,
                    &filename,
                    command_line_input.head,
                    ExecutionEngine::new(&tables)
                );

                match ingester {
                    Ok(mut ingester) => {
                        ingester.process(statement)
                    }
                    Err(err) => {
                        println!("{}", err);
                        return false;
                    }
                }
            } else {
                let ingester = FileIngester::new(
                    running,
                    &filename,
                    single_result,
                    ExecutionEngine::new(&tables),
                );

                match ingester {
                    Ok(mut ingester) => {
                        ingester.process(statement)
                    }
                    Err(err) => {
                        println!("{}", err);
                        return false;
                    }
                }
            };

            if let Err(err) = result {
                println!("Execution error: {}", err);
            }
        }
        _ => {}
    }

    false
}

fn main() {
    let command_line_input: CommandLineInput = CommandLineInput::from_args();

    let mut tables = Tables::new();
    if let Some(table_definition_filename) = command_line_input.data_definition_file.clone() {
        if !define_table(&mut tables, std::fs::read_to_string(table_definition_filename).unwrap()) {
            return;
        }
    }

    let running = Arc::new(AtomicBool::new(false));
    let running_clone = running.clone();
    ctrlc::set_handler(move || {
        if !running_clone.load(Ordering::SeqCst) {
            std::process::exit(0);
        }

        running_clone.store(false, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    if let Some(command) = command_line_input.command.clone() {
        execute(&command_line_input, &tables, running.clone(), command, true);
    } else {
        let mut line_editor = Editor::<()>::new();
        while let Ok(mut line) = line_editor.readline("> ") {
            if line.ends_with('\n') {
                line.pop();
            }

            line_editor.add_history_entry(line.clone());
            if execute(&command_line_input, &tables, running.clone(), line, false) {
                break;
            }
        }
    }
}
