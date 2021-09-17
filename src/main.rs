use std::io::{Write, Error};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::fs::File;

use structopt::StructOpt;

use rustyline::Editor;
use rustyline::validate::{Validator, ValidationContext, ValidationResult};
use rustyline::error::ReadlineError;
use rustyline_derive::{Completer, Helper, Highlighter, Hinter};

use sqlgrep::data_model::{Tables};
use sqlgrep::model::{Statement};
use sqlgrep::ingest::{FileIngester, FollowFileIngester, DisplayOptions, OutputFormat};
use sqlgrep::execution::execution_engine::ExecutionEngine;
use sqlgrep::parsing::parser_tree_converter;
use sqlgrep::parsing;

#[derive(Debug, StructOpt)]
#[structopt(name="sqlgrep", about="sqlgrep")]
struct CommandLineInput {
    #[structopt(name="input_filename", help="The input file")]
    input_file: Vec<String>,
    #[structopt(short, long("data-file"), help="The data definition file")]
    data_definition_file: Option<String>,
    #[structopt(short("f"), long("follow"), help="Follows the input file")]
    follow: bool,
    #[structopt(long, help="Starts following the file from the start instead of the end")]
    head: bool,
    #[structopt(short, long, help="Executes the given query")]
    command: Option<String>,
    #[structopt(long, help="Displays the execution statistics of queries")]
    show_run_stats: bool,
    #[structopt(long, help="The input data is given on stdin")]
    stdin: bool,
    #[structopt(long, help="The output format. Supported values: text, json, csv.", default_value="text")]
    format: String
}

#[derive(Completer, Helper, Highlighter, Hinter)]
struct InputValidator {}

impl Validator for InputValidator {
    fn validate(&self, ctx: &mut ValidationContext) -> Result<ValidationResult, ReadlineError> {
        let input = ctx.input();
        if !input.ends_with(';') {
            Ok(ValidationResult::Incomplete)
        } else {
            Ok(ValidationResult::Valid(None))
        }
    }
}

fn define_table(tables: &mut Tables, text: String) -> bool {
    let create_table_statement = parsing::parse(&text);
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
    match parsing::parse(line) {
        Ok(statement) => Some(statement),
        Err(err) => {
            let near_text = err.location().extract_near(line);

            if !near_text.is_empty() {
                println!("Failed parsing input: {} (near `{}`).", err, near_text);
            } else {
                println!("Failed parsing input: {}.", err);
            }

            None
        }
    }
}

fn execute(command_line_input: &CommandLineInput,
           tables: &mut Tables,
           running: Arc<AtomicBool>,
           query_line: String,
           single_result: bool) -> bool {
    if query_line == "exit" {
        return true;
    }

    running.store(true, Ordering::SeqCst);

    match parse_statement(&query_line) {
        Some(Statement::CreateTable(table_definition)) => {
            let table_name = table_definition.name.clone();
            tables.add_table(&table_name, table_definition);
            return false;
        },
        Some(statement) => {
            let mut files = if !command_line_input.stdin {
                let filenames = match statement.filename() {
                    Some(filename) => vec![filename.to_owned()],
                    None => command_line_input.input_file.clone()
                };

                if filenames.is_empty() {
                    println!("The input filename must be defined.");
                    return false;
                }

                let mut files = Vec::new();
                for filename in filenames {
                    match File::open(filename) {
                        Ok(file) => {
                            files.push(file);
                        }
                        Err(err) => {
                            println!("{}", err);
                            return false;
                        }
                    }
                }

                files
            } else {
                vec![File::open("/dev/stdin").unwrap()]
            };

            let mut display_options: DisplayOptions = Default::default();
            match command_line_input.format.as_str() {
                "text" => { display_options.output_format = OutputFormat::Text; },
                "json" => { display_options.output_format = OutputFormat::Json; },
                "csv" => { display_options.output_format = OutputFormat::CSV(";".to_owned()); }
                format => { panic!("Unsupported output format '{}'.", format) }
            }

            let result = if command_line_input.follow {
                let ingester = FollowFileIngester::new(
                    running,
                    files.remove(0),
                    command_line_input.head,
                    display_options,
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
                    files,
                    single_result,
                    display_options,
                    ExecutionEngine::new(&tables),
                );

                match ingester {
                    Ok(mut ingester) => {
                        let result = ingester.process(statement);
                        if command_line_input.show_run_stats {
                            println!(
                                "Executed query in {:.2} seconds, ingested {:.2} MB, processed {} lines.",
                                ingester.statistics.execution_time(),
                                ingester.statistics.ingested_megabytes(),
                                ingester.statistics.total_lines
                            );
                        }
                        result
                    }
                    Err(err) => {
                        println!("{}", err);
                        return false;
                    }
                }
            };

            if let Err(err) = result {
                println!("Execution error: {}.", err);
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
        execute(&command_line_input, &mut tables, running.clone(), command, true);
    } else {
        let mut line_editor = Editor::new();
        let validator = InputValidator {};
        line_editor.set_helper(Some(validator));

        while let Ok(mut line) = line_editor.readline("> ") {
            if line.ends_with('\n') {
                line.pop();
            }

            line_editor.add_history_entry(line.clone());
            if execute(&command_line_input, &mut tables, running.clone(), line, false) {
                break;
            }
        }
    }
}
