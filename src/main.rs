use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::fs::File;
use std::iter::FromIterator;

use structopt::StructOpt;

use rustyline::{Editor, Context};
use rustyline::validate::{Validator, ValidationContext, ValidationResult};
use rustyline::error::ReadlineError;
use rustyline_derive::{Helper, Highlighter, Hinter};
use rustyline::completion::{Completer, Pair};

use sqlgrep::{parsing, Tables, Statement, ExecutionEngine};
use sqlgrep::executor::{FileExecutor, FollowFileExecutor, DisplayOptions, OutputFormat};
use sqlgrep::helpers::TablePrinter;

#[derive(Debug, StructOpt)]
#[structopt(name="sqlgrep", about="sqlgrep")]
struct CommandLineInput {
    #[structopt(name="input_filename", help="The input file.")]
    input_file: Vec<String>,
    #[structopt(short, long("data-file"), help="The data definition file.")]
    data_definition_file: Option<String>,
    #[structopt(short("f"), long("follow"), help="Follows the input file.")]
    follow: bool,
    #[structopt(long, help="Starts following the file from the start instead of the end.")]
    head: bool,
    #[structopt(short, long, help="Executes the given query.")]
    command: Option<String>,
    #[structopt(long, help="Displays the execution statistics of queries.")]
    show_run_stats: bool,
    #[structopt(long, help="The input data is given on stdin.")]
    stdin: bool,
    #[structopt(long, help="The output format. Supported values: text, json, csv.", default_value="text")]
    format: OutputFormat,
    #[structopt(long, help="Edit the given table.")]
    edit_table: Option<String>
}

fn main() {
    let mut command_line_input: CommandLineInput = CommandLineInput::from_args();

    match command_line_input.edit_table.take() {
        None => {
            main_normal(command_line_input);
        }
        Some(table_name) => {
            #[cfg(unix)]
            sqlgrep::table_editor::run(
                command_line_input.data_definition_file,
                command_line_input.input_file,
                table_name
            );

            #[cfg(not(unix))]
            panic!("Table editor only supported on Unix.");
        }
    }
}

fn main_normal(command_line_input: CommandLineInput) {
    let mut tables = Tables::new();
    if let Some(table_definition_filename) = command_line_input.data_definition_file.clone() {
        let table_definition_content = std::fs::read_to_string(
            table_definition_filename
        ).expect("Failed to read data definition file.");

        if let Err(err) = define_table(&mut tables, table_definition_content.clone()) {
            println!("{}", err);
            std::process::exit(1);
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
        let mut line_editor = Editor::new().unwrap();
        line_editor.set_helper(Some(InputValidator::new(&tables)));

        while let Ok(mut line) = line_editor.readline("> ") {
            if line.ends_with('\n') {
                line.pop();
            }

            line_editor.add_history_entry(line.clone()).unwrap();
            if execute(&command_line_input, &mut tables, running.clone(), line, false) {
                break;
            }
        }
    }
}

fn define_table(tables: &mut Tables, text: String) -> Result<(), String> {
    match parsing::parse(&text) {
        Ok(create_table_statement) => {
            if tables.add_tables(create_table_statement) {
                Ok(())
            } else {
                Err("Expected CREATE TABLE statement.".to_owned())
            }
        }
        Err(err) => {
            Err(format!("Failed to create table: {}.", err))
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

    if query_line.starts_with("\\d") {
        display_table(tables, &query_line);
        return false;
    }

    running.store(true, Ordering::SeqCst);

    match parse_statement(&query_line) {
        Some(Statement::CreateTable(table_definition)) => {
            tables.add_table(table_definition);
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
                vec![File::open("/dev/stdin").expect("Failed to open standard input.")]
            };

            let mut display_options: DisplayOptions = Default::default();
            display_options.output_format = command_line_input.format.clone();

            let result = if command_line_input.follow {
                let executor = FollowFileExecutor::new(
                    running,
                    files.remove(0),
                    command_line_input.head,
                    display_options,
                    ExecutionEngine::new(&tables, &statement)
                );

                match executor {
                    Ok(mut executor) => {
                        executor.execute()
                    }
                    Err(err) => {
                        println!("{}", err);
                        return false;
                    }
                }
            } else {
                display_options.single_result = single_result;

                let executor = FileExecutor::new(
                    running,
                    files,
                    display_options,
                    ExecutionEngine::new(&tables, &statement),
                );

                match executor {
                    Ok(mut executor) => {
                        let result = executor.execute();

                        if command_line_input.show_run_stats {
                            println!(
                                "Executed query in {:.2} seconds, ingested {:.2} MB, processed {} lines.",
                                executor.statistics.execution_time(),
                                executor.statistics.ingested_megabytes(),
                                executor.statistics.total_lines
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

fn display_table(tables: &Tables, query_line: &str) {
    let query_parts = query_line.split(" ").collect::<Vec<_>>();
    if let Some(table_name) = query_parts.get(1) {
        if let Some(table) = tables.get(table_name) {
            let mut table_printer = TablePrinter::new(vec![
                "Column".to_owned(),
                "Type".to_owned(),
                "Nullable".to_owned(),
                "Default value".to_owned()
            ]);

            for column in &table.columns {
                table_printer.add_row(vec![
                    column.name.clone(),
                    column.column_type.to_string(),
                    column.options.nullable.to_string(),
                    column.default_value().to_string()
                ])
            }

            table_printer.print();
        } else {
            println!("'{}' is not a defined table.", table_name)
        }
    } else {
        let mut table_printer = TablePrinter::new(vec!["Table".to_owned()]);
        for table in tables.tables() {
            table_printer.add_row(vec![
                table.name.clone()
            ])
        }

        table_printer.print();
    }
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

#[derive(Helper, Highlighter, Hinter)]
struct InputValidator {
    completion_words: Vec<String>
}

impl InputValidator {
    pub fn new(tables: &Tables) -> InputValidator {
        InputValidator {
            completion_words: create_completion_words(tables)
        }
    }
}

fn create_completion_words(tables: &Tables) -> Vec<String> {
    let mut completion_words = parsing::completion_words();

    for table in tables.tables() {
        completion_words.push(table.name.clone());

        for column in &table.columns {
            completion_words.push(column.name.clone());
        }
    }

    completion_words
}

impl Validator for InputValidator {
    fn validate(&self, ctx: &mut ValidationContext) -> Result<ValidationResult, ReadlineError> {
        let input = ctx.input();
        if input == "exit" || input.starts_with("\\d") {
            return Ok(ValidationResult::Valid(None));
        }

        if !input.ends_with(';') {
            Ok(ValidationResult::Incomplete)
        } else {
            Ok(ValidationResult::Valid(None))
        }
    }
}

impl Completer for InputValidator {
    type Candidate = Pair;

    fn complete(&self, line: &str, pos: usize, _ctx: &Context<'_>) -> Result<(usize, Vec<Pair>), ReadlineError> {
        let mut current_word = Vec::new();
        for char in line.chars().rev() {
            if char.is_whitespace() {
                break;
            }

            current_word.push(char);
        }

        let current_word_length = current_word.len();
        let current_word = String::from_iter(current_word.into_iter().rev());
        let current_word_uppercase = current_word.to_uppercase();

        let mut results = Vec::new();
        for completion in &self.completion_words {
            if completion.starts_with(&current_word_uppercase) {
                results.push(Pair { display: completion.to_owned(), replacement: completion.to_owned() });
            } else if completion.starts_with(&current_word) {
                results.push(Pair { display: completion.to_owned(), replacement: completion.to_owned() });
            }
        }

        Ok((pos - current_word_length, results))
    }
}