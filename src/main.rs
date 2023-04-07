use std::cell::{RefCell};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::fs::File;
use std::iter::FromIterator;
use std::rc::Rc;
use cursive::theme::{BaseColor, Color};

use structopt::StructOpt;

use rustyline::{Editor, Context};
use rustyline::validate::{Validator, ValidationContext, ValidationResult};
use rustyline::error::ReadlineError;
use rustyline_derive::{Helper, Highlighter, Hinter};
use rustyline::completion::{Completer, Pair};

use cursive::traits::*;
use cursive::utils::markup::StyledString;
use cursive::views::{Button, LinearLayout, ListView, NamedView, Panel, SelectView, TextArea, TextView};

use sqlgrep::{parsing, Tables, Statement, ExecutionEngine};
use sqlgrep::data_model::TableDefinition;
use sqlgrep::ingest::{FileIngester, FollowFileIngester, DisplayOptions, OutputFormat};
use sqlgrep::parsing::tokenizer::keywords_list;
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
    format: String,
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
            main_table_editor(command_line_input, table_name);
        }
    }
}

fn main_normal(command_line_input: CommandLineInput) {
    let mut tables = Tables::new();
    if let Some(table_definition_filename) = command_line_input.data_definition_file.clone() {
        let table_definition_content = std::fs::read_to_string(table_definition_filename).expect("Failed to read data definition file.");

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
        let mut line_editor = Editor::new();
        line_editor.set_helper(Some(InputValidator::new(&tables)));

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

fn main_table_editor(mut command_line_input: CommandLineInput, table_name: String) {
    let table_definition_filename = command_line_input.data_definition_file.take().expect("The data definition file must be specified.");
    let table_definition_content = std::fs::read_to_string(&table_definition_filename).expect("Failed to read data definition file.");

    let input_filename = command_line_input.input_file.get(0).expect("The input file must be specified.");
    let input_file_content = std::fs::read_to_string(input_filename).expect("Failed to read input file.");

    let mut tables = Tables::new();
    if let Err(err) = define_table(&mut tables, table_definition_content.clone()) {
        println!("{}", err);
        std::process::exit(1);
    }

    let table_editor = TableEditor::new(
        tables,
        table_name,
        table_definition_filename,
        table_definition_content,
        input_file_content.lines().take(1000).map(|line| line.to_owned()).collect::<Vec<_>>()
    );
    table_editor.verify_table_exists();

    let table_editor = Rc::new(RefCell::new(table_editor));

    let table_editor_extract_data = table_editor.clone();
    let table_editor_update_table = table_editor.clone();
    let table_editor_edit_table = table_editor.clone();
    let table_editor_save_table = table_editor.clone();

    let mut siv = cursive::default();
    let width = 140;

    siv.add_layer(
        LinearLayout::vertical()
            .child(
                Panel::new(
                    SelectView::new()
                        .with_all(table_editor.borrow().input_lines.iter().enumerate().map(|(i, line)| (line.to_owned(), i)))
                        .on_select(move |siv, line_index| {
                            siv.call_on_name("extracted_data", |view: &mut TextView| {
                                let table_editor = table_editor_extract_data.borrow();
                                view.set_content(table_editor.extract_columns_for_line(*line_index).join(", "))
                            });
                        })
                        .with_name("input_lines")
                        .scrollable()
                        .max_height(10)
                )
                    .title("Input lines")
                    .fixed_width(width)
            )
            .child(
                Panel::new(
                    ListView::new()
                        .child(
                            "",
                            TextArea::new()
                                .content(table_editor.borrow().table_definition_content.clone())
                                .with_name("edit_table")
                                .max_height(30)
                        )
                        .child(
                            "",
                            TextView::new("")
                                .with_name("compile_error")
                        )
                        .child(
                            "",
                            LinearLayout::horizontal()
                                .child(
                                    Button::new(
                                        "Update",
                                        move |siv| {
                                            let mut table_editor = table_editor_update_table.borrow_mut();

                                            let edit_table_view = siv.find_name::<TextArea>("edit_table").unwrap();
                                            match define_table(&mut table_editor.tables, edit_table_view.get_content().to_owned()) {
                                                Ok(()) => {
                                                    let input_lines_view = siv.find_name::<SelectView<usize>>("input_lines").unwrap();
                                                    let line_index = input_lines_view.selection().map(|selection| *selection).unwrap_or(0);

                                                    let mut extracted_data_view = siv.find_name::<TextView>("extracted_data").unwrap();
                                                    extracted_data_view.set_content(table_editor.extract_columns_for_line(line_index).join(", "));

                                                    siv.call_on_name("compile_error", |view: &mut TextView| {
                                                        view.set_content("")
                                                    });
                                                }
                                                Err(err) => {
                                                    siv.call_on_name("compile_error", |view: &mut TextView| {
                                                        view.set_content(StyledString::styled(err, Color::Light(BaseColor::Red)))
                                                    });
                                                }
                                            }
                                        }
                                    )
                                )
                                .child(TextView::new(" "))
                                .child(
                                    Button::new(
                                        "Restore",
                                        move |siv| {
                                            siv.call_on_name("edit_table", |view: &mut NamedView<TextArea>| {
                                                view.get_mut().set_content(table_editor_edit_table.borrow().table_definition_content.clone())
                                            });
                                        }
                                    )
                                )
                                .child(TextView::new(" "))
                                .child(
                                    Button::new(
                                        "Save",
                                        move |siv| {
                                            let edit_table_view = siv.find_name::<TextArea>("edit_table").unwrap();
                                            table_editor_save_table.borrow_mut().save_table(edit_table_view.get_content());
                                        }
                                    )
                                )
                        )
                )
                    .title("Table")
                    .fixed_width(width)
            )
            .child(
                Panel::new(TextView::new("").with_name("extracted_data"))
                    .title("Extracted data")
                    .fixed_width(width)
            )
    );

    siv.run();
}

struct TableEditor {
    tables: Tables,
    table_name: String,
    table_definition_filename: String,
    table_definition_content: String,
    input_lines: Vec<String>
}

impl TableEditor {
    pub fn new(tables: Tables,
               table_name: String,
               table_definition_filename: String,
               table_definition_content: String,
               input_lines: Vec<String>) -> TableEditor {
        TableEditor {
            tables,
            table_name,
            table_definition_filename,
            table_definition_content,
            input_lines
        }
    }

    pub fn verify_table_exists(&self) {
        if self.tables.get(&self.table_name).is_none() {
            println!("Table '{}' not found.", self.table_name);
            std::process::exit(1);
        }
    }

    pub fn table(&self) -> &TableDefinition {
        self.tables.get(&self.table_name).unwrap()
    }

    pub fn extract_columns_for_line(&self, line_index: usize) -> Vec<String> {
        let table = self.table();
        let row = table.extract(&self.input_lines[line_index]);
        if row.columns.is_empty() {
            return Vec::new();
        }

        table.columns
            .iter()
            .enumerate()
            .map(|(index, column)| format!("{}: {}", column.name, row.columns[index]))
            .collect::<Vec<_>>()
    }

    pub fn save_table(&mut self, content: &str) {
        if let Err(err) = std::fs::write(&self.table_definition_filename, content) {
            println!("Failed to save file due to: {}", err);
            std::process::exit(1);
        }

        self.table_definition_content = content.to_owned();
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
        let parts = query_line.split(" ").collect::<Vec<_>>();
        if let Some(table) = parts.get(1) {
            let table = tables.get(table).unwrap();
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
            let mut table_printer = TablePrinter::new(vec!["Table".to_owned()]);
            for table in tables.tables() {
                table_printer.add_row(vec![
                    table.name.clone()
                ])
            }

            table_printer.print();
        }

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
    let mut completion_words = keywords_list(true);
    completion_words.push("COUNT".to_owned());
    completion_words.push("SUM".to_owned());
    completion_words.push("MAX".to_owned());
    completion_words.push("MIN".to_owned());
    completion_words.push("ARRAY_AGG".to_owned());

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