use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::iter::FromIterator;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::execution::{ExecutionError, ExecutionResult, ResultRow};
use crate::execution::execution_engine::{ExecutionConfig, ExecutionEngine};

pub struct ExecutionStatistics {
    execution_start: std::time::Instant,
    pub ingested_bytes: usize,
    pub total_lines: u64,
    pub total_result_rows: u64
}

impl ExecutionStatistics {
    pub fn new() -> ExecutionStatistics {
        ExecutionStatistics {
            execution_start: std::time::Instant::now(),
            ingested_bytes: 0,
            total_lines: 0,
            total_result_rows: 0
        }
    }

    pub fn execution_time(&self) -> f64 {
        (std::time::Instant::now() - self.execution_start).as_micros() as f64 / 1.0E6
    }

    pub fn ingested_megabytes(&self) -> f64 {
        self.ingested_bytes as f64 / 1024.0 / 1024.0
    }
}

#[derive(Clone, PartialEq)]
pub enum OutputFormat {
    Text,
    Json,
    CSV(String)
}

pub struct DisplayOptions {
    pub output_format: OutputFormat,
    pub single_result: bool,
}

impl Default for DisplayOptions {
    fn default() -> Self {
        DisplayOptions {
            output_format: OutputFormat::Text,
            single_result: false
        }
    }
}

pub struct FileExecutor<'a> {
    running: Arc<AtomicBool>,
    readers: Vec<BufReader<File>>,
    execution_engine: ExecutionEngine<'a>,
    display_options: DisplayOptions,
    pub statistics: ExecutionStatistics,
    pub print_result: bool,
    output_printer: OutputPrinter
}

impl<'a> FileExecutor<'a> {
    pub fn new(running: Arc<AtomicBool>,
               files: Vec<File>,
               display_options: DisplayOptions,
               execution_engine: ExecutionEngine<'a>) -> std::io::Result<FileExecutor<'a>> {
        let output_printer = OutputPrinter::new(display_options.output_format.clone());

        Ok(
            FileExecutor {
                running,
                readers: files.into_iter().map(|file| BufReader::new(file)).collect(),
                execution_engine,
                display_options,
                print_result: true,
                statistics: ExecutionStatistics::new(),
                output_printer
            }
        )
    }

    pub fn execute(&mut self) -> ExecutionResult<()> {
        let mut config = ExecutionConfig::default();
        if self.execution_engine.is_aggregate() {
            config.result = false;
        }

        let joined_table_data = self.execution_engine.create_joined_data(self.running.clone())?;

        for reader in std::mem::take(&mut self.readers).into_iter() {
            for line in reader.lines() {
                if !self.running.load(Ordering::SeqCst) {
                    break;
                }

                if let Ok(line) = line {
                    self.statistics.total_lines += 1;
                    self.statistics.ingested_bytes += line.len() + 1; // +1 for line ending

                    let output = self.execution_engine.execute(line, &config, joined_table_data.as_ref())?;
                    if let Some(result_row) = output.row {
                        if self.print_result {
                            self.statistics.total_result_rows += result_row.data.len() as u64;
                            self.output_printer.print(&result_row, self.display_options.single_result);
                        }
                    }

                    if output.reached_limit {
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        if self.execution_engine.is_aggregate() {
            config.result = true;
            config.update = false;

            let result = self.execution_engine.execute(String::new(), &config, joined_table_data.as_ref())?.row;
            if self.print_result {
                if let Some(result_row) = result {
                    self.statistics.total_result_rows += result_row.data.len() as u64;
                    self.output_printer.print(&result_row, true);
                }
            }
        }

        Ok(())
    }
}

pub struct FollowFileExecutor<'a> {
    running: Arc<AtomicBool>,
    reader: Option<BufReader<File>>,
    execution_engine: ExecutionEngine<'a>,
    output_printer: OutputPrinter
}

impl<'a> FollowFileExecutor<'a> {
    pub fn new(running: Arc<AtomicBool>,
               file: File,
               head: bool,
               display_options: DisplayOptions,
               execution_engine: ExecutionEngine<'a>) -> std::io::Result<FollowFileExecutor<'a>> {
        let output_printer = OutputPrinter::new(display_options.output_format.clone());

        let mut reader = BufReader::new(file);

        if head {
            reader.seek(SeekFrom::Start(0))?;
        } else {
            reader.seek(SeekFrom::End(0))?;
        }

        Ok(
            FollowFileExecutor {
                running,
                reader: Some(reader),
                execution_engine,
                output_printer
            }
        )
    }

    pub fn execute(&mut self) -> ExecutionResult<()> {
        let mut reader = self.reader.take().unwrap();
        let mut line = String::new();

        if self.execution_engine.is_join() {
            return Err(ExecutionError::JoinNotSupported);
        }

        loop {
            if !self.running.load(Ordering::SeqCst) {
                break;
            }

            if let Err(_) = reader.read_line(&mut line) {
                break;
            }

            // If we get an EOF in the middle of a line, read_line will return.
            // We will then try again and use content of current read line
            if !line.ends_with('\n') {
                continue;
            }

            if line.ends_with('\n') {
                line.pop();
            }

            let mut input_line = String::new();
            std::mem::swap(&mut input_line, &mut line);

            let output = self.execution_engine.execute(input_line, &ExecutionConfig::default(), None)?;
            if let Some(result_row) = output.row {
                if output.update {
                    print!("\x1B[2J\x1B[1;1H");
                }

                self.output_printer.print(&result_row, output.update);

                if output.reached_limit {
                    break;
                }
            }
        }

        Ok(())
    }
}

struct OutputPrinter {
    format: OutputFormat,
    first_line: bool
}

impl OutputPrinter {
    pub fn new(format: OutputFormat) -> OutputPrinter {
        OutputPrinter {
            format,
            first_line: true
        }
    }

    pub fn print(&mut self, result_row: &ResultRow, single_result: bool) {
        let multiple_rows = result_row.data.len() > 1;
        for row in &result_row.data {
            if row.columns.len() == 1 && result_row.columns[0] == "input" && self.format == OutputFormat::Text {
                println!("{}", row.columns[0]);
            } else {
                match &self.format {
                    OutputFormat::Text => {
                        let columns = result_row.columns
                            .iter()
                            .enumerate()
                            .map(|(projection_index, projection_name)| format!("{}: {}", projection_name, row.columns[projection_index]))
                            .collect::<Vec<_>>();

                        println!("{}", columns.join(", "));
                    }
                    OutputFormat::Json => {
                        let columns = serde_json::Map::from_iter(
                            result_row.columns
                                .iter()
                                .enumerate()
                                .map(|(projection_index, projection_name)| (projection_name.to_owned(), row.columns[projection_index].json_value()))
                        );

                        println!("{}", serde_json::to_string(&columns).unwrap());
                    }
                    OutputFormat::CSV(delimiter) => {
                        if self.first_line {
                            let column_names = result_row.columns
                                .iter()
                                .enumerate()
                                .map(|(_, projection_name)| format!("{}", projection_name))
                                .collect::<Vec<_>>();

                            println!("{}", column_names.join(&delimiter));
                        }

                        let columns = result_row.columns
                            .iter()
                            .enumerate()
                            .map(|(projection_index, _)| format!("{}", row.columns[projection_index]))
                            .collect::<Vec<_>>();

                        println!("{}", columns.join(&delimiter));
                    }
                }
            }
        }

        if !result_row.data.is_empty() {
            self.first_line = false;
        }

        if multiple_rows && !single_result {
            println!();
        }
    }
}

#[test]
fn test_file_executor_select1() {
    use crate::data_model::{ColumnDefinition, TableDefinition, Tables, RegexMode};
    use crate::execution::execution_engine::{ExecutionEngine};
    use crate::model::{ValueType};
    use crate::parsing::parse;

    let table_definition = TableDefinition::new(
        "connections",
        vec![
            ("line", "connection from ([0-9.]+) \\((.+)?\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)", RegexMode::Captures)
        ],
        vec![
            ColumnDefinition::with_regex("line", 1, "ip", ValueType::String),
            ColumnDefinition::with_regex("line", 2, "hostname", ValueType::String),
            ColumnDefinition::with_regex("line", 9, "year", ValueType::Int),
            ColumnDefinition::with_regex("line", 4, "month", ValueType::String),
            ColumnDefinition::with_regex("line", 5, "day", ValueType::Int),
            ColumnDefinition::with_regex("line", 6, "hour", ValueType::Int),
            ColumnDefinition::with_regex("line", 7, "minute", ValueType::Int),
            ColumnDefinition::with_regex("line", 8, "second", ValueType::Int),
        ]
    ).unwrap();

    let mut tables = Tables::new();
    tables.add_table(table_definition);

    let statement = parse("SELECT ip, hostname, year, month, day, hour, minute, second FROM connections WHERE day >= 15").unwrap();

    let mut executor = FileExecutor::new(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        ExecutionEngine::new(&tables, &statement)
    ).unwrap();

    let result = executor.execute();

    if let Err(err) = result {
        println!("{:?}", err);
        assert!(false);
    }
}

#[test]
fn test_file_executor_aggregate1() {
    use crate::data_model::{ColumnDefinition, TableDefinition, Tables, RegexMode};
    use crate::execution::execution_engine::{ExecutionEngine};
    use crate::model::{ValueType};
    use crate::parsing::parse;

    let table_definition = TableDefinition::new(
        "connections",
        vec![
            ("line", "connection from ([0-9.]+) \\((.+)?\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)", RegexMode::Captures)
        ],
        vec![
            ColumnDefinition::with_regex("line", 1, "ip", ValueType::String),
            ColumnDefinition::with_regex("line", 2, "hostname", ValueType::String),
            ColumnDefinition::with_regex("line", 9, "year", ValueType::Int),
            ColumnDefinition::with_regex("line", 4, "month", ValueType::String),
            ColumnDefinition::with_regex("line", 5, "day", ValueType::Int),
            ColumnDefinition::with_regex("line", 6, "hour", ValueType::Int),
            ColumnDefinition::with_regex("line", 7, "minute", ValueType::Int),
            ColumnDefinition::with_regex("line", 8, "second", ValueType::Int),
        ]
    ).unwrap();

    let mut tables = Tables::new();
    tables.add_table(table_definition);

    let statement = parse("SELECT hour, COUNT(*) AS count, MAX(minute) AS max_minute FROM connections WHERE day >= 15 GROUP BY hour").unwrap();

    let mut executor = FileExecutor::new(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        ExecutionEngine::new(&tables, &statement)
    ).unwrap();

    let result = executor.execute();

    if let Err(err) = result {
        println!("{:?}", err);
        assert!(false);
    }
}

#[test]
fn test_file_executor_aggregate2() {
    use crate::data_model::{ColumnDefinition, TableDefinition, Tables, RegexMode};
    use crate::execution::execution_engine::{ExecutionEngine};
    use crate::model::{ValueType};
    use crate::parsing::parse;

    let table_definition = TableDefinition::new(
        "connections",
        vec![
            ("line", "connection from ([0-9.]+) \\((.+)?\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)", RegexMode::Captures)
        ],
        vec![
            ColumnDefinition::with_regex("line", 1, "ip", ValueType::String),
            ColumnDefinition::with_regex("line", 2, "hostname", ValueType::String),
            ColumnDefinition::with_regex("line", 9, "year", ValueType::Int),
            ColumnDefinition::with_regex("line", 4, "month", ValueType::String),
            ColumnDefinition::with_regex("line", 5, "day", ValueType::Int),
            ColumnDefinition::with_regex("line", 6, "hour", ValueType::Int),
            ColumnDefinition::with_regex("line", 7, "minute", ValueType::Int),
            ColumnDefinition::with_regex("line", 8, "second", ValueType::Int),
        ]
    ).unwrap();

    let mut tables = Tables::new();
    tables.add_table(table_definition);

    let statement = parse("SELECT COUNT(*) AS count, MAX(minute) AS max_minute FROM connections WHERE day >= 15").unwrap();

    let mut executor = FileExecutor::new(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        ExecutionEngine::new(&tables, &statement)
    ).unwrap();

    let result = executor.execute();

    if let Err(err) = result {
        println!("{:?}", err);
        assert!(false);
    }
}

#[test]
fn test_file_executor_aggregate3() {
    use crate::data_model::{ColumnDefinition, TableDefinition, Tables, RegexMode};
    use crate::execution::execution_engine::{ExecutionEngine};
    use crate::model::{ValueType};
    use crate::parsing::parse;

    let table_definition = TableDefinition::new(
        "connections",
        vec![
            ("line", "connection from ([0-9.]+) \\((.+)?\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)", RegexMode::Captures)
        ],
        vec![
            ColumnDefinition::with_regex("line", 1, "ip", ValueType::String),
            ColumnDefinition::with_regex("line", 2, "hostname", ValueType::String),
            ColumnDefinition::with_regex("line", 9, "year", ValueType::Int),
            ColumnDefinition::with_regex("line", 4, "month", ValueType::String),
            ColumnDefinition::with_regex("line", 5, "day", ValueType::Int),
            ColumnDefinition::with_regex("line", 6, "hour", ValueType::Int),
            ColumnDefinition::with_regex("line", 7, "minute", ValueType::Int),
            ColumnDefinition::with_regex("line", 8, "second", ValueType::Int),
        ]
    ).unwrap();

    let mut tables = Tables::new();
    tables.add_table(table_definition);

    let statement = parse("SELECT hostname, COUNT(*) AS count, MAX(day) AS last_day FROM connections GROUP BY hostname").unwrap();

    let mut ingester = FileExecutor::new(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        ExecutionEngine::new(&tables, &statement)
    ).unwrap();

    let result = ingester.execute();

    if let Err(err) = result {
        println!("{:?}", err);
        assert!(false);
    }
}

#[test]
fn test_file_executor_aggregate4() {
    use crate::data_model::{ColumnDefinition, TableDefinition, Tables, RegexMode};
    use crate::execution::execution_engine::{ExecutionEngine};
    use crate::model::{ValueType};
    use crate::parsing::parse;

    let table_definition = TableDefinition::new(
        "connections",
        vec![
            ("line", "connection from ([0-9.]+) \\((.+)?\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)", RegexMode::Captures)
        ],
        vec![
            ColumnDefinition::with_regex("line", 1, "ip", ValueType::String),
            ColumnDefinition::with_regex("line", 2, "hostname", ValueType::String),
            ColumnDefinition::with_regex("line", 9, "year", ValueType::Int),
            ColumnDefinition::with_regex("line", 4, "month", ValueType::String),
            ColumnDefinition::with_regex("line", 5, "day", ValueType::Int),
            ColumnDefinition::with_regex("line", 6, "hour", ValueType::Int),
            ColumnDefinition::with_regex("line", 7, "minute", ValueType::Int),
            ColumnDefinition::with_regex("line", 8, "second", ValueType::Int),
        ]
    ).unwrap();

    let mut tables = Tables::new();
    tables.add_table(table_definition);

    let statement = parse(
        "SELECT hostname, hour, COUNT(*) AS count, MAX(minute) AS max_minute FROM connections GROUP BY hostname, hour WHERE day >= 15"
    ).unwrap();

    let mut executor = FileExecutor::new(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        ExecutionEngine::new(&tables, &statement)
    ).unwrap();

    let result = executor.execute();

    if let Err(err) = result {
        println!("{:?}", err);
        assert!(false);
    }
}

#[test]
fn test_file_executor_aggregate5() {
    use crate::data_model::{ColumnDefinition, TableDefinition, Tables, RegexMode};
    use crate::execution::execution_engine::{ExecutionEngine};
    use crate::model::{ValueType};
    use crate::parsing::parse;

    let table_definition = TableDefinition::new(
        "connections",
        vec![
            ("line", "connection from ([0-9.]+) \\((.+)?\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)", RegexMode::Captures)
        ],
        vec![
            ColumnDefinition::with_regex("line", 1, "ip", ValueType::String),
            ColumnDefinition::with_regex("line", 2, "hostname", ValueType::String),
            ColumnDefinition::with_regex("line", 9, "year", ValueType::Int),
            ColumnDefinition::with_regex("line", 4, "month", ValueType::String),
            ColumnDefinition::with_regex("line", 5, "day", ValueType::Int),
            ColumnDefinition::with_regex("line", 6, "hour", ValueType::Int),
            ColumnDefinition::with_regex("line", 7, "minute", ValueType::Int),
            ColumnDefinition::with_regex("line", 8, "second", ValueType::Int),
        ]
    ).unwrap();

    let mut tables = Tables::new();
    tables.add_table(table_definition);

    let statement = parse(
        "SELECT hostname, COUNT(*) AS count, MAX(day) AS last_day FROM connections GROUP BY hostname HAVING hostname IS NOT NULL AND COUNT(*) >= 30"
    ).unwrap();

    let mut executor = FileExecutor::new(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        ExecutionEngine::new(&tables, &statement)
    ).unwrap();

    let result = executor.execute();

    if let Err(err) = result {
        println!("{:?}", err);
        assert!(false);
    }
}