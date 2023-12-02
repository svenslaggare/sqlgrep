use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::iter::FromIterator;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::execution::{ExecutionError, ExecutionResult, ResultRow};
use crate::execution::execution_engine::{ExecutionConfig, ExecutionEngine};
use crate::helpers::FollowFileIterator;

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
        (std::time::Instant::now() - self.execution_start).as_secs_f64()
    }

    pub fn ingested_megabytes(&self) -> f64 {
        self.ingested_bytes as f64 / 1024.0 / 1024.0
    }
}

pub struct FileExecutor<'a, TPrinter: Printer = ConsolePrinter> {
    running: Arc<AtomicBool>,
    readers: Vec<BufReader<File>>,
    execution_engine: ExecutionEngine<'a>,
    display_options: DisplayOptions,
    statistics: ExecutionStatistics,
    output_printer: OutputPrinter<TPrinter>
}

impl<'a, TPrinter: Printer> FileExecutor<'a, TPrinter> {
    pub fn with_output_printer(running: Arc<AtomicBool>,
                               files: Vec<File>,
                               display_options: DisplayOptions,
                               printer: TPrinter,
                               execution_engine: ExecutionEngine<'a>) -> std::io::Result<FileExecutor<'a, TPrinter>> {
        let output_printer = OutputPrinter::<TPrinter>::with_printer(printer, display_options.output_format.clone());

        Ok(
            FileExecutor {
                running,
                readers: files.into_iter().map(|file| BufReader::new(file)).collect(),
                execution_engine,
                display_options,
                statistics: ExecutionStatistics::new(),
                output_printer
            }
        )
    }

    pub fn statistics(&self) -> &ExecutionStatistics {
        &self.statistics
    }

    pub fn output_printer(&self) -> &OutputPrinter<TPrinter> {
        &self.output_printer
    }

    pub fn execute(&mut self) -> ExecutionResult<()> {
        let config = self.execution_engine.execution_config();
        self.execution_engine.execute_joined_table(self.running.clone())?;

        for reader in std::mem::take(&mut self.readers).into_iter() {
            for line in reader.lines() {
                if !self.running.load(Ordering::SeqCst) {
                    break;
                }

                if let Ok(line) = line {
                    self.statistics.total_lines += 1;
                    self.statistics.ingested_bytes += line.len() + 1; // +1 for line ending

                    let output = self.execution_engine.execute(line, &config)?;
                    if let Some(result_row) = output.result_row {
                        if self.display_options.print_result {
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
            let result_row = self.execution_engine.execute(
                String::new(),
                &ExecutionConfig::aggregate_result()
            )?.result_row;

            if self.display_options.print_result {
                if let Some(result_row) = result_row {
                    self.statistics.total_result_rows += result_row.data.len() as u64;
                    self.output_printer.print(&result_row, true);
                }
            }
        }

        Ok(())
    }
}

impl<'a> FileExecutor<'a, ConsolePrinter> {
    pub fn new(running: Arc<AtomicBool>,
               files: Vec<File>,
               display_options: DisplayOptions,
               execution_engine: ExecutionEngine<'a>) -> std::io::Result<FileExecutor<'a, ConsolePrinter>> {
        FileExecutor::with_output_printer(
            running,
            files,
            display_options,
            ConsolePrinter::default(),
            execution_engine
        )
    }
}

pub struct DisplayOptions {
    pub output_format: OutputFormat,
    pub single_result: bool,
    pub print_result: bool
}

impl Default for DisplayOptions {
    fn default() -> Self {
        DisplayOptions {
            output_format: OutputFormat::Text,
            single_result: false,
            print_result: true
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OutputFormat {
    Text,
    Json,
    CSV(String)
}

impl FromStr for OutputFormat {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "text" => Ok(OutputFormat::Text),
            "json" => Ok(OutputFormat::Json),
            "csv" => Ok(OutputFormat::CSV(";".to_owned())),
            other => Err(format!("'{}' is not a defined output format.", other))
        }
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
        if self.execution_engine.is_join() {
            return Err(ExecutionError::JoinNotSupported);
        }

        for input_line in FollowFileIterator::new(self.reader.take().unwrap()) {
            if !self.running.load(Ordering::SeqCst) {
                break;
            }

            let output = self.execution_engine.execute(input_line, &ExecutionConfig::default())?;
            if let Some(result_row) = output.result_row {
                if output.updated {
                    print!("\x1B[2J\x1B[1;1H");
                }

                self.output_printer.print(&result_row, output.updated);

                if output.reached_limit {
                    break;
                }
            }
        }

        Ok(())
    }
}

pub struct OutputPrinter<T: Printer = ConsolePrinter> {
    printer: T,
    format: OutputFormat,
    first_line: bool
}

impl<T: Printer> OutputPrinter<T> {
    pub fn with_printer(printer: T, format: OutputFormat) -> OutputPrinter<T> {
        OutputPrinter {
            printer,
            format,
            first_line: true
        }
    }

    pub fn printer(&self) -> &T {
        &self.printer
    }

    pub fn print(&mut self, result_row: &ResultRow, single_result: bool) {
        let multiple_rows = result_row.data.len() > 1;
        for row in &result_row.data {
            if row.columns.len() == 1 && result_row.columns[0] == "input" && self.format == OutputFormat::Text {
                self.printer.println(&format!("{}", row.columns[0]));
            } else {
                match &self.format {
                    OutputFormat::Text => {
                        let columns = result_row.columns
                            .iter()
                            .enumerate()
                            .map(|(projection_index, projection_name)| format!("{}: {}", projection_name, row.columns[projection_index]))
                            .collect::<Vec<_>>();

                        self.printer.println(&format!("{}", columns.join(", ")));
                    }
                    OutputFormat::Json => {
                        let columns = serde_json::Map::from_iter(
                            result_row.columns
                                .iter()
                                .enumerate()
                                .map(|(projection_index, projection_name)| (projection_name.to_owned(), row.columns[projection_index].json_value()))
                        );

                        self.printer.println(&format!("{}", serde_json::to_string(&columns).unwrap()));
                    }
                    OutputFormat::CSV(delimiter) => {
                        if self.first_line {
                            let column_names = result_row.columns
                                .iter()
                                .enumerate()
                                .map(|(_, projection_name)| format!("{}", projection_name))
                                .collect::<Vec<_>>();

                            self.printer.println(&format!("{}", column_names.join(&delimiter)));
                        }

                        let columns = result_row.columns
                            .iter()
                            .enumerate()
                            .map(|(projection_index, _)| format!("{}", row.columns[projection_index]))
                            .collect::<Vec<_>>();

                        self.printer.println(&format!("{}", columns.join(&delimiter)));
                    }
                }
            }
        }

        if !result_row.data.is_empty() {
            self.first_line = false;
        }

        if multiple_rows && !single_result {
            self.printer.println("");
        }
    }
}

impl<T: Printer + Default> OutputPrinter<T> {
    pub fn new(format: OutputFormat) -> OutputPrinter<T> {
        OutputPrinter::with_printer(T::default(), format)
    }
}

pub trait Printer {
    fn println(&mut self, line: &str);
}

pub struct ConsolePrinter {}
impl Default for ConsolePrinter {
    fn default() -> Self {
        ConsolePrinter {}
    }
}

impl Printer for ConsolePrinter {
    fn println(&mut self, line: &str) {
        println!("{}", line);
    }
}

#[test]
fn test_file_executor_select1() {
    use crate::data_model::{ColumnDefinition, TableDefinition, Tables, RegexMode};
    use crate::execution::execution_engine::{ExecutionEngine};
    use crate::model::{ValueType};
    use crate::parsing::parse;
    use crate::integration_tests::CapturedPrinter;

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

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &statement)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(584, executor.statistics().total_result_rows);

    assert_eq!("ip: '24.54.76.216', hostname: '24-54-76-216.bflony.adelphia.net', year: 2005, month: 'Jun', day: 17, hour: 7, minute: 7, second: 0", executor.output_printer().printer().lines()[0]);
    assert_eq!("ip: '207.30.238.8', hostname: 'host8.topspot.net', year: 2005, month: 'Jul', day: 17, hour: 12, minute: 31, second: 0", executor.output_printer().printer().lines()[292]);
    assert_eq!("ip: '218.38.58.3', hostname: NULL, year: 2005, month: 'Jul', day: 27, hour: 10, minute: 59, second: 53", executor.output_printer().printer().lines()[583]);
}

#[test]
fn test_file_executor_aggregate1() {
    use crate::data_model::{ColumnDefinition, TableDefinition, Tables, RegexMode};
    use crate::execution::execution_engine::{ExecutionEngine};
    use crate::model::{ValueType};
    use crate::parsing::parse;
    use crate::integration_tests::CapturedPrinter;

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

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &statement)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(18, executor.statistics().total_result_rows);

    assert_eq!("hour: 2, count: 38, max_minute: 38", executor.output_printer().printer().lines()[0]);
    assert_eq!("hour: 3, count: 56, max_minute: 40", executor.output_printer().printer().lines()[1]);
    assert_eq!("hour: 4, count: 2, max_minute: 6", executor.output_printer().printer().lines()[2]);
    assert_eq!("hour: 5, count: 23, max_minute: 47", executor.output_printer().printer().lines()[3]);
    assert_eq!("hour: 6, count: 53, max_minute: 39", executor.output_printer().printer().lines()[4]);
    assert_eq!("hour: 7, count: 8, max_minute: 7", executor.output_printer().printer().lines()[5]);
    assert_eq!("hour: 8, count: 46, max_minute: 14", executor.output_printer().printer().lines()[6]);
    assert_eq!("hour: 9, count: 73, max_minute: 44", executor.output_printer().printer().lines()[7]);
    assert_eq!("hour: 10, count: 24, max_minute: 59", executor.output_printer().printer().lines()[8]);
    assert_eq!("hour: 12, count: 23, max_minute: 31", executor.output_printer().printer().lines()[9]);
    assert_eq!("hour: 13, count: 42, max_minute: 46", executor.output_printer().printer().lines()[10]);
    assert_eq!("hour: 14, count: 30, max_minute: 44", executor.output_printer().printer().lines()[11]);
    assert_eq!("hour: 15, count: 23, max_minute: 9", executor.output_printer().printer().lines()[12]);
    assert_eq!("hour: 18, count: 13, max_minute: 55", executor.output_printer().printer().lines()[13]);
    assert_eq!("hour: 19, count: 36, max_minute: 29", executor.output_printer().printer().lines()[14]);
    assert_eq!("hour: 20, count: 7, max_minute: 55", executor.output_printer().printer().lines()[15]);
    assert_eq!("hour: 21, count: 23, max_minute: 23", executor.output_printer().printer().lines()[16]);
    assert_eq!("hour: 23, count: 64, max_minute: 42", executor.output_printer().printer().lines()[17]);
}

#[test]
fn test_file_executor_aggregate2() {
    use crate::data_model::{ColumnDefinition, TableDefinition, Tables, RegexMode};
    use crate::execution::execution_engine::{ExecutionEngine};
    use crate::model::{ValueType};
    use crate::parsing::parse;
    use crate::integration_tests::CapturedPrinter;

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

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &statement)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(1, executor.statistics().total_result_rows);

    assert_eq!("count: 584, max_minute: 59", executor.output_printer().printer().lines()[0]);
}

#[test]
fn test_file_executor_aggregate3() {
    use crate::data_model::{ColumnDefinition, TableDefinition, Tables, RegexMode};
    use crate::execution::execution_engine::{ExecutionEngine};
    use crate::model::{ValueType};
    use crate::parsing::parse;
    use crate::integration_tests::CapturedPrinter;

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

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &statement)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(9, executor.statistics().total_result_rows);

    assert_eq!("hostname: NULL, count: 453, last_day: 29", executor.output_printer().printer().lines()[0]);
    assert_eq!("hostname: '24-54-76-216.bflony.adelphia.net', count: 8, last_day: 17", executor.output_printer().printer().lines()[1]);
    assert_eq!("hostname: '82-68-222-194.dsl.in-addr.zen.co.uk', count: 23, last_day: 17", executor.output_printer().printer().lines()[2]);
    assert_eq!("hostname: '82-68-222-195.dsl.in-addr.zen.co.uk', count: 23, last_day: 17", executor.output_printer().printer().lines()[3]);
    assert_eq!("hostname: 'aml-sfh-3310b.adsl.wanadoo.nl', count: 32, last_day: 17", executor.output_printer().printer().lines()[4]);
    assert_eq!("hostname: 'dsl-082-083-227-067.arcor-ip.net', count: 23, last_day: 10", executor.output_printer().printer().lines()[5]);
    assert_eq!("hostname: 'dsl-Chn-static-059.45.101.203.touchtelindia.net', count: 23, last_day: 17", executor.output_printer().printer().lines()[6]);
    assert_eq!("hostname: 'host8.topspot.net', count: 46, last_day: 17", executor.output_printer().printer().lines()[7]);
    assert_eq!("hostname: 'lns-vlq-45-tou-82-252-162-81.adsl.proxad.net', count: 22, last_day: 18", executor.output_printer().printer().lines()[8]);
}

#[test]
fn test_file_executor_aggregate4() {
    use crate::data_model::{ColumnDefinition, TableDefinition, Tables, RegexMode};
    use crate::execution::execution_engine::{ExecutionEngine};
    use crate::model::{ValueType};
    use crate::parsing::parse;
    use crate::integration_tests::CapturedPrinter;

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

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &statement)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(22, executor.statistics().total_result_rows);

    assert_eq!("hostname: NULL, hour: 2, count: 23, max_minute: 38", executor.output_printer().printer().lines()[0]);
    assert_eq!("hostname: NULL, hour: 19, count: 36, max_minute: 29", executor.output_printer().printer().lines()[10]);
    assert_eq!("hostname: 'lns-vlq-45-tou-82-252-162-81.adsl.proxad.net', hour: 20, count: 7, max_minute: 55", executor.output_printer().printer().lines()[21]);
}

#[test]
fn test_file_executor_aggregate5() {
    use crate::data_model::{ColumnDefinition, TableDefinition, Tables, RegexMode};
    use crate::execution::execution_engine::{ExecutionEngine};
    use crate::model::{ValueType};
    use crate::parsing::parse;
    use crate::integration_tests::CapturedPrinter;

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

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &statement)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(2, executor.statistics().total_result_rows);

    assert_eq!("hostname: 'aml-sfh-3310b.adsl.wanadoo.nl', count: 32, last_day: 17", executor.output_printer().printer().lines()[0]);
    assert_eq!("hostname: 'host8.topspot.net', count: 46, last_day: 17", executor.output_printer().printer().lines()[1]);
}