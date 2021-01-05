use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::data_model::{ColumnDefinition, TableDefinition, Tables};
use crate::execution::{ExecutionResult, ResultRow};
use crate::execution::execution_engine::{ExecutionConfig, ExecutionEngine};
use crate::model::{Aggregate, AggregateStatement, CompareOperator, Statement};
use crate::model::{ExpressionTree, SelectStatement, Value, ValueType};

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

pub struct FileIngester<'a> {
    running: Arc<AtomicBool>,
    reader: Option<BufReader<File>>,
    single_result: bool,
    execution_engine: ExecutionEngine<'a>,
    pub statistics: ExecutionStatistics,
    pub print_result: bool
}

impl<'a> FileIngester<'a> {
    pub fn new(running: Arc<AtomicBool>,
               filename: &str,
               single_result: bool,
               execution_engine: ExecutionEngine<'a>) -> std::io::Result<FileIngester<'a>> {
        let reader = BufReader::new(File::open(filename)?);

        Ok(
            FileIngester {
                running,
                reader: Some(reader),
                single_result,
                execution_engine,
                print_result: true,
                statistics: ExecutionStatistics::new()
            }
        )
    }

    pub fn process(&mut self, statement: Statement) -> ExecutionResult<()> {
        let mut config = ExecutionConfig::default();
        if statement.is_aggregate() {
            config.result = false;
        }

        for line in self.reader.take().unwrap().lines() {
            if let Ok(line) = line {
                self.statistics.total_lines += 1;
                self.statistics.ingested_bytes += line.len() + 1; // +1 for line ending

                let (result, _) = self.execution_engine.execute(&statement, line, &config);
                if let Some(result_row) = result? {
                    if self.print_result {
                        self.statistics.total_result_rows += result_row.data.len() as u64;
                        OutputPrinter::new(self.single_result).print(&result_row)
                    }
                }
            } else {
                break;
            }

            if !self.running.load(Ordering::SeqCst) {
                break;
            }
        }

        if statement.is_aggregate() {
            config.result = true;
            config.update = false;

            let (result, _) = self.execution_engine.execute(&statement, String::new(), &config);
            if self.print_result {
                if let Some(result_row) = result? {
                    self.statistics.total_result_rows += result_row.data.len() as u64;
                    OutputPrinter::new(true).print(&result_row)
                }
            }
        }

        Ok(())
    }
}

pub struct FollowFileIngester<'a> {
    running: Arc<AtomicBool>,
    reader: Option<BufReader<File>>,
    execution_engine: ExecutionEngine<'a>
}

impl<'a> FollowFileIngester<'a> {
    pub fn new(running: Arc<AtomicBool>,
               filename: &str,
               head: bool,
               execution_engine: ExecutionEngine<'a>) -> std::io::Result<FollowFileIngester<'a>> {
        let mut reader = BufReader::new(File::open(filename)?);

        if head {
            reader.seek(SeekFrom::Start(0))?;
        } else {
            reader.seek(SeekFrom::End(0))?;
        }

        Ok(
            FollowFileIngester {
                running,
                reader: Some(reader),
                execution_engine
            }
        )
    }

    pub fn process(&mut self, statement: Statement) -> ExecutionResult<()> {
        let mut reader = self.reader.take().unwrap();
        let mut line = String::new();

        loop {
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

            let (result, refresh) = self.execution_engine.execute(&statement, input_line, &ExecutionConfig::default());
            if let Some(result_row) = result? {
                if refresh {
                    print!("\x1B[2J\x1B[1;1H");
                }

                OutputPrinter::new(refresh).print(&result_row)
            }

            if !self.running.load(Ordering::SeqCst) {
                break;
            }
        }

        Ok(())
    }
}

struct OutputPrinter {
    single_result: bool
}

impl OutputPrinter {
    pub fn new(single_result: bool) -> OutputPrinter {
        OutputPrinter {
            single_result
        }
    }

    pub fn print(&self, result_row: &ResultRow) {
        let multiple_rows = result_row.data.len() > 1;
        for row in &result_row.data {
            if row.columns.len() == 1 && result_row.columns[0] == "input" {
                println!("{}", row.columns[0]);
            } else {
                let columns = result_row.columns
                    .iter()
                    .enumerate()
                    .map(|(projection_index, projection_name)| format!("{}: {}", projection_name, row.columns[projection_index]))
                    .collect::<Vec<_>>();

                println!("{}", columns.join(", "));
            }
        }

        if multiple_rows && !self.single_result {
            println!();
        }
    }
}

#[test]
fn test_file_ingest1() {
    let table_definition = TableDefinition::new(
        "connections",
        vec![
            ("line", "connection from ([0-9.]+) \\((.+)?\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)")
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
    tables.add_table("connections", table_definition);

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        "testdata/ftpd_data.txt",
        false,
        ExecutionEngine::new(&tables)
    ).unwrap();

    let result = ingester.process(Statement::Select(SelectStatement {
        projections: vec![
            ("ip".to_owned(), ExpressionTree::ColumnAccess("ip".to_owned())),
            ("hostname".to_owned(), ExpressionTree::ColumnAccess("hostname".to_owned())),
            ("year".to_owned(), ExpressionTree::ColumnAccess("year".to_owned())),
            ("month".to_owned(), ExpressionTree::ColumnAccess("month".to_owned())),
            ("day".to_owned(), ExpressionTree::ColumnAccess("day".to_owned())),
            ("hour".to_owned(), ExpressionTree::ColumnAccess("hour".to_owned())),
            ("minute".to_owned(), ExpressionTree::ColumnAccess("minute".to_owned())),
            ("second".to_owned(), ExpressionTree::ColumnAccess("second".to_owned()))
        ],
        from: "connections".to_string(),
        filename: None,
        filter: Some(ExpressionTree::Compare {
            left: Box::new(ExpressionTree::ColumnAccess("day".to_owned())),
            right: Box::new(ExpressionTree::Value(Value::Int(15))),
            operator: CompareOperator::GreaterThanOrEqual
        })
    }));

    if let Err(err) = result {
        println!("{:?}", err);
        assert!(false);
    }
}

#[test]
fn test_file_ingest2() {
    let table_definition = TableDefinition::new(
        "connections",
        vec![
            ("line", "connection from ([0-9.]+) \\((.+)?\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)")
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
    tables.add_table("connections", table_definition);

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        "testdata/ftpd_data.txt",
        false,
        ExecutionEngine::new(&tables)
    ).unwrap();

    let result = ingester.process(Statement::Aggregate(AggregateStatement {
        aggregates: vec![
            ("hour".to_owned(), Aggregate::GroupKey("hour".to_owned())),
            ("count".to_owned(), Aggregate::Count(None)),
            ("max_minute".to_owned(), Aggregate::Max(ExpressionTree::ColumnAccess("minute".to_owned()))),
        ],
        from: "connections".to_string(),
        filename: None,
        filter: Some(ExpressionTree::Compare {
            left: Box::new(ExpressionTree::ColumnAccess("day".to_owned())),
            right: Box::new(ExpressionTree::Value(Value::Int(15))),
            operator: CompareOperator::GreaterThanOrEqual
        }),
        group_by: Some(vec!["hour".to_owned()]),
        having: None
    }));

    if let Err(err) = result {
        println!("{:?}", err);
        assert!(false);
    }
}

#[test]
fn test_file_ingest3() {
    let table_definition = TableDefinition::new(
        "connections",
        vec![
            ("line", "connection from ([0-9.]+) \\((.+)?\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)")
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
    tables.add_table("connections", table_definition);

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        "testdata/ftpd_data.txt",
        false,
        ExecutionEngine::new(&tables)
    ).unwrap();

    let result = ingester.process(Statement::Aggregate(AggregateStatement {
        aggregates: vec![
            ("count".to_owned(), Aggregate::Count(None)),
            ("max_minute".to_owned(), Aggregate::Max(ExpressionTree::ColumnAccess("minute".to_owned()))),
        ],
        from: "connections".to_string(),
        filename: None,
        filter: Some(ExpressionTree::Compare {
            left: Box::new(ExpressionTree::ColumnAccess("day".to_owned())),
            right: Box::new(ExpressionTree::Value(Value::Int(15))),
            operator: CompareOperator::GreaterThanOrEqual
        }),
        group_by: None,
        having: None
    }));

    if let Err(err) = result {
        println!("{:?}", err);
        assert!(false);
    }
}

#[test]
fn test_file_ingest4() {
    let table_definition = TableDefinition::new(
        "connections",
        vec![
            ("line", "connection from ([0-9.]+) \\((.+)?\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)")
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
    tables.add_table("connections", table_definition);

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        "testdata/ftpd_data.txt",
        false,
        ExecutionEngine::new(&tables)
    ).unwrap();

    let result = ingester.process(Statement::Aggregate(AggregateStatement {
        aggregates: vec![
            ("hostname".to_owned(), Aggregate::GroupKey("hostname".to_owned())),
            ("count".to_owned(), Aggregate::Count(None)),
            ("last_day".to_owned(), Aggregate::Max(ExpressionTree::ColumnAccess("day".to_owned()))),
        ],
        from: "connections".to_string(),
        filename: None,
        filter: None,
        group_by: Some(vec!["hostname".to_owned()]),
        having: None
    }));

    if let Err(err) = result {
        println!("{:?}", err);
        assert!(false);
    }
}

#[test]
fn test_file_ingest5() {
    let table_definition = TableDefinition::new(
        "connections",
        vec![
            ("line", "connection from ([0-9.]+) \\((.+)?\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)")
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
    tables.add_table("connections", table_definition);

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        "testdata/ftpd_data.txt",
        false,
        ExecutionEngine::new(&tables)
    ).unwrap();

    let result = ingester.process(Statement::Aggregate(AggregateStatement {
        aggregates: vec![
            ("hostname".to_owned(), Aggregate::GroupKey("hostname".to_owned())),
            ("hour".to_owned(), Aggregate::GroupKey("hour".to_owned())),
            ("count".to_owned(), Aggregate::Count(None)),
            ("max_minute".to_owned(), Aggregate::Max(ExpressionTree::ColumnAccess("minute".to_owned()))),
        ],
        from: "connections".to_string(),
        filename: None,
        filter: Some(ExpressionTree::Compare {
            left: Box::new(ExpressionTree::ColumnAccess("day".to_owned())),
            right: Box::new(ExpressionTree::Value(Value::Int(15))),
            operator: CompareOperator::GreaterThanOrEqual
        }),
        group_by: Some(vec!["hostname".to_owned(), "hour".to_owned()]),
        having: None
    }));

    if let Err(err) = result {
        println!("{:?}", err);
        assert!(false);
    }
}

#[test]
fn test_file_ingest6() {
    let table_definition = TableDefinition::new(
        "connections",
        vec![
            ("line", "connection from ([0-9.]+) \\((.+)?\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)")
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
    tables.add_table("connections", table_definition);

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        "testdata/ftpd_data.txt",
        false,
        ExecutionEngine::new(&tables)
    ).unwrap();

    let result = ingester.process(Statement::Aggregate(AggregateStatement {
        aggregates: vec![
            ("hostname".to_owned(), Aggregate::GroupKey("hostname".to_owned())),
            ("count".to_owned(), Aggregate::Count(None)),
            ("last_day".to_owned(), Aggregate::Max(ExpressionTree::ColumnAccess("day".to_owned()))),
        ],
        from: "connections".to_string(),
        filename: None,
        filter: None,
        group_by: Some(vec!["hostname".to_owned()]),
        having: Some(
            ExpressionTree::And {
                left: Box::new(ExpressionTree::IsNot {
                    left: Box::new(ExpressionTree::Aggregate(0, Box::new(Aggregate::GroupKey("hostname".to_owned())))),
                    right: Box::new(ExpressionTree::Value(Value::Null))
                }),
                right: Box::new(ExpressionTree::Compare {
                    operator: CompareOperator::GreaterThan,
                    left: Box::new(ExpressionTree::Aggregate(1, Box::new(Aggregate::Count(None)))),
                    right: Box::new(ExpressionTree::Value(Value::Int(30)))
                })
            }
        )
    }));

    if let Err(err) = result {
        println!("{:?}", err);
        assert!(false);
    }
}