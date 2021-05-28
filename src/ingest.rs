use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::iter::FromIterator;
use std::collections::HashMap;

use crate::data_model::{ColumnDefinition, TableDefinition, Tables};
use crate::execution::{ExecutionResult, ResultRow, ExecutionError};
use crate::execution::execution_engine::{ExecutionConfig, ExecutionEngine, JoinedTableData};
use crate::model::{Aggregate, AggregateStatement, CompareOperator, Statement, JoinClause};
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

pub struct DisplayOptions {
    pub json_output: bool
}

impl Default for DisplayOptions {
    fn default() -> Self {
        DisplayOptions {
            json_output: false
        }
    }
}

pub struct FileIngester<'a> {
    running: Arc<AtomicBool>,
    readers: Vec<BufReader<File>>,
    single_result: bool,
    execution_engine: ExecutionEngine<'a>,
    pub statistics: ExecutionStatistics,
    pub print_result: bool,
    display_options: DisplayOptions
}

impl<'a> FileIngester<'a> {
    pub fn new(running: Arc<AtomicBool>,
               files: Vec<File>,
               single_result: bool,
               display_options: DisplayOptions,
               execution_engine: ExecutionEngine<'a>) -> std::io::Result<FileIngester<'a>> {

        Ok(
            FileIngester {
                running,
                readers: files.into_iter().map(|file| BufReader::new(file)).collect(),
                single_result,
                execution_engine,
                print_result: true,
                statistics: ExecutionStatistics::new(),
                display_options
            }
        )
    }

    pub fn process(&mut self, statement: Statement) -> ExecutionResult<()> {
        let mut config = ExecutionConfig::default();
        if statement.is_aggregate() {
            config.result = false;
        }

        let joined_table_data = self.get_joined_data(statement.join_clause());

        for reader in std::mem::take(&mut self.readers).into_iter() {
            for line in reader.lines() {
                if let Ok(line) = line {
                    self.statistics.total_lines += 1;
                    self.statistics.ingested_bytes += line.len() + 1; // +1 for line ending

                    let (result, _) = self.execution_engine.execute(&statement, line, &config, joined_table_data.as_ref());
                    if let Some(result_row) = result? {
                        if self.print_result {
                            self.statistics.total_result_rows += result_row.data.len() as u64;
                            OutputPrinter::new(self.single_result, self.display_options.json_output).print(&result_row)
                        }
                    }
                } else {
                    break;
                }

                if !self.running.load(Ordering::SeqCst) {
                    break;
                }
            }
        }

        if statement.is_aggregate() {
            config.result = true;
            config.update = false;

            let (result, _) = self.execution_engine.execute(&statement, String::new(), &config, joined_table_data.as_ref());
            if self.print_result {
                if let Some(result_row) = result? {
                    self.statistics.total_result_rows += result_row.data.len() as u64;
                    OutputPrinter::new(true, self.display_options.json_output).print(&result_row)
                }
            }
        }

        Ok(())
    }

    fn get_joined_data(&mut self, join: Option<&JoinClause>) -> Option<JoinedTableData> {
        if let Some(join) = join {
            let join_statement = Statement::Select(SelectStatement {
                projections: vec![("wildcard".to_owned(), ExpressionTree::Wildcard)],
                from: join.joined_table.clone(),
                filename: None,
                filter: None,
                join: None
            });

            let joined_table = self.execution_engine.get_table(&join.joined_table).unwrap().clone();
            let join_on_column_index = joined_table.index_for(&join.joined_column).unwrap();
            let mut joined_table_data = JoinedTableData::new(
                joined_table,
                join_on_column_index,
                HashMap::new()
            );

            let config = ExecutionConfig::default();

            for line in BufReader::new(File::open(&join.joined_filename).unwrap()).lines() {
                let line = line.unwrap();
                let (result, _) = self.execution_engine.execute(&join_statement, line.clone(), &config, None);

                if let Ok(Some(result)) = result {
                    for row in result.data {
                        let join_on_value = row.columns[join_on_column_index].clone();
                        joined_table_data.rows
                            .entry(join_on_value.clone())
                            .or_insert_with(|| Vec::new())
                            .push(row);
                    }
                }
            }

            Some(joined_table_data)
        } else {
            None
        }
    }
}

pub struct FollowFileIngester<'a> {
    running: Arc<AtomicBool>,
    reader: Option<BufReader<File>>,
    execution_engine: ExecutionEngine<'a>,
    display_options: DisplayOptions
}

impl<'a> FollowFileIngester<'a> {
    pub fn new(running: Arc<AtomicBool>,
               file: File,
               head: bool,
               display_options: DisplayOptions,
               execution_engine: ExecutionEngine<'a>) -> std::io::Result<FollowFileIngester<'a>> {
        let mut reader = BufReader::new(file);

        if head {
            reader.seek(SeekFrom::Start(0))?;
        } else {
            reader.seek(SeekFrom::End(0))?;
        }

        Ok(
            FollowFileIngester {
                running,
                reader: Some(reader),
                execution_engine,
                display_options
            }
        )
    }

    pub fn process(&mut self, statement: Statement) -> ExecutionResult<()> {
        let mut reader = self.reader.take().unwrap();
        let mut line = String::new();

        if statement.join_clause().is_some() {
            return Err(ExecutionError::JoinNotSupported);
        }

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

            let (result, refresh) = self.execution_engine.execute(&statement, input_line, &ExecutionConfig::default(), None);
            if let Some(result_row) = result? {
                if refresh {
                    print!("\x1B[2J\x1B[1;1H");
                }

                OutputPrinter::new(refresh, self.display_options.json_output).print(&result_row)
            }

            if !self.running.load(Ordering::SeqCst) {
                break;
            }
        }

        Ok(())
    }
}

struct OutputPrinter {
    single_result: bool,
    json_output: bool
}

impl OutputPrinter {
    pub fn new(single_result: bool, json_output: bool) -> OutputPrinter {
        OutputPrinter {
            single_result,
            json_output
        }
    }

    pub fn print(&self, result_row: &ResultRow) {
        let multiple_rows = result_row.data.len() > 1;
        for row in &result_row.data {
            if row.columns.len() == 1 && result_row.columns[0] == "input" && !self.json_output {
                println!("{}", row.columns[0]);
            } else {
                if self.json_output {
                    let columns = serde_json::Map::from_iter(
                        result_row.columns
                            .iter()
                            .enumerate()
                            .map(|(projection_index, projection_name)| (projection_name.to_owned(), row.columns[projection_index].json_value()))
                    );

                    println!("{}", serde_json::to_string(&columns).unwrap());
                } else {
                    let columns = result_row.columns
                        .iter()
                        .enumerate()
                        .map(|(projection_index, projection_name)| format!("{}: {}", projection_name, row.columns[projection_index]))
                        .collect::<Vec<_>>();

                    println!("{}", columns.join(", "));
                }
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
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        false,
        Default::default(),
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
        }),
        join: None
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
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        false,
        Default::default(),
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
        having: None,
        join: None
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
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        false,
        Default::default(),
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
        having: None,
        join: None
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
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        false,
        Default::default(),
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
        having: None,
        join: None
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
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        false,
        Default::default(),
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
        having: None,
        join: None
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
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        false,
        Default::default(),
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
        ),
        join: None
    }));

    if let Err(err) = result {
        println!("{:?}", err);
        assert!(false);
    }
}