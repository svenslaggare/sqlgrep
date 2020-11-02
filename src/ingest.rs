use std::io::{BufReader, BufRead, Seek, SeekFrom, Read};
use std::fs::File;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::model::{CompareOperator, Aggregate, AggregateStatement, Statement};
use crate::data_model::{TableDefinition, ColumnDefinition, Tables};
use crate::model::{ValueType, SelectStatement, ExpressionTree, Value};
use crate::execution_engine::{ExecutionEngine};
use crate::execution_model::{ExecutionResult, ResultRow};

pub struct FileIngester<'a> {
    running: Arc<AtomicBool>,
    reader: Option<BufReader<File>>,
    single_result: bool,
    execution_engine: ExecutionEngine<'a>
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
                execution_engine
            }
        )
    }

    pub fn process(&mut self, statement: Statement) -> ExecutionResult<()> {
        let mut print_only_last = false;
        let mut last_result_raw = None;

        for line in self.reader.take().unwrap().lines() {
            if let Ok(line) = line {
                let (result, print_only_last_this) = self.execution_engine.execute(&statement, line);
                print_only_last = print_only_last_this;

                if let Some(result_row) = result? {
                    if !print_only_last {
                        OutputPrinter::new(self.single_result).print(&result_row)
                    }
                    last_result_raw = Some(result_row);
                }
            } else {
                break;
            }

            if !self.running.load(Ordering::SeqCst) {
                break;
            }
        }

        if print_only_last {
            if let Some(result_row) = last_result_raw {
                OutputPrinter::new(true).print(&result_row)
            }
        }

        Ok(())
    }
}

pub struct FollowFileIngester<'a> {
    running: Arc<AtomicBool>,
    reader: Option<BufReader<File>>,
    position: u64,
    execution_engine: ExecutionEngine<'a>
}

impl<'a> FollowFileIngester<'a> {
    pub fn new(running: Arc<AtomicBool>,
               filename: &str,
               head: bool,
               execution_engine: ExecutionEngine<'a>) -> std::io::Result<FollowFileIngester<'a>> {
        let mut reader = BufReader::new(File::open(filename)?);

        let position = if !head {
            reader.seek(SeekFrom::End(0))?
        } else {
            0
        };

        Ok(
            FollowFileIngester {
                running,
                reader: Some(reader),
                position,
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

            // If we get an EOF in the middle of a line, read_line will return. We will then try again and use content of current read line
            if !line.ends_with('\n') {
                continue;
            }

            if line.ends_with('\n') {
                line.pop();
            }

            let mut input_line = String::new();
            std::mem::swap(&mut input_line, &mut line);

            let (result, refresh) = self.execution_engine.execute(&statement, input_line);
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
            ColumnDefinition::new("line", 1, "ip", ValueType::String),
            ColumnDefinition::new("line", 2, "hostname", ValueType::String),
            ColumnDefinition::new("line", 9, "year", ValueType::Int),
            ColumnDefinition::new("line", 4, "month", ValueType::String),
            ColumnDefinition::new("line", 5, "day", ValueType::Int),
            ColumnDefinition::new("line", 6, "hour", ValueType::Int),
            ColumnDefinition::new("line", 7, "minute", ValueType::Int),
            ColumnDefinition::new("line", 8, "second", ValueType::Int),
        ]
    ).unwrap();

    let mut tables = Tables::new();
    tables.add_table("connections", table_definition);

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        "testdata/test1.log",
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
            ColumnDefinition::new("line", 1, "ip", ValueType::String),
            ColumnDefinition::new("line", 2, "hostname", ValueType::String),
            ColumnDefinition::new("line", 9, "year", ValueType::Int),
            ColumnDefinition::new("line", 4, "month", ValueType::String),
            ColumnDefinition::new("line", 5, "day", ValueType::Int),
            ColumnDefinition::new("line", 6, "hour", ValueType::Int),
            ColumnDefinition::new("line", 7, "minute", ValueType::Int),
            ColumnDefinition::new("line", 8, "second", ValueType::Int),
        ]
    ).unwrap();

    let mut tables = Tables::new();
    tables.add_table("connections", table_definition);

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        "testdata/test1.log",
        false,
        ExecutionEngine::new(&tables)
    ).unwrap();

    let result = ingester.process(Statement::Aggregate(AggregateStatement {
        aggregates: vec![
            ("hour".to_owned(), Aggregate::GroupKey("hour".to_owned())),
            ("count".to_owned(), Aggregate::Count),
            ("max_minute".to_owned(), Aggregate::Max(ExpressionTree::ColumnAccess("minute".to_owned()))),
        ],
        from: "connections".to_string(),
        filename: None,
        filter: Some(ExpressionTree::Compare {
            left: Box::new(ExpressionTree::ColumnAccess("day".to_owned())),
            right: Box::new(ExpressionTree::Value(Value::Int(15))),
            operator: CompareOperator::GreaterThanOrEqual
        }),
        group_by: Some("hour".to_owned())
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
            ColumnDefinition::new("line", 1, "ip", ValueType::String),
            ColumnDefinition::new("line", 2, "hostname", ValueType::String),
            ColumnDefinition::new("line", 9, "year", ValueType::Int),
            ColumnDefinition::new("line", 4, "month", ValueType::String),
            ColumnDefinition::new("line", 5, "day", ValueType::Int),
            ColumnDefinition::new("line", 6, "hour", ValueType::Int),
            ColumnDefinition::new("line", 7, "minute", ValueType::Int),
            ColumnDefinition::new("line", 8, "second", ValueType::Int),
        ]
    ).unwrap();

    let mut tables = Tables::new();
    tables.add_table("connections", table_definition);

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        "testdata/test1.log",
        false,
        ExecutionEngine::new(&tables)
    ).unwrap();

    let result = ingester.process(Statement::Aggregate(AggregateStatement {
        aggregates: vec![
            ("count".to_owned(), Aggregate::Count),
            ("max_minute".to_owned(), Aggregate::Max(ExpressionTree::ColumnAccess("minute".to_owned()))),
        ],
        from: "connections".to_string(),
        filename: None,
        filter: Some(ExpressionTree::Compare {
            left: Box::new(ExpressionTree::ColumnAccess("day".to_owned())),
            right: Box::new(ExpressionTree::Value(Value::Int(15))),
            operator: CompareOperator::GreaterThanOrEqual
        }),
        group_by: None
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
            ColumnDefinition::new("line", 1, "ip", ValueType::String),
            ColumnDefinition::new("line", 2, "hostname", ValueType::String),
            ColumnDefinition::new("line", 9, "year", ValueType::Int),
            ColumnDefinition::new("line", 4, "month", ValueType::String),
            ColumnDefinition::new("line", 5, "day", ValueType::Int),
            ColumnDefinition::new("line", 6, "hour", ValueType::Int),
            ColumnDefinition::new("line", 7, "minute", ValueType::Int),
            ColumnDefinition::new("line", 8, "second", ValueType::Int),
        ]
    ).unwrap();

    let mut tables = Tables::new();
    tables.add_table("connections", table_definition);

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        "testdata/test1.log",
        false,
        ExecutionEngine::new(&tables)
    ).unwrap();

    let result = ingester.process(Statement::Aggregate(AggregateStatement {
        aggregates: vec![
            ("hostname".to_owned(), Aggregate::GroupKey("hostname".to_owned())),
            ("count".to_owned(), Aggregate::Count),
            ("last_day".to_owned(), Aggregate::Max(ExpressionTree::ColumnAccess("day".to_owned()))),
        ],
        from: "connections".to_string(),
        filename: None,
        filter: None,
        group_by: Some("hostname".to_owned())
    }));

    if let Err(err) = result {
        println!("{:?}", err);
        assert!(false);
    }
}
