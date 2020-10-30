use std::io::{BufReader, BufRead};
use std::fs::File;

use crate::model::{CompareOperator, Aggregate, AggregateStatement, Statement};
use crate::data_model::{TableDefinition, ColumnDefinition, Tables};
use crate::model::{ValueType, SelectStatement, ExpressionTree, Value};
use crate::process_engine::{ProcessEngine};
use crate::execution_model::{ExecutionResult, ResultRow};

pub struct FileIngester<'a> {
    reader: Option<BufReader<File>>,
    process_engine: ProcessEngine<'a>
}

impl<'a> FileIngester<'a> {
    pub fn new(filename: &str, process_engine: ProcessEngine<'a>) -> std::io::Result<FileIngester<'a>> {
        let reader = BufReader::new(File::open(filename)?);

        Ok(
            FileIngester {
                reader: Some(reader),
                process_engine
            }
        )
    }

    pub fn process(&mut self, statement: Statement) -> ExecutionResult<()> {
        for line in self.reader.take().unwrap().lines() {
            if let Ok(line) = line {
                let result = match &statement {
                    Statement::Select(select_statement) => {
                        self.process_engine.process_select(&select_statement, line)
                    }
                    Statement::Aggregate(aggregate_statement) => {
                        self.process_engine.process_aggregate(&aggregate_statement, line)
                    }
                };

                if let Some(result_row) = result? {
                    self.print_result(result_row);
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    pub fn process_select(&mut self, select_statement: SelectStatement) -> ExecutionResult<()> {
        self.process(Statement::Select(select_statement))
    }

    pub fn process_aggregate(&mut self, aggregate_statement: AggregateStatement) -> ExecutionResult<()> {
        self.process(Statement::Aggregate(aggregate_statement))
    }

    fn print_result(&self, result_row: ResultRow) {
        let multiple_rows = result_row.data.len() > 1;
        for row in result_row.data {
            let columns = result_row.columns
                .iter()
                .enumerate()
                .map(|(projection_index, projection_name)| format!("{}: {}", projection_name, row.columns[projection_index]))
                .collect::<Vec<_>>();

            println!("{}", columns.join(", "));
        }

        if multiple_rows {
            println!();
        }
    }
}

#[test]
fn test_ingest1() {
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

    let mut ingester = FileIngester::new("testdata/test1.log", ProcessEngine::new(&tables)).unwrap();
    let result = ingester.process_select(SelectStatement {
        projections: vec![
            // ("input".to_owned(), ExpressionTree::ColumnAccess("input".to_owned())),
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
        filter: Some(ExpressionTree::Compare {
            left: Box::new(ExpressionTree::ColumnAccess("day".to_owned())),
            right: Box::new(ExpressionTree::Value(Value::Int(15))),
            operator: CompareOperator::GreaterThanOrEqual
        })
    });

    if let Err(err) = result {
        println!("{:?}", err);
        assert!(false);
    }
}

#[test]
fn test_ingest2() {
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

    let mut ingester = FileIngester::new("testdata/test1.log", ProcessEngine::new(&tables)).unwrap();

    let result = ingester.process_aggregate(AggregateStatement {
        aggregates: vec![
            ("hour".to_owned(), Aggregate::GroupKey),
            ("count".to_owned(), Aggregate::Count),
            ("max_minute".to_owned(), Aggregate::Max(ExpressionTree::ColumnAccess("minute".to_owned()))),
        ],
        from: "connections".to_string(),
        filter: Some(ExpressionTree::Compare {
            left: Box::new(ExpressionTree::ColumnAccess("day".to_owned())),
            right: Box::new(ExpressionTree::Value(Value::Int(15))),
            operator: CompareOperator::GreaterThanOrEqual
        }),
        group_by: Some("hour".to_owned())
    });

    if let Err(err) = result {
        println!("{:?}", err);
        assert!(false);
    }
}

#[test]
fn test_ingest3() {
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

    let mut ingester = FileIngester::new("testdata/test1.log", ProcessEngine::new(&tables)).unwrap();
    let result = ingester.process_aggregate(AggregateStatement {
        aggregates: vec![
            ("count".to_owned(), Aggregate::Count),
            ("max_minute".to_owned(), Aggregate::Max(ExpressionTree::ColumnAccess("minute".to_owned()))),
        ],
        from: "connections".to_string(),
        filter: Some(ExpressionTree::Compare {
            left: Box::new(ExpressionTree::ColumnAccess("day".to_owned())),
            right: Box::new(ExpressionTree::Value(Value::Int(15))),
            operator: CompareOperator::GreaterThanOrEqual
        }),
        group_by: None
    });

    if let Err(err) = result {
        println!("{:?}", err);
        assert!(false);
    }
}

#[test]
fn test_ingest4() {
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

    let mut ingester = FileIngester::new("testdata/test1.log", ProcessEngine::new(&tables)).unwrap();
    let result = ingester.process_aggregate(AggregateStatement {
        aggregates: vec![
            ("hostname".to_owned(), Aggregate::GroupKey),
            ("count".to_owned(), Aggregate::Count),
            ("last_day".to_owned(), Aggregate::Max(ExpressionTree::ColumnAccess("day".to_owned()))),
        ],
        from: "connections".to_string(),
        filter: None,
        group_by: Some("hostname".to_owned())
    });

    if let Err(err) = result {
        println!("{:?}", err);
        assert!(false);
    }
}
