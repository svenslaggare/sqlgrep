use std::io::{BufReader, BufRead};
use std::fs::File;

use crate::model::CompareOperator;
use crate::data_model::{TableDefinition, ColumnDefinition, Tables};
use crate::model::{ValueType, SelectStatement, ExpressionTree, Value};
use crate::select_execution::{ SelectExecutionResult};
use crate::process_engine::ProcessEngine;

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

    pub fn process_select(&mut self, select_statement: SelectStatement) -> SelectExecutionResult<()> {
        let table_definition = self.process_engine.get_table(&select_statement.from)?;

        for line in self.reader.take().unwrap().lines() {
            if let Ok(line) = line {
                let line_copy = line.clone();
                if let Some(result_row) = self.process_engine.process_select(table_definition, &select_statement, line)? {
                    println!("{}", line_copy);
                    for (projection_index, (projection_name, _)) in select_statement.projection.iter().enumerate() {
                        println!("\t * {}: {}", projection_name, result_row.columns[projection_index]);
                    }
                }
            } else {
                 break;
            }
        }

        Ok(())
    }
}

#[test]
fn test_ingest() {
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
        projection: vec![
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
            operator: CompareOperator::Equal
        })
    });

    if let Err(err) = result {
        println!("{:?}", err);
    }
}