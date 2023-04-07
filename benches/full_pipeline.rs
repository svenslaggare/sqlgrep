use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::fs::File;

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use sqlgrep::data_model::{TableDefinition, ColumnDefinition, Tables, RegexMode};
use sqlgrep::model::{ValueType, SelectStatement, ExpressionTree, CompareOperator, Value, Statement, AggregateStatement, Aggregate};
use sqlgrep::executor::{FileExecutor, DisplayOptions};
use sqlgrep::execution::execution_engine::ExecutionEngine;

fn filtering() {
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
    tables.add_table("connections", table_definition);

    let mut ingester = FileExecutor::new(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data_large.txt").unwrap()],
        false,
        DisplayOptions::default(),
        ExecutionEngine::new(&tables)
    ).unwrap();
    ingester.print_result = false;

    let result = ingester.execute(Statement::Select(SelectStatement {
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

fn aggregate() {
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
    tables.add_table("connections", table_definition);

    let mut ingester = FileExecutor::new(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data_large.txt").unwrap()],
        false,
        DisplayOptions::default(),
        ExecutionEngine::new(&tables)
    ).unwrap();
    ingester.print_result = false;

    let result = ingester.execute(Statement::Aggregate(AggregateStatement {
        aggregates: vec![
            ("hour".to_owned(), Aggregate::GroupKey("hour".to_owned()), None),
            ("count".to_owned(), Aggregate::Count(None), None),
            ("max_minute".to_owned(), Aggregate::Max(ExpressionTree::ColumnAccess("minute".to_owned())), None),
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

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("filtering", |b| b.iter(|| filtering()));
    c.bench_function("aggregate", |b| b.iter(|| aggregate()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);