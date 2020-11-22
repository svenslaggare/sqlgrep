use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::{parser, parse_tree_converter};
use crate::data_model::{Tables, TableDefinition};
use crate::ingest::FileIngester;
use crate::execution_engine::ExecutionEngine;

fn load_table_definition(filename: &str) -> TableDefinition {
    let table_definition_tree = parser::parse_str(&std::fs::read_to_string(filename).unwrap()).unwrap();
    let table_definition_tree = parse_tree_converter::transform_statement(table_definition_tree).unwrap();
    table_definition_tree.extract_create_table().unwrap()
}

fn create_tables(filename: &str) -> Tables {
    let table_definition = load_table_definition(filename);

    let mut tables = Tables::new();
    let table_name = table_definition.name.clone();
    tables.add_table(&table_name, table_definition);

    tables
}

#[test]
fn test_ssh1() {
    let tables = create_tables("testdata/ssh_failure.txt");

    let query_tree = parser::parse_str("SELECT * FROM ssh").unwrap();
    let query = parse_tree_converter::transform_statement(query_tree).unwrap();

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        "testdata/ssh_data.txt",
        false,
        ExecutionEngine::new(&tables)
    ).unwrap();

    ingester.process(query).unwrap();
    assert_eq!(386, ingester.statistics.total_result_rows);
}

#[test]
fn test_ssh2() {
    let tables = create_tables("testdata/ssh_failure.txt");

    let query_tree = parser::parse_str("SELECT hostname, COUNT() AS count FROM ssh GROUP BY hostname").unwrap();
    let query = parse_tree_converter::transform_statement(query_tree).unwrap();

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        "testdata/ssh_data.txt",
        false,
        ExecutionEngine::new(&tables)
    ).unwrap();

    ingester.process(query).unwrap();
    assert_eq!(14, ingester.statistics.total_result_rows);
}

#[test]
fn test_ftpd1() {
    let tables = create_tables("testdata/ftpd.txt");

    let query_tree = parser::parse_str("SELECT * FROM connections WHERE hostname IS NOT NULL").unwrap();
    let query = parse_tree_converter::transform_statement(query_tree).unwrap();

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        "testdata/ftpd_data.txt",
        false,
        ExecutionEngine::new(&tables)
    ).unwrap();

    ingester.process(query).unwrap();
    assert_eq!(200, ingester.statistics.total_result_rows);
}

#[test]
fn test_ftpd2() {
    let tables = create_tables("testdata/ftpd.txt");

    let query_tree = parser::parse_str("SELECT hostname, COUNT() FROM connections WHERE hostname IS NOT NULL GROUP BY hostname").unwrap();
    let query = parse_tree_converter::transform_statement(query_tree).unwrap();

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        "testdata/ftpd_data.txt",
        false,
        ExecutionEngine::new(&tables)
    ).unwrap();

    ingester.process(query).unwrap();
    assert_eq!(8, ingester.statistics.total_result_rows);
}

#[test]
fn test_ftpd3() {
    let tables = create_tables("testdata/ftpd.txt");

    let query_tree = parser::parse_str("SELECT hostname, COUNT() FROM connections WHERE hostname IS NOT NULL HAVING COUNT() > 22 GROUP BY hostname").unwrap();
    let query = parse_tree_converter::transform_statement(query_tree).unwrap();

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        "testdata/ftpd_data.txt",
        false,
        ExecutionEngine::new(&tables)
    ).unwrap();

    ingester.process(query).unwrap();
    assert_eq!(6, ingester.statistics.total_result_rows);
}

#[test]
fn test_ftpd4() {
    let tables = create_tables("testdata/ftpd.txt");

    let query_tree = parser::parse_str("SELECT hostname, COUNT(hostname) FROM connections GROUP BY hostname").unwrap();
    let query = parse_tree_converter::transform_statement(query_tree).unwrap();

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        "testdata/ftpd_data.txt",
        false,
        ExecutionEngine::new(&tables)
    ).unwrap();

    ingester.process(query).unwrap();
    assert_eq!(8, ingester.statistics.total_result_rows);
}