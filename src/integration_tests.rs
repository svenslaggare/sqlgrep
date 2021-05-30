use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::fs::File;

use crate::data_model::{TableDefinition, Tables};
use crate::execution::execution_engine::ExecutionEngine;
use crate::ingest::FileIngester;
use crate::parsing::{parse_tree_converter, parser};
use crate::model::Statement;

fn create_tables(filename: &str) -> Tables {
    let mut tables = Tables::new();

    let table_definition_tree = parser::parse_str(&std::fs::read_to_string(filename).unwrap()).unwrap();
    let table_definition_tree = parse_tree_converter::transform_statement(table_definition_tree).unwrap();
    match table_definition_tree {
        Statement::Select(_) | Statement::Aggregate(_) => {
            panic!("Expected CREATE TABLE.");
        }
        Statement::CreateTable(table_definition) => {
            let table_name = table_definition.name.clone();
            tables.add_table(&table_name, table_definition);
        }
        Statement::Multiple(statements) => {
            for statement in statements {
                if let Statement::CreateTable(table_definition) = statement {
                    let table_name = table_definition.name.clone();
                    tables.add_table(&table_name, table_definition);
                }
            }
        }
    }

    tables
}

#[test]
fn test_ssh1() {
    let tables = create_tables("testdata/ssh_failure.txt");

    let query_tree = parser::parse_str("SELECT * FROM ssh").unwrap();
    let query = parse_tree_converter::transform_statement(query_tree).unwrap();

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ssh_data.txt").unwrap()],
        false,
        Default::default(),
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
        vec![File::open("testdata/ssh_data.txt").unwrap()],
        false,
        Default::default(),
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
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        false,
        Default::default(),
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
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        false,
        Default::default(),
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
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        false,
        Default::default(),
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
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        false,
        Default::default(),
        ExecutionEngine::new(&tables)
    ).unwrap();

    ingester.process(query).unwrap();
    assert_eq!(8, ingester.statistics.total_result_rows);
}

#[test]
fn test_ftpd5() {
    let tables = create_tables("testdata/ftpd.txt");

    let query_tree = parser::parse_str("SELECT hostname, COUNT(hostname) FROM connections WHERE regexp_matches(hostname, '.*in-addr.zen.co.uk') GROUP BY hostname").unwrap();
    let query = parse_tree_converter::transform_statement(query_tree).unwrap();

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        false,
        Default::default(),
        ExecutionEngine::new(&tables)
    ).unwrap();

    ingester.process(query).unwrap();
    assert_eq!(2, ingester.statistics.total_result_rows);
}

#[test]
fn test_client1() {
    let tables = create_tables("testdata/clients.txt");

    let query_tree = parser::parse_str("SELECT * FROM clients WHERE device_id >= 180;").unwrap();
    let query = parse_tree_converter::transform_statement(query_tree).unwrap();

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/clients_data.json").unwrap()],
        false,
        Default::default(),
        ExecutionEngine::new(&tables)
    ).unwrap();

    ingester.process(query).unwrap();
    assert_eq!(232, ingester.statistics.total_result_rows);
}

#[test]
fn test_client2() {
    let tables = create_tables("testdata/clients.txt");

    let query_tree = parser::parse_str("SELECT * FROM clients WHERE events IS NOT NULL").unwrap();
    let query = parse_tree_converter::transform_statement(query_tree).unwrap();

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/clients_data.json").unwrap()],
        false,
        Default::default(),
        ExecutionEngine::new(&tables)
    ).unwrap();

    ingester.process(query).unwrap();
    assert_eq!(3, ingester.statistics.total_result_rows);
}


#[test]
fn test_join1() {
    let tables = create_tables("testdata/dummy.txt");

    let query_tree = parser::parse_str("SELECT hostname, COUNT(*) FROM dummy1 INNER JOIN dummy2::'testdata/dummy2_data.txt' ON dummy1.hostname=dummy2.hostname GROUP BY hostname;").unwrap();
    let query = parse_tree_converter::transform_statement(query_tree).unwrap();

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/dummy1_data.txt").unwrap()],
        false,
        Default::default(),
        ExecutionEngine::new(&tables)
    ).unwrap();

    ingester.process(query).unwrap();
    assert_eq!(2, ingester.statistics.total_result_rows);
}

#[test]
fn test_join2() {
    let tables = create_tables("testdata/dummy.txt");

    let query_tree = parser::parse_str("SELECT hostname, min, dummy2.max FROM dummy1 INNER JOIN dummy2::'testdata/dummy2_data.txt' ON dummy1.hostname=dummy2.hostname").unwrap();
    let query = parse_tree_converter::transform_statement(query_tree).unwrap();

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/dummy1_data.txt").unwrap()],
        false,
        Default::default(),
        ExecutionEngine::new(&tables)
    ).unwrap();

    ingester.process(query).unwrap();
    assert_eq!(3, ingester.statistics.total_result_rows);
}

#[test]
fn test_join3() {
    let tables = create_tables("testdata/dummy.txt");

    let query_tree = parser::parse_str("SELECT hostname, dummy1.min, max FROM dummy2 OUTER JOIN dummy1::'testdata/dummy1_data.txt' ON dummy2.hostname=dummy1.hostname").unwrap();
    let query = parse_tree_converter::transform_statement(query_tree).unwrap();

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/dummy2_data.txt").unwrap()],
        false,
        Default::default(),
        ExecutionEngine::new(&tables)
    ).unwrap();

    ingester.process(query).unwrap();
    assert_eq!(4, ingester.statistics.total_result_rows);
}
