use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::fs::File;

use crate::data_model::{Tables};
use crate::execution::execution_engine::ExecutionEngine;
use crate::ingest::FileIngester;
use crate::model::Statement;
use crate::parsing;

fn create_tables(filename: &str) -> Tables {
    let mut tables = Tables::new();

    let table_definition = parsing::parse(&std::fs::read_to_string(filename).unwrap()).unwrap();
    match table_definition {
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

    let query = parsing::parse("SELECT * FROM ssh").unwrap();

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

    let query = parsing::parse("SELECT hostname, COUNT() AS count FROM ssh GROUP BY hostname").unwrap();

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

    let query = parsing::parse("SELECT * FROM connections WHERE hostname IS NOT NULL").unwrap();

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

    let query = parsing::parse("SELECT hostname, COUNT() FROM connections WHERE hostname IS NOT NULL GROUP BY hostname").unwrap();

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

    let query = parsing::parse("SELECT hostname, COUNT() FROM connections WHERE hostname IS NOT NULL HAVING COUNT() > 22 GROUP BY hostname").unwrap();

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

    let query = parsing::parse("SELECT hostname, COUNT(hostname) FROM connections GROUP BY hostname").unwrap();

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

    let query = parsing::parse("SELECT hostname, COUNT(hostname) FROM connections WHERE regexp_matches(hostname, '.*in-addr.zen.co.uk') GROUP BY hostname").unwrap();

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
fn test_ftpd6() {
    let tables = create_tables("testdata/ftpd_array.txt");

    let query = parsing::parse("SELECT * FROM connections WHERE hostname IS NOT NULL").unwrap();

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
fn test_ftpd7() {
    let tables = create_tables("testdata/ftpd_timestamp.txt");

    let query = parsing::parse("SELECT ip, hostname, timestamp, EXTRACT(EPOCH FROM timestamp) FROM connections WHERE hostname IS NOT NULL").unwrap();

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
fn test_ftpd8() {
    let tables = create_tables("testdata/ftpd_default.txt");

    let query = parsing::parse("SELECT * FROM connections WHERE ip IS NOT NULL").unwrap();

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        false,
        Default::default(),
        ExecutionEngine::new(&tables)
    ).unwrap();

    ingester.process(query).unwrap();
    assert_eq!(653, ingester.statistics.total_result_rows);
}

#[test]
fn test_ftpd9() {
    let tables = create_tables("testdata/ftpd.txt");

    let query = parsing::parse("SELECT hostname, array_unique(array_agg(ip)) AS ips FROM connections WHERE hostname IS NOT NULL GROUP BY hostname").unwrap();

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
fn test_ftpd_csv1() {
    let tables = create_tables("testdata/ftpd_csv.txt");

    let query = parsing::parse("SELECT * FROM connections WHERE hostname IS NOT NULL").unwrap();

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data_csv.txt").unwrap()],
        false,
        Default::default(),
        ExecutionEngine::new(&tables)
    ).unwrap();

    ingester.process(query).unwrap();
    assert_eq!(200, ingester.statistics.total_result_rows);
}

#[test]
fn test_client1() {
    let tables = create_tables("testdata/clients.txt");

    let query = parsing::parse("SELECT * FROM clients WHERE device_id >= 180;").unwrap();

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

    let query = parsing::parse("SELECT * FROM clients WHERE events IS NOT NULL").unwrap();

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
fn test_client3() {
    let tables = create_tables("testdata/clients.txt");

    let query = parsing::parse("SELECT timestamp, events[1] AS event FROM clients WHERE events IS NOT NULL").unwrap();

    let mut ingester = FileIngester::new(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/clients_data.json").unwrap()],
        true,
        Default::default(),
        ExecutionEngine::new(&tables)
    ).unwrap();

    ingester.process(query).unwrap();
    assert_eq!(3, ingester.statistics.total_result_rows);
}

#[test]
fn test_join1() {
    let tables = create_tables("testdata/dummy.txt");

    let query = parsing::parse("SELECT hostname, COUNT(*) FROM dummy1 INNER JOIN dummy2::'testdata/dummy2_data.txt' ON dummy1.hostname=dummy2.hostname GROUP BY hostname;").unwrap();

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

    let query = parsing::parse("SELECT hostname, min, dummy2.max FROM dummy1 INNER JOIN dummy2::'testdata/dummy2_data.txt' ON dummy1.hostname=dummy2.hostname").unwrap();

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

    let query = parsing::parse("SELECT hostname, dummy1.min, max FROM dummy2 OUTER JOIN dummy1::'testdata/dummy1_data.txt' ON dummy2.hostname=dummy1.hostname").unwrap();

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
