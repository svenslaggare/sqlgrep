use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::fs::File;

use crate::data_model::{Tables};
use crate::execution::execution_engine::ExecutionEngine;
use crate::executor::{DisplayOptions, FileExecutor, Printer};
use crate::parsing;

fn create_tables(filename: &str) -> Tables {
    let mut tables = Tables::new();

    let table_definition = parsing::parse(&std::fs::read_to_string(filename).unwrap()).unwrap();
    if !tables.add_tables(table_definition) {
        panic!("Expected CREATE TABLE.");
    }

    tables
}

#[test]
fn test_ssh1() {
    let tables = create_tables("testdata/ssh_failure.txt");

    let query = parsing::parse("SELECT * FROM ssh").unwrap();

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ssh_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(386, executor.statistics().total_result_rows);

    assert_eq!("hostname: '5.36.59.76.dynamic-dsl-ip.omantel.net.om', username: 'root'", executor.output_printer().printer().lines[0]);
    assert_eq!("hostname: '183.62.140.253', username: 'root'", executor.output_printer().printer().lines[385]);
}

#[test]
fn test_ssh2() {
    let tables = create_tables("testdata/ssh_failure.txt");

    let query = parsing::parse("SELECT hostname, COUNT() AS count FROM ssh GROUP BY hostname").unwrap();

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ssh_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(14, executor.statistics().total_result_rows);

    assert_eq!("hostname: '103.207.39.16', count: 1", executor.output_printer().printer().lines[0]);
    assert_eq!("hostname: '103.207.39.212', count: 1", executor.output_printer().printer().lines[1]);
    assert_eq!("hostname: '103.99.0.122', count: 11", executor.output_printer().printer().lines[2]);
    assert_eq!("hostname: '104.192.3.34', count: 1", executor.output_printer().printer().lines[3]);
    assert_eq!("hostname: '106.5.5.195', count: 2", executor.output_printer().printer().lines[4]);
    assert_eq!("hostname: '112.95.230.3', count: 24", executor.output_printer().printer().lines[5]);
    assert_eq!("hostname: '123.235.32.19', count: 7", executor.output_printer().printer().lines[6]);
    assert_eq!("hostname: '183.62.140.253', count: 278", executor.output_printer().printer().lines[7]);
    assert_eq!("hostname: '187.141.143.180', count: 51", executor.output_printer().printer().lines[8]);
    assert_eq!("hostname: '191.210.223.172', count: 1", executor.output_printer().printer().lines[9]);
    assert_eq!("hostname: '195.154.37.122', count: 1", executor.output_printer().printer().lines[10]);
    assert_eq!("hostname: '5.188.10.180', count: 1", executor.output_printer().printer().lines[11]);
    assert_eq!("hostname: '5.36.59.76.dynamic-dsl-ip.omantel.net.om', count: 2", executor.output_printer().printer().lines[12]);
    assert_eq!("hostname: '60.2.12.12', count: 5", executor.output_printer().printer().lines[13]);
}

#[test]
fn test_ftpd1() {
    let tables = create_tables("testdata/ftpd.txt");

    let query = parsing::parse("SELECT * FROM connections WHERE hostname IS NOT NULL").unwrap();

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(200, executor.statistics().total_result_rows);

    assert_eq!("ip: '24.54.76.216', hostname: '24-54-76-216.bflony.adelphia.net', year: 2005, month: 'Jun', day: 17, hour: 7, minute: 7, second: 0", executor.output_printer().printer().lines[0]);
    assert_eq!("ip: '82.68.222.195', hostname: '82-68-222-195.dsl.in-addr.zen.co.uk', year: 2005, month: 'Jul', day: 17, hour: 23, minute: 21, second: 54", executor.output_printer().printer().lines[199]);
}

#[test]
fn test_ftpd2() {
    let tables = create_tables("testdata/ftpd.txt");

    let query = parsing::parse(
        "SELECT hostname, COUNT() FROM connections WHERE hostname IS NOT NULL GROUP BY hostname"
    ).unwrap();

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(8, executor.statistics().total_result_rows);

    assert_eq!("hostname: '24-54-76-216.bflony.adelphia.net', count1: 8", executor.output_printer().printer().lines[0]);
    assert_eq!("hostname: '82-68-222-194.dsl.in-addr.zen.co.uk', count1: 23", executor.output_printer().printer().lines[1]);
    assert_eq!("hostname: '82-68-222-195.dsl.in-addr.zen.co.uk', count1: 23", executor.output_printer().printer().lines[2]);
    assert_eq!("hostname: 'aml-sfh-3310b.adsl.wanadoo.nl', count1: 32", executor.output_printer().printer().lines[3]);
    assert_eq!("hostname: 'dsl-082-083-227-067.arcor-ip.net', count1: 23", executor.output_printer().printer().lines[4]);
    assert_eq!("hostname: 'dsl-Chn-static-059.45.101.203.touchtelindia.net', count1: 23", executor.output_printer().printer().lines[5]);
    assert_eq!("hostname: 'host8.topspot.net', count1: 46", executor.output_printer().printer().lines[6]);
    assert_eq!("hostname: 'lns-vlq-45-tou-82-252-162-81.adsl.proxad.net', count1: 22", executor.output_printer().printer().lines[7]);
}

#[test]
fn test_ftpd3() {
    let tables = create_tables("testdata/ftpd.txt");

    let query = parsing::parse(
        "SELECT hostname, COUNT() FROM connections WHERE hostname IS NOT NULL HAVING COUNT() > 22 GROUP BY hostname"
    ).unwrap();

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(6, executor.statistics().total_result_rows);

    assert_eq!("hostname: '82-68-222-194.dsl.in-addr.zen.co.uk', count1: 23", executor.output_printer().printer().lines[0]);
    assert_eq!("hostname: '82-68-222-195.dsl.in-addr.zen.co.uk', count1: 23", executor.output_printer().printer().lines[1]);
    assert_eq!("hostname: 'aml-sfh-3310b.adsl.wanadoo.nl', count1: 32", executor.output_printer().printer().lines[2]);
    assert_eq!("hostname: 'dsl-082-083-227-067.arcor-ip.net', count1: 23", executor.output_printer().printer().lines[3]);
    assert_eq!("hostname: 'dsl-Chn-static-059.45.101.203.touchtelindia.net', count1: 23", executor.output_printer().printer().lines[4]);
    assert_eq!("hostname: 'host8.topspot.net', count1: 46", executor.output_printer().printer().lines[5]);
}

#[test]
fn test_ftpd4() {
    let tables = create_tables("testdata/ftpd.txt");

    let query = parsing::parse("SELECT hostname, COUNT(hostname) FROM connections GROUP BY hostname").unwrap();

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(8, executor.statistics().total_result_rows);

    assert_eq!("hostname: '24-54-76-216.bflony.adelphia.net', count1: 8", executor.output_printer().printer().lines[0]);
    assert_eq!("hostname: '82-68-222-194.dsl.in-addr.zen.co.uk', count1: 23", executor.output_printer().printer().lines[1]);
    assert_eq!("hostname: '82-68-222-195.dsl.in-addr.zen.co.uk', count1: 23", executor.output_printer().printer().lines[2]);
    assert_eq!("hostname: 'aml-sfh-3310b.adsl.wanadoo.nl', count1: 32", executor.output_printer().printer().lines[3]);
    assert_eq!("hostname: 'dsl-082-083-227-067.arcor-ip.net', count1: 23", executor.output_printer().printer().lines[4]);
    assert_eq!("hostname: 'dsl-Chn-static-059.45.101.203.touchtelindia.net', count1: 23", executor.output_printer().printer().lines[5]);
    assert_eq!("hostname: 'host8.topspot.net', count1: 46", executor.output_printer().printer().lines[6]);
    assert_eq!("hostname: 'lns-vlq-45-tou-82-252-162-81.adsl.proxad.net', count1: 22", executor.output_printer().printer().lines[7]);
}

#[test]
fn test_ftpd5() {
    let tables = create_tables("testdata/ftpd.txt");

    let query = parsing::parse(
        "SELECT hostname, COUNT(hostname) FROM connections WHERE regexp_matches(hostname, '.*in-addr.zen.co.uk') GROUP BY hostname"
    ).unwrap();

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(2, executor.statistics().total_result_rows);

    assert_eq!("hostname: '82-68-222-194.dsl.in-addr.zen.co.uk', count1: 23", executor.output_printer().printer().lines[0]);
    assert_eq!("hostname: '82-68-222-195.dsl.in-addr.zen.co.uk', count1: 23", executor.output_printer().printer().lines[1]);
}

#[test]
fn test_ftpd6() {
    let tables = create_tables("testdata/ftpd_array.txt");

    let query = parsing::parse("SELECT * FROM connections WHERE hostname IS NOT NULL").unwrap();

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(200, executor.statistics().total_result_rows);

    assert_eq!("ip: '24.54.76.216', hostname: '24-54-76-216.bflony.adelphia.net', timestamp: {'2005', 'Jun', '17', '07', '07', '00'}", executor.output_printer().printer().lines[0]);
    assert_eq!("ip: '82.68.222.195', hostname: '82-68-222-195.dsl.in-addr.zen.co.uk', timestamp: {'2005', 'Jul', '17', '23', '21', '54'}", executor.output_printer().printer().lines[199]);
}

#[test]
fn test_ftpd7() {
    let tables = create_tables("testdata/ftpd_timestamp.txt");

    let query = parsing::parse("SELECT ip, hostname, timestamp, EXTRACT(EPOCH FROM timestamp) FROM connections WHERE hostname IS NOT NULL").unwrap();

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(200, executor.statistics().total_result_rows);

    assert_eq!("ip: '24.54.76.216', hostname: '24-54-76-216.bflony.adelphia.net', timestamp: 2005-06-17 07:07:00.000, p3: 1118984820.00", executor.output_printer().printer().lines[0]);
    assert_eq!("ip: '82.68.222.195', hostname: '82-68-222-195.dsl.in-addr.zen.co.uk', timestamp: 2005-07-17 23:21:54.000, p3: 1121635314.00", executor.output_printer().printer().lines[199]);
}

#[test]
fn test_ftpd8() {
    let tables = create_tables("testdata/ftpd_default.txt");

    let query = parsing::parse("SELECT * FROM connections WHERE ip IS NOT NULL").unwrap();

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(653, executor.statistics().total_result_rows);

    assert_eq!("ip: '24.54.76.216', hostname: '24-54-76-216.bflony.adelphia.net', year: 2005, month: 'Jun', day: 17, hour: 7, minute: 7, second: 0", executor.output_printer().printer().lines[0]);
    assert_eq!("ip: '218.38.58.3', hostname: 'unknown', year: 2005, month: 'Jul', day: 27, hour: 10, minute: 59, second: 53", executor.output_printer().printer().lines[652]);
}

#[test]
fn test_ftpd9() {
    let tables = create_tables("testdata/ftpd.txt");

    let query = parsing::parse(
        "SELECT hostname, array_unique(array_agg(ip)) AS ips FROM connections WHERE hostname IS NOT NULL GROUP BY hostname"
    ).unwrap();

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(8, executor.statistics().total_result_rows);

    assert_eq!("hostname: '24-54-76-216.bflony.adelphia.net', ips: {'24.54.76.216'}", executor.output_printer().printer().lines[0]);
    assert_eq!("hostname: '82-68-222-194.dsl.in-addr.zen.co.uk', ips: {'82.68.222.194'}", executor.output_printer().printer().lines[1]);
    assert_eq!("hostname: '82-68-222-195.dsl.in-addr.zen.co.uk', ips: {'82.68.222.195'}", executor.output_printer().printer().lines[2]);
    assert_eq!("hostname: 'aml-sfh-3310b.adsl.wanadoo.nl', ips: {'83.116.207.11'}", executor.output_printer().printer().lines[3]);
    assert_eq!("hostname: 'dsl-082-083-227-067.arcor-ip.net', ips: {'82.83.227.67'}", executor.output_printer().printer().lines[4]);
    assert_eq!("hostname: 'dsl-Chn-static-059.45.101.203.touchtelindia.net', ips: {'203.101.45.59'}", executor.output_printer().printer().lines[5]);
    assert_eq!("hostname: 'host8.topspot.net', ips: {'207.30.238.8'}", executor.output_printer().printer().lines[6]);
    assert_eq!("hostname: 'lns-vlq-45-tou-82-252-162-81.adsl.proxad.net', ips: {'82.252.162.81'}", executor.output_printer().printer().lines[7]);
}

#[test]
fn test_ftpd10() {
    let tables = create_tables("testdata/ftpd_timestamp.txt");

    let query = parsing::parse(
        "SELECT EXTRACT(hour FROM timestamp), COUNT(*) FROM connections WHERE hostname IS NOT NULL GROUP BY EXTRACT(hour FROM timestamp)"
    ).unwrap();

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(10, executor.statistics().total_result_rows);

    assert_eq!("p0: 2, count1: 15", executor.output_printer().printer().lines[0]);
    assert_eq!("p0: 4, count1: 2", executor.output_printer().printer().lines[1]);
    assert_eq!("p0: 6, count1: 30", executor.output_printer().printer().lines[2]);
    assert_eq!("p0: 7, count1: 31", executor.output_printer().printer().lines[3]);
    assert_eq!("p0: 12, count1: 23", executor.output_printer().printer().lines[4]);
    assert_eq!("p0: 14, count1: 23", executor.output_printer().printer().lines[5]);
    assert_eq!("p0: 15, count1: 23", executor.output_printer().printer().lines[6]);
    assert_eq!("p0: 20, count1: 7", executor.output_printer().printer().lines[7]);
    assert_eq!("p0: 21, count1: 23", executor.output_printer().printer().lines[8]);
    assert_eq!("p0: 23, count1: 23", executor.output_printer().printer().lines[9]);
}

#[test]
fn test_ftpd11() {
    let tables = create_tables("testdata/ftpd.txt");

    let query = parsing::parse(
        "SELECT DISTINCT hostname FROM connections WHERE hostname IS NOT NULL"
    ).unwrap();

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(8, executor.statistics().total_result_rows);

    assert_eq!("hostname: '24-54-76-216.bflony.adelphia.net'", executor.output_printer().printer().lines[0]);
    assert_eq!("hostname: 'lns-vlq-45-tou-82-252-162-81.adsl.proxad.net'", executor.output_printer().printer().lines[1]);
    assert_eq!("hostname: 'dsl-082-083-227-067.arcor-ip.net'", executor.output_printer().printer().lines[2]);
    assert_eq!("hostname: 'aml-sfh-3310b.adsl.wanadoo.nl'", executor.output_printer().printer().lines[3]);
    assert_eq!("hostname: 'host8.topspot.net'", executor.output_printer().printer().lines[4]);
    assert_eq!("hostname: 'dsl-Chn-static-059.45.101.203.touchtelindia.net'", executor.output_printer().printer().lines[5]);
    assert_eq!("hostname: '82-68-222-194.dsl.in-addr.zen.co.uk'", executor.output_printer().printer().lines[6]);
    assert_eq!("hostname: '82-68-222-195.dsl.in-addr.zen.co.uk'", executor.output_printer().printer().lines[7]);
}

#[test]
fn test_ftpd_csv1() {
    let tables = create_tables("testdata/ftpd_csv.txt");

    let query = parsing::parse("SELECT * FROM connections WHERE hostname IS NOT NULL").unwrap();

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/ftpd_data_csv.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(200, executor.statistics().total_result_rows);

    assert_eq!("ip: '24.54.76.216', hostname: '24-54-76-216.bflony.adelphia.net', year: 2005, month: 'Jun', day: 17, hour: 7, minute: 7, second: 0", executor.output_printer().printer().lines[0]);
    assert_eq!("ip: '82.68.222.195', hostname: '82-68-222-195.dsl.in-addr.zen.co.uk', year: 2005, month: 'Jul', day: 17, hour: 23, minute: 21, second: 54", executor.output_printer().printer().lines[199]);
}

#[test]
fn test_client1() {
    let tables = create_tables("testdata/clients.txt");

    let query = parsing::parse("SELECT * FROM clients WHERE device_id >= 180;").unwrap();

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/clients_data.json").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(232, executor.statistics().total_result_rows);

    assert_eq!("timestamp: 1609790687065, device_id: 189, mac_address: '10:41:11:98:05:52', events: NULL", executor.output_printer().printer().lines[0]);
    assert_eq!("timestamp: 1609799887613, device_id: 256, mac_address: '10:41:11:98:0b:7e', events: NULL", executor.output_printer().printer().lines[231]);
}

#[test]
fn test_client2() {
    let tables = create_tables("testdata/clients.txt");

    let query = parsing::parse("SELECT * FROM clients WHERE events IS NOT NULL").unwrap();

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/clients_data.json").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(3, executor.statistics().total_result_rows);

    assert_eq!("timestamp: 1609789423312, device_id: 167, mac_address: '10:41:11:98:03:35', events: {'started', 'stopped'}", executor.output_printer().printer().lines[0]);
    assert_eq!("timestamp: 1609789423325, device_id: 167, mac_address: '10:41:11:98:03:35', events: {'stopped'}", executor.output_printer().printer().lines[1]);
    assert_eq!("timestamp: 1609789426639, device_id: 172, mac_address: '10:41:11:98:0b:66', events: {}", executor.output_printer().printer().lines[2]);
}

#[test]
fn test_client3() {
    let tables = create_tables("testdata/clients.txt");

    let query = parsing::parse("SELECT timestamp, events[1] AS event FROM clients WHERE events IS NOT NULL").unwrap();

    let mut display_options = DisplayOptions::default();
    display_options.single_result = true;
    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/clients_data.json").unwrap()],
        display_options,
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(3, executor.statistics().total_result_rows);

    assert_eq!("timestamp: 1609789423312, event: 'started'", executor.output_printer().printer().lines[0]);
    assert_eq!("timestamp: 1609789423325, event: 'stopped'", executor.output_printer().printer().lines[1]);
    assert_eq!("timestamp: 1609789426639, event: NULL", executor.output_printer().printer().lines[2]);
}

#[test]
fn test_join1() {
    let tables = create_tables("testdata/dummy.txt");

    let query = parsing::parse("SELECT hostname, COUNT(*) FROM dummy1 INNER JOIN dummy2::'testdata/dummy2_data.txt' ON dummy1.hostname=dummy2.hostname GROUP BY hostname;").unwrap();

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/dummy1_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(2, executor.statistics().total_result_rows);

    assert_eq!("hostname: 'test.se', count1: 1", executor.output_printer().printer().lines[0]);
    assert_eq!("hostname: 'test2.com', count1: 2", executor.output_printer().printer().lines[1]);
}

#[test]
fn test_join2() {
    let tables = create_tables("testdata/dummy.txt");

    let query = parsing::parse("SELECT hostname, min, dummy2.max FROM dummy1 INNER JOIN dummy2::'testdata/dummy2_data.txt' ON dummy1.hostname=dummy2.hostname").unwrap();

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/dummy1_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(3, executor.statistics().total_result_rows);

    assert_eq!("hostname: 'test.se', min: 424, dummy2.max: 414", executor.output_printer().printer().lines[0]);
    assert_eq!("hostname: 'test2.com', min: 1313, dummy2.max: 1131", executor.output_printer().printer().lines[1]);
    assert_eq!("hostname: 'test2.com', min: 1331, dummy2.max: 1131", executor.output_printer().printer().lines[2]);
}

#[test]
fn test_join3() {
    let tables = create_tables("testdata/dummy.txt");

    let query = parsing::parse("SELECT hostname, dummy1.min, max FROM dummy2 OUTER JOIN dummy1::'testdata/dummy1_data.txt' ON dummy2.hostname=dummy1.hostname").unwrap();

    let mut executor = FileExecutor::with_output_printer(
        Arc::new(AtomicBool::new(true)),
        vec![File::open("testdata/dummy2_data.txt").unwrap()],
        Default::default(),
        CapturedPrinter::new(),
        ExecutionEngine::new(&tables, &query)
    ).unwrap();

    executor.execute().unwrap();
    assert_eq!(4, executor.statistics().total_result_rows);

    assert_eq!("hostname: 'test.se', dummy1.min: 424, max: 414", executor.output_printer().printer().lines[0]);
    assert_eq!("hostname: 'test3.com', dummy1.min: NULL, max: 34134", executor.output_printer().printer().lines[1]);
    assert_eq!("hostname: 'test2.com', dummy1.min: 1313, max: 1131", executor.output_printer().printer().lines[2]);
    assert_eq!("hostname: 'test2.com', dummy1.min: 1331, max: 1131", executor.output_printer().printer().lines[3]);
}

pub struct CapturedPrinter {
    lines: Vec<String>
}

impl CapturedPrinter {
    pub fn new() -> CapturedPrinter {
        CapturedPrinter {
            lines: Vec::new()
        }
    }

    pub fn lines(&self) -> &Vec<String> {
        &self.lines
    }
}

impl Printer for CapturedPrinter {
    fn println(&mut self, line: &str) {
        self.lines.push(line.to_owned());
    }
}