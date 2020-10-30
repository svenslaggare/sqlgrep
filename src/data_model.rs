use std::collections::HashMap;

use regex::Regex;

use crate::model::Value;
use crate::model::ValueType;

pub struct Row {
    pub columns: Vec<Value>
}

impl Row {
    pub fn any_result(&self) -> bool {
        self.columns.iter().any(|x| x.is_not_null())
    }
}

pub struct TableDefinition {
    pub name: String,
    patterns: Vec<(String, Regex)>,
    pub columns: Vec<ColumnDefinition>
}

impl TableDefinition {
    pub fn new(name: &str, patterns: Vec<(&str, &str)>, columns: Vec<ColumnDefinition>) -> Option<TableDefinition> {
        let mut compiled_patterns = Vec::new();
        for (name, pattern) in patterns {
            let pattern = Regex::new(pattern).ok()?;
            compiled_patterns.push((name.to_owned(), pattern));
        }

        Some(
            TableDefinition {
                name: name.to_owned(),
                patterns: compiled_patterns,
                columns
            }
        )
    }

    pub fn extract(&self, line: &str) -> Row {
        let mut extracted_results = HashMap::new();
        for (pattern_name, pattern) in &self.patterns {
            if let Some(capture) = pattern.captures(line) {
                extracted_results.insert(pattern_name, capture);
            }
        }

        let mut columns = Vec::new();
        for column in &self.columns {
            if let Some(capture_result) = extracted_results.get(&column.pattern_name) {
                if let Some(group_result) = capture_result.get(column.group_index) {
                    columns.push(Value::from_option(column.column_type.parse(group_result.as_str())));
                } else {
                    columns.push(Value::Null);
                }
            } else {
                columns.push(Value::Null);
            }
        }

        Row {
            columns
        }
    }
}

pub struct ColumnDefinition {
    pub pattern_name: String,
    pub group_index: usize,
    pub name: String,
    pub column_type: ValueType
}

impl ColumnDefinition {
    pub fn new(pattern_name: &str, group_index: usize, name: &str, column_type: ValueType) -> ColumnDefinition {
        ColumnDefinition {
            pattern_name: pattern_name.to_owned(),
            group_index,
            name: name.to_owned(),
            column_type
        }
    }
}

pub struct Tables {
    tables: HashMap<String, TableDefinition>
}

impl Tables {
    pub fn new() -> Tables {
        Tables {
            tables: HashMap::new()
        }
    }

    pub fn add_table(&mut self, name: &str, definition: TableDefinition) {
        self.tables.insert(name.to_owned(), definition);
    }

    pub fn get(&self, name: &str) -> Option<&TableDefinition> {
        self.tables.get(name)
    }
}

#[test]
fn test_table_extract1() {
    let table_definition = TableDefinition::new(
        "test",
        vec![("line", "([0-9]+)")],
        vec![
            ColumnDefinition::new("line", 1, "x", ValueType::Int)
        ]
    ).unwrap();

    let result = table_definition.extract("A: 4711");
    assert_eq!(Value::Int(4711), result.columns[0]);
}

#[test]
fn test_table_extract2() {
    let table_definition = TableDefinition::new(
        "test",
        vec![("line", "([A-Z]): ([0-9]+)")],
        vec![
            ColumnDefinition::new("line", 2, "x", ValueType::Int),
            ColumnDefinition::new("line", 1, "y", ValueType::String)
        ]
    ).unwrap();

    let result = table_definition.extract("A: 4711");
    assert_eq!(Value::Int(4711), result.columns[0]);
    assert_eq!(Value::String("A".to_owned()), result.columns[1]);
}

#[test]
fn test_table_extract3() {
    let table_definition = TableDefinition::new(
        "test",
        vec![("line", "([0-9]+)")],
        vec![
            ColumnDefinition::new("line", 1, "x", ValueType::Int)
        ]
    ).unwrap();

    let result = table_definition.extract("B: aba");
    assert!(!result.any_result());
}

#[test]
fn test_table_extract_multiple1() {
    let table_definition = TableDefinition::new(
        "test",
        vec![
            ("line1", "A: ([0-9]+)"),
            ("line2", "B: ([a-z]+)")
        ],
        vec![
            ColumnDefinition::new("line1", 1, "x", ValueType::Int),
            ColumnDefinition::new("line2", 1, "y", ValueType::String)
        ]
    ).unwrap();

    let result = table_definition.extract("A: 4711");
    assert_eq!(Value::Int(4711), result.columns[0]);
    assert_eq!(Value::Null, result.columns[1]);
}

#[test]
fn test_table_extract_multiple2() {
    let table_definition = TableDefinition::new(
        "test",
        vec![
            ("line1", "A: ([0-9]+)"),
            ("line2", "B: ([a-z]+)")
        ],
        vec![
            ColumnDefinition::new("line1", 1, "x", ValueType::Int),
            ColumnDefinition::new("line2", 1, "y", ValueType::String)
        ]
    ).unwrap();

    let result = table_definition.extract("A: 4711 B: aba");
    assert_eq!(Value::Int(4711), result.columns[0]);
    assert_eq!(Value::String("aba".to_owned()), result.columns[1]);
}

#[test]
fn test_table_extract_complex1() {
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

    let result = table_definition.extract("Jun 20 03:40:59 combo ftpd[8829]: connection from 222.33.90.199 () at Mon Jun 20 03:40:59 2005");
    assert_eq!(8, result.columns.len());
    assert_eq!(&Value::String("222.33.90.199".to_owned()), &result.columns[0]);
    assert_eq!(&Value::Null, &result.columns[1]);
    assert_eq!(&Value::Int(2005), &result.columns[2]);
    assert_eq!(&Value::String("Jun".to_owned()), &result.columns[3]);
    assert_eq!(&Value::Int(20), &result.columns[4]);
    assert_eq!(&Value::Int(3), &result.columns[5]);
    assert_eq!(&Value::Int(40), &result.columns[6]);
    assert_eq!(&Value::Int(59), &result.columns[7]);
}

#[test]
fn test_table_extract_complex2() {
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

    let result = table_definition.extract("Jun 18 02:08:12 combo ftpd[31286]: connection from 82.252.162.81 (lns-vlq-45-tou-82-252-162-81.adsl.proxad.net) at Sat Jun 18 02:08:12 2005");
    assert_eq!(8, result.columns.len());
    assert_eq!(&Value::String("82.252.162.81".to_owned()), &result.columns[0]);
    assert_eq!(&Value::String("lns-vlq-45-tou-82-252-162-81.adsl.proxad.net".to_owned()), &result.columns[1]);
    assert_eq!(&Value::Int(2005), &result.columns[2]);
    assert_eq!(&Value::String("Jun".to_owned()), &result.columns[3]);
    assert_eq!(&Value::Int(18), &result.columns[4]);
    assert_eq!(&Value::Int(2), &result.columns[5]);
    assert_eq!(&Value::Int(8), &result.columns[6]);
    assert_eq!(&Value::Int(12), &result.columns[7]);
}

#[test]
fn test_table_extract_complex3() {
    let table_definition = TableDefinition::new(
        "connections",
        vec![
            ("line", "connection from ([0-9.]+) \\((.*)\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)")
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

    let result = table_definition.extract("Jun 18 02:08:12 combo ftpd[31286]: connection from 82.252.162.81 (lns-vlq-45-tou-82-252-162-81.adsl.proxad.net) at Sat Jun 18 02:08:12 2005");
    assert_eq!(8, result.columns.len());
    assert_eq!(&Value::String("82.252.162.81".to_owned()), &result.columns[0]);
    assert_eq!(&Value::String("lns-vlq-45-tou-82-252-162-81.adsl.proxad.net".to_owned()), &result.columns[1]);
    assert_eq!(&Value::Int(2005), &result.columns[2]);
    assert_eq!(&Value::String("Jun".to_owned()), &result.columns[3]);
    assert_eq!(&Value::Int(18), &result.columns[4]);
    assert_eq!(&Value::Int(2), &result.columns[5]);
    assert_eq!(&Value::Int(8), &result.columns[6]);
    assert_eq!(&Value::Int(12), &result.columns[7]);
}