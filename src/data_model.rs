use std::collections::HashMap;

use regex::Regex;

use crate::model::{Value, Float};
use crate::model::ValueType;

pub struct Row {
    pub columns: Vec<Value>
}

impl Row {
    pub fn new(columns: Vec<Value>) -> Row {
        Row {
            columns
        }
    }

    pub fn any_result(&self) -> bool {
        self.columns.iter().any(|x| x.is_not_null())
    }
}

pub struct TableDefinition {
    pub name: String,
    pub patterns: Vec<(String, Regex)>,
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
            let mut is_null = false;
            if let Some(capture_result) = extracted_results.get(&column.pattern_name) {
                let group_result = capture_result.get(column.group_index);
                if column.column_type == ValueType::Bool {
                    columns.push(Value::Bool(group_result.is_some()));
                } else {
                    if let Some(group) = group_result {
                        let mut value = Value::from_option(column.column_type.parse(group.as_str()));
                        if column.options.trim {
                            value.modify(
                                |_| {},
                                |_| {},
                                |_| {},
                                |x| *x = x.trim().to_owned()
                            )
                        }

                        columns.push(value);
                    } else {
                        columns.push(Value::Null);
                        is_null = true;
                    }
                }
            } else {
                columns.push(Value::Null);
                is_null = true;
            }

            if is_null && !column.options.nullable {
                columns.clear();
                break;
            }
        }

        Row {
            columns
        }
    }
}

pub struct ColumnOptions {
    pub nullable: bool,
    pub trim: bool
}

impl ColumnOptions {
    pub fn new() -> ColumnOptions {
        ColumnOptions {
            nullable: true,
            trim: false
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn trim(mut self) -> Self {
        self.trim = true;
        self
    }
}

pub struct ColumnDefinition {
    pub pattern_name: String,
    pub group_index: usize,
    pub name: String,
    pub column_type: ValueType,
    pub options: ColumnOptions
}

impl ColumnDefinition {
    pub fn new(pattern_name: &str,
               group_index: usize,
               name: &str,
               column_type: ValueType) -> ColumnDefinition {
        ColumnDefinition {
            pattern_name: pattern_name.to_owned(),
            group_index,
            name: name.to_owned(),
            column_type,
            options: ColumnOptions::new()
        }
    }

    pub fn with_options(pattern_name: &str,
                        group_index: usize,
                        name: &str,
                        column_type: ValueType,
                        options: ColumnOptions) -> ColumnDefinition {
        ColumnDefinition {
            pattern_name: pattern_name.to_owned(),
            group_index,
            name: name.to_owned(),
            column_type,
            options
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

    pub fn with_tables(tables: Vec<TableDefinition>) -> Tables {
        let mut new_tables = Tables::new();

        for table in tables {
            let name = table.name.clone();
            new_tables.add_table(&name, table);
        }

        new_tables
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
fn test_table_extract4() {
    let table_definition = TableDefinition::new(
        "test",
        vec![("line", "A: ([0-9]+)?")],
        vec![
            ColumnDefinition::new("line", 1, "x", ValueType::Bool),
        ]
    ).unwrap();

    let result = table_definition.extract("A: 4711");
    assert_eq!(Value::Bool(true), result.columns[0]);

    let result = table_definition.extract("A: aba");
    assert_eq!(Value::Bool(false), result.columns[0]);
}

#[test]
fn test_table_extract5() {
    let table_definition = TableDefinition::new(
        "test",
        vec![("line", "([0-9\\.]+)")],
        vec![
            ColumnDefinition::new("line", 1, "x", ValueType::Float)
        ]
    ).unwrap();

    let result = table_definition.extract("A: 4711");
    assert_eq!(Value::Float(Float(4711.0)), result.columns[0]);

    let result = table_definition.extract("A: 4711.1337");
    assert_eq!(Value::Float(Float(4711.1337)), result.columns[0]);
}

#[test]
fn test_table_extract6() {
    let table_definition = TableDefinition::new(
        "test",
        vec![("line", "([0-9 ]+)")],
        vec![
            ColumnDefinition::with_options("line", 1, "x", ValueType::String, ColumnOptions::new().trim())
        ]
    ).unwrap();

    let result = table_definition.extract("A: 4711    ");
    assert_eq!(Value::String("4711".to_owned()), result.columns[0]);
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
fn test_table_extract_multiple3() {
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
fn test_table_extract_multiple4() {
    let table_definition = TableDefinition::new(
        "test",
        vec![
            ("line1", "A: ([0-9]+)"),
            ("line2", "B: ([a-z]+)")
        ],
        vec![
            ColumnDefinition::new("line1", 1, "x", ValueType::Int),
            ColumnDefinition::with_options("line2", 1, "y", ValueType::String, ColumnOptions::new().not_null())
        ]
    ).unwrap();

    let result = table_definition.extract("A: 4711 B: aba");
    assert_eq!(Value::Int(4711), result.columns[0]);
    assert_eq!(Value::String("aba".to_owned()), result.columns[1]);
}

#[test]
fn test_table_extract_multiple5() {
    let table_definition = TableDefinition::new(
        "test",
        vec![
            ("line1", "A: ([0-9]+)"),
            ("line2", "B: ([a-z]+)")
        ],
        vec![
            ColumnDefinition::new("line1", 1, "x", ValueType::Int),
            ColumnDefinition::with_options("line2", 1, "y", ValueType::String, ColumnOptions::new().not_null())
        ]
    ).unwrap();

    let result = table_definition.extract("A: 4711");
    assert_eq!(false, result.any_result());
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