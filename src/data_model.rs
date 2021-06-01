use std::collections::HashMap;

use itertools::Itertools;

use regex::{Regex, Captures};

use serde_json::json;

use crate::model::{Value, Float, create_timestamp};
use crate::model::ValueType;

#[derive(Debug)]
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

#[derive(Debug, Clone)]
pub struct TableDefinition {
    pub name: String,
    pub patterns: Vec<(String, Regex)>,
    pub columns: Vec<ColumnDefinition>,
    pub fully_qualified_column_names: Vec<String>,
    any_json_columns: bool,
}

impl TableDefinition {
    pub fn new(name: &str, patterns: Vec<(&str, &str)>, columns: Vec<ColumnDefinition>) -> Option<TableDefinition> {
        let mut compiled_patterns = Vec::new();
        for (name, pattern) in patterns {
            let pattern = Regex::new(pattern).ok()?;
            compiled_patterns.push((name.to_owned(), pattern));
        }

        let mut any_json_columns = false;
        for column in &columns {
            if let ColumnParsing::Json(..) = column.parsing {
                any_json_columns = true;
                break;
            }
        }

        let fully_qualified_column_names = columns
            .iter()
            .map(|column| format!("{}.{}", name, column.name))
            .collect::<Vec<_>>();

        Some(
            TableDefinition {
                name: name.to_owned(),
                patterns: compiled_patterns,
                columns,
                fully_qualified_column_names,
                any_json_columns
            }
        )
    }

    pub fn extract(&self, line: &str) -> Row {
        let parsing_input = ParsingInput::new(&self, line);

        let mut columns = Vec::new();
        for column in &self.columns {
            let mut value = column.parsing.extract(&column, &parsing_input);

            if column.options.trim {
                value.modify(
                    |_| {},
                    |_| {},
                    |_| {},
                    |x| *x = x.trim().to_owned(),
                    |_| {},
                    |_| {}
                )
            }

            let is_null = value.is_null();
            columns.push(value);
            if is_null && !column.options.nullable {
                columns.clear();
                break;
            }
        }

        Row {
            columns
        }
    }

    pub fn index_for(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .find_position(|column| column.name == name)
            .map(|(index, _)| index)
    }
}

#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    pub parsing: ColumnParsing,
    pub name: String,
    pub column_type: ValueType,
    pub options: ColumnOptions
}

impl ColumnDefinition {
    pub fn with_regex(pattern_name: &str,
                      group_index: usize,
                      name: &str,
                      column_type: ValueType) -> ColumnDefinition {
        ColumnDefinition {
            parsing: ColumnParsing::Regex(RegexPattern {
                pattern_name: pattern_name.to_owned(),
                group_index,
            }),
            name: name.to_owned(),
            column_type,
            options: ColumnOptions::new()
        }
    }

    pub fn with_parsing(parsing: ColumnParsing,
                        name: &str,
                        column_type: ValueType) -> ColumnDefinition {
        ColumnDefinition {
            parsing,
            name: name.to_owned(),
            column_type,
            options: ColumnOptions::new()
        }
    }

    pub fn with_options(parsing: ColumnParsing,
                        name: &str,
                        column_type: ValueType,
                        options: ColumnOptions) -> ColumnDefinition {
        ColumnDefinition {
            parsing,
            name: name.to_owned(),
            column_type,
            options
        }
    }
}

pub struct ParsingInput<'a> {
    regex_results: HashMap<&'a String, Captures<'a>>,
    json_value: serde_json::Value
}

impl<'a> ParsingInput<'a> {
    pub fn new(table: &'a TableDefinition, line: &'a str) -> ParsingInput<'a> {
        let mut regex_results = HashMap::new();
        for (pattern_name, pattern) in &table.patterns {
            if let Some(capture) = pattern.captures(line) {
                regex_results.insert(pattern_name, capture);
            }
        }

        let json_value: serde_json::Value = if table.any_json_columns {
            serde_json::from_str(line).unwrap_or(serde_json::Value::Null)
        } else {
            serde_json::Value::Null
        };

        ParsingInput {
            regex_results,
            json_value
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct RegexPattern {
    pub pattern_name: String,
    pub group_index: usize
}

impl RegexPattern {
    pub fn new(pattern_name: String, group_index: usize) -> RegexPattern {
        RegexPattern {
            pattern_name,
            group_index
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum ColumnParsing {
    Regex(RegexPattern),
    MultiRegex(Vec<RegexPattern>),
    Json(JsonAccess)
}

impl ColumnParsing {
    pub fn extract(&self, column: &ColumnDefinition, parsing_input: &ParsingInput) -> Value {
        match self {
            ColumnParsing::Regex(pattern) => {
                ColumnParsing::extract_using_regex(&column.column_type, parsing_input, pattern)
            }
            ColumnParsing::MultiRegex(patterns) => {
                match &column.column_type {
                    ValueType::Array(element) => {
                        let mut values = Vec::new();
                        for pattern in patterns {
                            values.push(ColumnParsing::extract_using_regex(element, parsing_input, pattern));
                        }

                        if values.iter().any(|v| v.is_not_null()) {
                            Value::Array(*element.clone(), values)
                        } else {
                            Value::Null
                        }
                    }
                    ValueType::Timestamp => {
                        let mut year = 0i32;
                        let mut month = 1u32;
                        let mut day = 1u32;
                        let mut hour = 0u32;
                        let mut minute = 0u32;
                        let mut second = 0u32;

                        for (index, pattern) in patterns.iter().enumerate() {
                            let value = ColumnParsing::extract_using_regex(&ValueType::Int, parsing_input, pattern);

                            if let Value::Int(value_i64) = value {
                                match index {
                                    0 => { year = value_i64 as i32 },
                                    1 => { month = value_i64 as u32 },
                                    2 => { day = value_i64 as u32  },
                                    3 => { hour = value_i64 as u32  },
                                    4 => { minute = value_i64 as u32  },
                                    5 => { second = value_i64 as u32  }
                                    _ => {}
                                }
                            } else {
                                if index == 1 {
                                    if let Value::String(value) = ColumnParsing::extract_using_regex(&ValueType::String, parsing_input, pattern) {
                                        match value.to_lowercase().as_str() {
                                            "jan" => { month = 1; },
                                            "feb" => { month = 2; },
                                            "mar" => { month = 3; },
                                            "apr" => { month = 4; },
                                            "may" => { month = 5; },
                                            "jun" | "june" => { month = 6; },
                                            "jul" | "july" => { month = 7; },
                                            "aug" => { month = 8; },
                                            "sep" | "sept" => { month = 9; },
                                            "oct" => { month = 10; },
                                            "nov" => { month = 11; }
                                            "dec" => { month = 12; }
                                            _ => { return Value::Null; }
                                        }
                                    }
                                } else {
                                    return Value::Null;
                                }
                            }
                        }

                        match create_timestamp(year, month, day, hour, minute, second) {
                            Some(timestamp) => Value::Timestamp(timestamp),
                            None => Value::Null
                        }

                    }
                    _ => Value::Null
                }
            }
            ColumnParsing::Json(access) => {
                if let Some(value) = access.get_value(&parsing_input.json_value) {
                    if column.options.convert {
                        value
                            .as_str()
                            .map(|value| column.column_type.parse(value).unwrap_or(Value::Null))
                            .unwrap_or(Value::Null)
                    } else {
                        column.column_type.convert_json(value)
                    }
                } else {
                    Value::Null
                }
            }
        }
    }

    fn extract_using_regex(column_type: &ValueType,
                           parsing_input: &ParsingInput,
                           pattern: &RegexPattern) -> Value {
        let pattern_name = &pattern.pattern_name;
        let group_index = &pattern.group_index;
        if let Some(capture_result) = parsing_input.regex_results.get(&pattern_name) {
            let group_result = capture_result.get(*group_index);
            if column_type == &ValueType::Bool {
                Value::Bool(group_result.is_some())
            } else {
                if let Some(group) = group_result {
                    Value::from_option(column_type.parse(group.as_str()))
                } else {
                    Value::Null
                }
            }
        } else {
            Value::Null
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum JsonAccess {
    Field { name: String, inner: Option<Box<JsonAccess>> },
    Array { index: usize, inner: Option<Box<JsonAccess>> }
}

impl JsonAccess {
    pub fn from_linear(parts: Vec<JsonAccess>) -> JsonAccess {
        let mut current = None;
        for mut part in parts.into_iter().rev() {
            part.set_inner(current);
            current = Some(part);
        }

        current.unwrap()
    }

    pub fn get_value<'a>(&self, json_value: &'a serde_json::Value) -> Option<&'a serde_json::Value> {
        match self {
            JsonAccess::Field { name, inner } => {
                let value = json_value.get(name)?;

                if let Some(inner) = inner {
                    inner.get_value(value)
                } else {
                    Some(value)
                }
            }
            JsonAccess::Array { index, inner } => {
                let value = json_value.as_array()?.get(*index)?;

                if let Some(inner) = inner {
                    inner.get_value(value)
                } else {
                    Some(value)
                }
            }
        }
    }

    fn set_inner(&mut self, new_inner: Option<JsonAccess>) {
        if let Some(new_inner) = new_inner {
            match self {
                JsonAccess::Field { inner, .. } => {
                    *inner = Some(Box::new(new_inner));
                }
                JsonAccess::Array { inner, .. } => {
                    *inner = Some(Box::new(new_inner));
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnOptions {
    pub nullable: bool,
    pub trim: bool,
    pub convert: bool
}

impl ColumnOptions {
    pub fn new() -> ColumnOptions {
        ColumnOptions {
            nullable: true,
            trim: false,
            convert: false
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

    pub fn convert(mut self) -> Self {
        self.convert = true;
        self
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
            ColumnDefinition::with_regex("line", 1, "x", ValueType::Int)
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
            ColumnDefinition::with_regex("line", 2, "x", ValueType::Int),
            ColumnDefinition::with_regex("line", 1, "y", ValueType::String)
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
            ColumnDefinition::with_regex("line", 1, "x", ValueType::Int)
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
            ColumnDefinition::with_regex("line", 1, "x", ValueType::Bool),
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
            ColumnDefinition::with_regex("line", 1, "x", ValueType::Float)
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
            ColumnDefinition::with_options(
                ColumnParsing::Regex(RegexPattern { pattern_name: "line".to_string(), group_index: 1 }),
                "x", ValueType::String,
                ColumnOptions::new().trim()
            )
        ]
    ).unwrap();

    let result = table_definition.extract("A: 4711    ");
    assert_eq!(Value::String("4711".to_owned()), result.columns[0]);
}

#[test]
fn test_table_extract_array1() {
    let table_definition = TableDefinition::new(
        "test",
        vec![("line", "([0-9]+), ([0-9]+), ([0-9]+)")],
        vec![
            ColumnDefinition::with_options(
                ColumnParsing::MultiRegex(vec![
                    RegexPattern::new("line".to_owned(), 1),
                    RegexPattern::new("line".to_owned(), 2),
                    RegexPattern::new("line".to_owned(), 3)
                ]),
                "x",
                ValueType::Array(Box::new(ValueType::Int)),
                ColumnOptions::new()
            )
        ]
    ).unwrap();

    let result = table_definition.extract("A: 13, 47, 11");
    assert_eq!(
        Value::Array(ValueType::Int, vec![Value::Int(13), Value::Int(47), Value::Int(11)]),
        result.columns[0]
    );
}

#[test]
fn test_table_extract_array2() {
    let table_definition = TableDefinition::new(
        "test",
        vec![("line", "([0-9]+), ([0-9]+), ([0-9]+)")],
        vec![
            ColumnDefinition::with_options(
                ColumnParsing::MultiRegex(vec![
                    RegexPattern::new("line".to_owned(), 1),
                    RegexPattern::new("line".to_owned(), 2),
                    RegexPattern::new("line".to_owned(), 3)
                ]),
                "x",
                ValueType::Array(Box::new(ValueType::Int)),
                ColumnOptions::new()
            )
        ]
    ).unwrap();

    let result = table_definition.extract("A: haha");
    assert_eq!(
        Value::Null,
        result.columns[0]
    );
}

#[test]
fn test_table_extract_array3() {
    let table_definition = TableDefinition::new(
        "test",
        vec![("line", "([0-9]+), ([0-9]+), ([0-9]+)")],
        vec![
            ColumnDefinition::with_options(
                ColumnParsing::MultiRegex(vec![
                    RegexPattern::new("line".to_owned(), 1),
                    RegexPattern::new("line".to_owned(), 2),
                    RegexPattern::new("line".to_owned(), 3)
                ]),
                "x",
                ValueType::Array(Box::new(ValueType::Int)),
                ColumnOptions::new()
            )
        ]
    ).unwrap();

    let result = table_definition.extract("A: 13, B, 47");
    assert_eq!(
        Value::Null,
        result.columns[0]
    );
}

#[test]
fn test_table_extract_timestamp1() {
    let table_definition = TableDefinition::new(
        "test",
        vec![("line", "([0-9]+)-([0-9]+)-([0-9]+) ([0-9]+):([0-9]+):([0-9]+)")],
        vec![
            ColumnDefinition::with_options(
                ColumnParsing::MultiRegex(vec![
                    RegexPattern::new("line".to_owned(), 1),
                    RegexPattern::new("line".to_owned(), 2),
                    RegexPattern::new("line".to_owned(), 3),
                    RegexPattern::new("line".to_owned(), 4),
                    RegexPattern::new("line".to_owned(), 5),
                    RegexPattern::new("line".to_owned(), 6),
                ]),
                "x",
                ValueType::Timestamp,
                ColumnOptions::new()
            )
        ]
    ).unwrap();

    let result = table_definition.extract("2021-06-01 16:55:11");
    assert_eq!(
        Value::Timestamp(create_timestamp(2021, 6, 1, 16, 55, 11).unwrap()),
        result.columns[0]
    );
}

#[test]
fn test_table_extract_timestamp2() {
    let table_definition = TableDefinition::new(
        "test",
        vec![("line", "([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)")],
        vec![
            ColumnDefinition::with_options(
                ColumnParsing::MultiRegex(vec![
                    RegexPattern::new("line".to_owned(), 6),
                    RegexPattern::new("line".to_owned(), 1),
                    RegexPattern::new("line".to_owned(), 2),
                    RegexPattern::new("line".to_owned(), 3),
                    RegexPattern::new("line".to_owned(), 4),
                    RegexPattern::new("line".to_owned(), 5),
                ]),
                "x",
                ValueType::Timestamp,
                ColumnOptions::new()
            )
        ]
    ).unwrap();

    let result = table_definition.extract("Jul 9 22:53:22 2005");
    assert_eq!(
        Value::Timestamp(create_timestamp(2005, 7, 9, 22, 53, 22).unwrap()),
        result.columns[0]
    );
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
            ColumnDefinition::with_regex("line1", 1, "x", ValueType::Int),
            ColumnDefinition::with_regex("line2", 1, "y", ValueType::String)
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
            ColumnDefinition::with_regex("line1", 1, "x", ValueType::Int),
            ColumnDefinition::with_regex("line2", 1, "y", ValueType::String)
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
            ColumnDefinition::with_regex("line1", 1, "x", ValueType::Int),
            ColumnDefinition::with_regex("line2", 1, "y", ValueType::String)
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
            ColumnDefinition::with_regex("line1", 1, "x", ValueType::Int),
            ColumnDefinition::with_options(ColumnParsing::Regex(RegexPattern { pattern_name: "line2".to_string(), group_index: 1 }), "y", ValueType::String, ColumnOptions::new().not_null())
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
            ColumnDefinition::with_regex("line1", 1, "x", ValueType::Int),
            ColumnDefinition::with_options(ColumnParsing::Regex(RegexPattern { pattern_name: "line2".to_string(), group_index: 1 }), "y", ValueType::String, ColumnOptions::new().not_null())
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
fn test_json_access1() {
    let value = json!({
        "test1": 4711,
        "test2": {
            "test3": 1337
        }
    });

    assert_eq!(
        Some(&serde_json::Value::Number(serde_json::Number::from(4711))),
        JsonAccess::Field { name: "test1".to_owned(), inner: None }.get_value(&value)
    );

    assert_eq!(
        Some(&serde_json::Value::Number(serde_json::Number::from(1337))),
        JsonAccess::Field { name: "test2".to_owned(), inner: Some(Box::new(JsonAccess::Field { name: "test3".to_owned(), inner: None })) }.get_value(&value)
    );
}

#[test]
fn test_json_access2() {
    let value = json!({
        "test1": [1, 2],
        "test2": {
            "test3": [3, 4]
        }
    });

    assert_eq!(
        Some(&serde_json::Value::Number(serde_json::Number::from(1))),
        JsonAccess::Field {
            name: "test1".to_owned(),
            inner: Some(Box::new(JsonAccess::Array { index: 0, inner: None }))
        }.get_value(&value)
    );

    assert_eq!(
        Some(&serde_json::Value::Number(serde_json::Number::from(4))),
        JsonAccess::Field {
            name: "test2".to_owned(),
            inner: Some(Box::new(JsonAccess::Field {
                name: "test3".to_owned(),
                inner: Some(Box::new(JsonAccess::Array { index: 1, inner: None }))
            }))
        }.get_value(&value)
    );
}

#[test]
fn test_table_json1() {
    let table_definition = TableDefinition::new(
        "test",
        vec![],
        vec![
            ColumnDefinition::with_parsing(
                ColumnParsing::Json(JsonAccess::Field { name: "test2".to_owned(), inner: Some(Box::new(JsonAccess::Field { name: "test3".to_owned(), inner: None })) }),
                "x",
                ValueType::Int
            )
        ]
    ).unwrap();

    let result = table_definition.extract(r#"{ "test2": { "test3": 4711 } }"#);
    assert_eq!(Value::Int(4711), result.columns[0]);
}

#[test]
fn test_table_json2() {
    let table_definition = TableDefinition::new(
        "test",
        vec![("line", "([0-9 ]+)")],
        vec![
            ColumnDefinition::with_parsing(
                ColumnParsing::Json(JsonAccess::Field { name: "test2".to_owned(), inner: Some(Box::new(JsonAccess::Field { name: "test3".to_owned(), inner: None })) }),
                "x",
                ValueType::Int
            )
        ]
    ).unwrap();

    let result = table_definition.extract(r#"{ "test2": { "test3": 4711 } }"#);
    assert_eq!(Value::Int(4711), result.columns[0]);
}

#[test]
fn test_table_json3() {
    let table_definition = TableDefinition::new(
        "test",
        vec![],
        vec![
            ColumnDefinition::with_options(
                ColumnParsing::Json(JsonAccess::Field { name: "test2".to_owned(), inner: Some(Box::new(JsonAccess::Field { name: "test3".to_owned(), inner: None })) }),
                "x",
                ValueType::Int,
                ColumnOptions::new().convert()
            )
        ]
    ).unwrap();

    let result = table_definition.extract(r#"{ "test2": { "test3": "4711" } }"#);
    assert_eq!(Value::Int(4711), result.columns[0]);
}

#[test]
fn test_table_json4() {
    let table_definition = TableDefinition::new(
        "test",
        vec![],
        vec![
            ColumnDefinition::with_parsing(
                ColumnParsing::Json(JsonAccess::Field { name: "test1".to_owned(), inner: None }),
                "x",
                ValueType::Array(Box::new(ValueType::Int))
            )
        ]
    ).unwrap();

    let result = table_definition.extract(r#"{ "test1": [1, 2, 3] }"#);
    assert_eq!(Value::Array(ValueType::Int, vec![Value::Int(1), Value::Int(2), Value::Int(3)]), result.columns[0]);
}

#[test]
fn test_table_json5() {
    let table_definition = TableDefinition::new(
        "test",
        vec![],
        vec![
            ColumnDefinition::with_parsing(
                ColumnParsing::Json(JsonAccess::Field { name: "test1".to_owned(), inner: None }),
                "x",
                ValueType::Array(Box::new(ValueType::Bool))
            )
        ]
    ).unwrap();

    let result = table_definition.extract(r#"{ "test1": [1, 2, 3] }"#);
    assert_eq!(Value::Array(ValueType::Bool, vec![Value::Null, Value::Null, Value::Null]), result.columns[0]);
}