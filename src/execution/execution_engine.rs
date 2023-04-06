use std::collections::{HashMap};
use std::hash::{Hash, Hasher};
use std::io::{BufReader, BufRead};
use std::fs::File;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use fnv::FnvHasher;

use crate::data_model::{ColumnDefinition, ColumnParsing, JsonAccess, RegexResultReference, Row, TableDefinition, Tables, RegexMode};
use crate::execution::{ExecutionError, ExecutionResult, HashMapColumnProvider, ResultRow};
use crate::execution::aggregate_execution::AggregateExecutionEngine;
use crate::execution::join::{execute_join, JoinedTableData};
use crate::execution::select_execution::SelectExecutionEngine;
use crate::model::{AggregateStatement, CompareOperator, create_timestamp, ExpressionTree, SelectStatement, Statement, Value, ValueType, NullableCompareOperator};

pub struct ExecutionConfig {
    pub result: bool,
    pub update: bool
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        ExecutionConfig {
            result: true,
            update: true
        }
    }
}

pub struct ExecutionEngine<'a> {
    tables: &'a Tables,
    aggregate_statement_hash: Option<u64>,
    aggregate_execution_engine: AggregateExecutionEngine
}

impl<'a> ExecutionEngine<'a> {
    pub fn new(tables: &'a Tables) -> ExecutionEngine<'a> {
        ExecutionEngine {
            tables,
            aggregate_statement_hash: None,
            aggregate_execution_engine: AggregateExecutionEngine::new()
        }
    }

    pub fn execute(&mut self,
                   statement: &Statement,
                   line: String,
                   config: &ExecutionConfig,
                   joined_table_data: Option<&JoinedTableData>) -> ExecutionResult<(Option<ResultRow>, bool)> {
        match statement {
            Statement::Select(select_statement) => {
                Ok((self.execute_select(&select_statement, line, joined_table_data)?, false))
            }
            Statement::Aggregate(aggregate_statement) => {
                if config.update && config.result {
                    Ok((self.execute_aggregate(&aggregate_statement, line, joined_table_data)?, true))
                } else if config.update && !config.result {
                    Ok((self.execute_aggregate_update(&aggregate_statement, line, joined_table_data).map(|_| None)?, false))
                } else if !config.update && config.result {
                    Ok((self.execute_aggregate_result(&aggregate_statement).map(|x| Some(x))?, true))
                } else {
                    Err(ExecutionError::NotSupportedOperation)
                }
            }
            Statement::CreateTable(_) => {
                Err(ExecutionError::NotSupportedOperation)
            }
            Statement::Multiple(_) => {
                Err(ExecutionError::NotSupportedOperation)
            }
        }
    }

    fn execute_select(&mut self,
                      select_statement: &SelectStatement,
                      line: String,
                      joined_table_data: Option<&JoinedTableData>) -> ExecutionResult<Option<ResultRow>> {
        let table_definition = self.get_table(&select_statement.from)?;
        let select_execution_engine = SelectExecutionEngine::new();
        let row = table_definition.extract(&line);

        if row.any_result() {
            let line_value = Value::String(line);

            if let Some(joined_table_data) = joined_table_data {
                Ok(
                    execute_join(
                        table_definition,
                        &row,
                        &line_value,
                        select_statement.join.as_ref().unwrap(),
                        joined_table_data,
                        true,
                        |column_provider| {
                            select_execution_engine.execute(select_statement, column_provider)
                        }
                    )?.0
                )
            } else {
                select_execution_engine.execute(
                    select_statement,
                    HashMapColumnProvider::with_table_keys(
                        ExecutionEngine::create_columns_mapping(&table_definition, &row, &line_value),
                        table_definition
                    )
                )
            }
        } else {
            Ok(None)
        }
    }

    fn execute_aggregate(&mut self,
                         aggregate_statement: &AggregateStatement,
                         line: String,
                         joined_table_data: Option<&JoinedTableData>) -> ExecutionResult<Option<ResultRow>> {
        self.try_clear_aggregate_state(aggregate_statement);

        let table_definition = self.tables.get(&aggregate_statement.from)
            .ok_or_else(|| ExecutionError::TableNotFound(aggregate_statement.from.clone()))?;
        let row = table_definition.extract(&line);

        if row.any_result() {
            let line_value = Value::String(line);

            if let Some(joined_table_data) = joined_table_data {
                Ok(
                    execute_join(
                        table_definition,
                        &row,
                        &line_value,
                        aggregate_statement.join.as_ref().unwrap(),
                        joined_table_data,
                        false,
                        |column_provider| {
                            self.aggregate_execution_engine.execute(
                                aggregate_statement,
                                column_provider
                            )
                        }
                    )?.0
                )
            } else {
                self.aggregate_execution_engine.execute(
                    aggregate_statement,
                    HashMapColumnProvider::with_table_keys(
                        ExecutionEngine::create_columns_mapping(&table_definition, &row, &line_value),
                        table_definition
                    )
                )
            }
        } else {
            Ok(None)
        }
    }

    fn execute_aggregate_update(&mut self,
                                aggregate_statement: &AggregateStatement,
                                line: String,
                                joined_table_data: Option<&JoinedTableData>) -> ExecutionResult<bool> {
        self.try_clear_aggregate_state(aggregate_statement);

        let table_definition = self.tables.get(&aggregate_statement.from)
            .ok_or_else(|| ExecutionError::TableNotFound(aggregate_statement.from.clone()))?;

        let row = table_definition.extract(&line);

        if row.any_result() {
            let line_value = Value::String(line);

            if let Some(joined_table_data) = joined_table_data {
                let (_, joined) = execute_join(
                    table_definition,
                    &row,
                    &line_value,
                    aggregate_statement.join.as_ref().unwrap(),
                    joined_table_data,
                    false,
                    |column_provider| {
                        self.aggregate_execution_engine.execute_update(
                            aggregate_statement,
                            column_provider
                        )?;

                        Ok(None)
                    }
                )?;

                if !joined {
                    return Ok(false);
                }
            } else {
                self.aggregate_execution_engine.execute_update(
                    aggregate_statement,
                    HashMapColumnProvider::with_table_keys(
                        ExecutionEngine::create_columns_mapping(&table_definition, &row, &line_value),
                        table_definition
                    )
                )?;
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn execute_aggregate_result(&self, aggregate_statement: &AggregateStatement) -> ExecutionResult<ResultRow> {
        self.aggregate_execution_engine.execute_result(aggregate_statement)
    }

    pub fn create_joined_data(&mut self, statement: &Statement, running: Arc<AtomicBool>) -> ExecutionResult<Option<JoinedTableData>> {
        Ok(
            match statement.join_clause() {
                Some(join_clause) => Some(JoinedTableData::execute(self, running.clone(), join_clause)?),
                None => None,
            }
        )
    }

    pub fn create_columns_mapping(table_definition: &'a TableDefinition,
                                  row: &'a Row,
                                  line: &'a Value) -> HashMap<&'a str, &'a Value> {
        let mut columns_mapping = HashMap::new();
        for (column_index, column) in table_definition.columns.iter().enumerate() {
            columns_mapping.insert(column.name.as_str(), &row.columns[column_index]);
            columns_mapping.insert(&table_definition.fully_qualified_column_names[column_index], &row.columns[column_index]);
        }

        columns_mapping.insert("input", line);
        columns_mapping
    }

    pub fn get_table(&self, name: &str) -> ExecutionResult<&TableDefinition> {
        self.tables.get(&name).ok_or_else(|| ExecutionError::TableNotFound(name.to_owned()))
    }

    fn try_clear_aggregate_state(&mut self, aggregate_statement: &AggregateStatement) {
        let mut hasher = FnvHasher::default();
        aggregate_statement.hash(&mut hasher);
        let hash = hasher.finish();

        if let Some(current_hash) = self.aggregate_statement_hash {
            if current_hash != hash {
                self.aggregate_execution_engine.clear();
            }
        }

        self.aggregate_statement_hash = Some(hash);
    }
}

#[test]
fn test_regex_array1() {
    let mut tables = Tables::new();
    tables.add_table(
        TableDefinition::new(
            "connections",
            vec![
                ("line", "connection from ([0-9.]+) \\((.+)?\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)", RegexMode::Captures)
            ],
            vec![
                ColumnDefinition::with_regex("line", 1, "ip", ValueType::String),
                ColumnDefinition::with_regex("line", 2, "hostname", ValueType::String),
                ColumnDefinition::with_parsing(
                    ColumnParsing::MultiRegex(vec![
                        RegexResultReference { pattern_name: "line".to_string(), group_index: 9 },
                        RegexResultReference { pattern_name: "line".to_string(), group_index: 4 },
                        RegexResultReference { pattern_name: "line".to_string(), group_index: 5 },
                        RegexResultReference { pattern_name: "line".to_string(), group_index: 6 },
                        RegexResultReference { pattern_name: "line".to_string(), group_index: 7 },
                        RegexResultReference { pattern_name: "line".to_string(), group_index: 8 },
                    ]),
                    "datetime",
                    ValueType::Array(Box::new(ValueType::String))
                )
            ],
        ).unwrap()
    );

    let mut execution_engine = ExecutionEngine::new(&tables);
    let statement = Statement::Select(SelectStatement {
        projections: vec![
            ("ip".to_owned(), ExpressionTree::ColumnAccess("ip".to_owned())),
            ("hostname".to_owned(), ExpressionTree::ColumnAccess("hostname".to_owned())),
            ("datetime".to_owned(), ExpressionTree::ColumnAccess("datetime".to_owned())),
        ],
        from: "connections".to_string(),
        filename: None,
        filter: Some(ExpressionTree::Compare {
            operator: CompareOperator::Equal,
            left: Box::new(ExpressionTree::ColumnAccess("hostname".to_owned())),
            right: Box::new(ExpressionTree::Value(Value::String("lns-vlq-45-tou-82-252-162-81.adsl.proxad.net".to_owned())))
        }),
        join: None
    });

    let mut result_rows = Vec::new();
    for line in BufReader::new(File::open("testdata/ftpd_data.txt").unwrap()).lines() {
        let (result, _) = execution_engine.execute(
            &statement,
            line.unwrap(),
            &ExecutionConfig::default(),
            None
        ).unwrap();

        if let Some(result) = result {
            result_rows.extend(result.data);
        }
    }

    assert_eq!(22, result_rows.len());

    assert_eq!(Value::String("82.252.162.81".to_owned()), result_rows[0].columns[0]);
    assert_eq!(Value::String("lns-vlq-45-tou-82-252-162-81.adsl.proxad.net".to_owned()), result_rows[0].columns[1]);
    assert_eq!(
        Value::Array(ValueType::String, vec![Value::String("2005".to_owned()), Value::String("Jun".to_owned()), Value::String("17".to_owned()), Value::String("20".to_owned()), Value::String("55".to_owned()), Value::String("06".to_owned())]),
        result_rows[0].columns[2]
    );

    assert_eq!(Value::String("82.252.162.81".to_owned()), result_rows[21].columns[0]);
    assert_eq!(Value::String("lns-vlq-45-tou-82-252-162-81.adsl.proxad.net".to_owned()), result_rows[21].columns[1]);
    assert_eq!(
        Value::Array(ValueType::String, vec![Value::String("2005".to_owned()), Value::String("Jun".to_owned()), Value::String("18".to_owned()), Value::String("02".to_owned()), Value::String("08".to_owned()), Value::String("12".to_owned())]),
        result_rows[21].columns[2]
    );
}

#[test]
fn test_timestamp1() {
    let mut tables = Tables::new();
    tables.add_table(
        TableDefinition::new(
            "connections",
            vec![
                ("line", "connection from ([0-9.]+) \\((.+)?\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)", RegexMode::Captures)
            ],
            vec![
                ColumnDefinition::with_regex("line", 1, "ip", ValueType::String),
                ColumnDefinition::with_regex("line", 2, "hostname", ValueType::String),
                ColumnDefinition::with_parsing(
                    ColumnParsing::MultiRegex(vec![
                        RegexResultReference { pattern_name: "line".to_string(), group_index: 9 },
                        RegexResultReference { pattern_name: "line".to_string(), group_index: 4 },
                        RegexResultReference { pattern_name: "line".to_string(), group_index: 5 },
                        RegexResultReference { pattern_name: "line".to_string(), group_index: 6 },
                        RegexResultReference { pattern_name: "line".to_string(), group_index: 7 },
                        RegexResultReference { pattern_name: "line".to_string(), group_index: 8 },
                    ]),
                    "timestamp",
                    ValueType::Timestamp
                )
            ],
        ).unwrap()
    );

    let mut execution_engine = ExecutionEngine::new(&tables);
    let statement = Statement::Select(SelectStatement {
        projections: vec![
            ("ip".to_owned(), ExpressionTree::ColumnAccess("ip".to_owned())),
            ("hostname".to_owned(), ExpressionTree::ColumnAccess("hostname".to_owned())),
            ("timestamp".to_owned(), ExpressionTree::ColumnAccess("timestamp".to_owned())),
        ],
        from: "connections".to_string(),
        filename: None,
        filter: Some(ExpressionTree::Compare {
            operator: CompareOperator::Equal,
            left: Box::new(ExpressionTree::ColumnAccess("hostname".to_owned())),
            right: Box::new(ExpressionTree::Value(Value::String("lns-vlq-45-tou-82-252-162-81.adsl.proxad.net".to_owned())))
        }),
        join: None
    });

    let mut result_rows = Vec::new();
    for line in BufReader::new(File::open("testdata/ftpd_data.txt").unwrap()).lines() {
        let (result, _) = execution_engine.execute(
            &statement,
            line.unwrap(),
            &ExecutionConfig::default(),
            None
        ).unwrap();

        if let Some(result) = result {
            result_rows.extend(result.data);
        }
    }

    assert_eq!(22, result_rows.len());

    assert_eq!(Value::String("82.252.162.81".to_owned()), result_rows[0].columns[0]);
    assert_eq!(Value::String("lns-vlq-45-tou-82-252-162-81.adsl.proxad.net".to_owned()), result_rows[0].columns[1]);
    assert_eq!(
        Value::Timestamp(create_timestamp(2005, 6, 17, 20, 55, 6, 0).unwrap()),
        result_rows[0].columns[2]
    );

    assert_eq!(Value::String("82.252.162.81".to_owned()), result_rows[21].columns[0]);
    assert_eq!(Value::String("lns-vlq-45-tou-82-252-162-81.adsl.proxad.net".to_owned()), result_rows[21].columns[1]);
    assert_eq!(
        Value::Timestamp(create_timestamp(2005, 6, 18, 2, 8, 12, 0).unwrap()),
        result_rows[21].columns[2]
    );
}

#[test]
fn test_json_array1() {
    let mut tables = Tables::new();
    tables.add_table(
        TableDefinition::new(
            "clients",
            vec![],
            vec![
                ColumnDefinition::with_parsing(
                    ColumnParsing::Json(JsonAccess::Field { name: "timestamp".to_owned(), inner: None }),
                    "timestamp",
                    ValueType::Int
                ),
                ColumnDefinition::with_parsing(
                    ColumnParsing::Json(JsonAccess::Field { name: "events".to_owned(), inner: None }),
                    "events",
                    ValueType::Array(Box::new(ValueType::String))
                )
            ],
        ).unwrap()
    );

    let mut execution_engine = ExecutionEngine::new(&tables);
    let statement = Statement::Select(SelectStatement {
        projections: vec![
            ("timestamp".to_owned(), ExpressionTree::ColumnAccess("timestamp".to_owned())),
            ("event".to_owned(), ExpressionTree::ArrayElementAccess {
                array: Box::new(ExpressionTree::ColumnAccess("events".to_owned())),
                index: Box::new(ExpressionTree::Value(Value::Int(1)))
            })
        ],
        from: "clients".to_string(),
        filename: None,
        filter: Some(ExpressionTree::NullableCompare {
            operator: NullableCompareOperator::NotEqual,
            left: Box::new(ExpressionTree::ColumnAccess("events".to_owned())),
            right: Box::new(ExpressionTree::Value(Value::Null))
        }),
        join: None
    });

    let mut result_rows = Vec::new();
    for line in BufReader::new(File::open("testdata/clients_data.json").unwrap()).lines() {
        let (result, _) = execution_engine.execute(
            &statement,
            line.unwrap(),
            &ExecutionConfig::default(),
            None
        ).unwrap();

        if let Some(result) = result {
            result_rows.extend(result.data);
        }
    }

    assert_eq!(3, result_rows.len());

    assert_eq!(Value::Int(1609789423312), result_rows[0].columns[0]);
    assert_eq!(Value::String("started".to_owned()), result_rows[0].columns[1]);

    assert_eq!(Value::Int(1609789423325), result_rows[1].columns[0]);
    assert_eq!(Value::String("stopped".to_owned()), result_rows[1].columns[1]);

    assert_eq!(Value::Int(1609789426639), result_rows[2].columns[0]);
    assert_eq!(Value::Null, result_rows[2].columns[1]);
}