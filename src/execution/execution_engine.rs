use std::collections::{HashMap};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::data_model::{Row, TableDefinition};
use crate::execution::{ExecutionError, ExecutionResult, HashMapColumnProvider, ResultRow};
use crate::execution::aggregate_execution::AggregateExecutionEngine;
use crate::execution::join::{execute_join, JoinedTableData};
use crate::execution::select_execution::SelectExecutionEngine;
use crate::{Statement, Tables};
use crate::model::{AggregateStatement, SelectStatement, Value};

pub struct ExecutionConfig {
    pub result: bool,
    pub update: bool
}

impl ExecutionConfig {
    pub fn aggregate_update() -> ExecutionConfig {
        ExecutionConfig {
            result: false,
            update: true
        }
    }

    pub fn aggregate_result() -> ExecutionConfig {
        ExecutionConfig {
            result: true,
            update: false
        }
    }
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        ExecutionConfig {
            result: true,
            update: true
        }
    }
}

#[derive(Debug)]
pub struct ExecutionOutput {
    pub result_row: Option<ResultRow>,
    pub updated: bool,
    pub joined: bool,
    pub reached_limit: bool
}

impl ExecutionOutput {
    pub fn new(row: Option<ResultRow>) -> ExecutionOutput {
        ExecutionOutput {
            result_row: row,
            updated: false,
            joined: false,
            reached_limit: false
        }
    }

    pub fn joined(row: Option<ResultRow>) -> ExecutionOutput {
        ExecutionOutput {
            result_row: row,
            updated: false,
            joined: true,
            reached_limit: false
        }
    }

    pub fn empty() -> ExecutionOutput {
        ExecutionOutput {
            result_row: None,
            updated: false,
            joined: false,
            reached_limit: false
        }
    }

    pub fn with_updated(self) -> ExecutionOutput {
        let mut new = self;
        new.updated = true;
        new
    }

    pub fn with_reached_limit(self) -> ExecutionOutput {
        let mut new = self;
        new.reached_limit = true;
        new
    }
}

pub struct ExecutionEngine<'a> {
    tables: &'a Tables,
    statement: &'a Statement,
    aggregate_execution_engine: AggregateExecutionEngine,
    num_output_rows: usize,
    joined_table_data: Option<JoinedTableData>
}

impl<'a> ExecutionEngine<'a> {
    pub fn new(tables: &'a Tables, statement: &'a Statement) -> ExecutionEngine<'a> {
        ExecutionEngine {
            tables,
            statement,
            aggregate_execution_engine: AggregateExecutionEngine::new(),
            num_output_rows: 0,
            joined_table_data: None
        }
    }

    pub fn with_executed_joined_table(tables: &'a Tables, statement: &'a Statement) -> ExecutionResult<ExecutionEngine<'a>> {
        let mut engine = ExecutionEngine::new(tables, statement);
        engine.execute_joined_table(Arc::new(AtomicBool::new(true)))?;
        Ok(engine)
    }

    pub fn is_aggregate(&self) -> bool {
        self.statement.is_aggregate()
    }

    pub fn is_join(&self) -> bool {
        self.statement.join_clause().is_some()
    }

    pub fn execution_config(&self) -> ExecutionConfig {
        if self.is_aggregate() {
            ExecutionConfig::aggregate_update()
        } else {
            ExecutionConfig::default()
        }
    }

    pub fn execute(&mut self, line: String, config: &ExecutionConfig) -> ExecutionResult<ExecutionOutput> {
        match self.statement {
            Statement::Select(select_statement) => {
                let output = self.execute_select(&select_statement, line)?;
                let output = self.update_limit(select_statement.limit, output);
                Ok(output)
            }
            Statement::Aggregate(aggregate_statement) => {
                if config.update && config.result {
                    let output = self.execute_aggregate(&aggregate_statement, line)?.with_updated();
                    let output = self.update_limit(aggregate_statement.limit, output);
                    Ok(output)
                } else if config.update && !config.result {
                    self.execute_aggregate_update(&aggregate_statement, line).map(|_| ExecutionOutput::empty())
                } else if !config.update && config.result {
                    let mut output = ExecutionOutput::new(
                        self.execute_aggregate_result(&aggregate_statement).map(|x| Some(x))?
                    ).with_updated();

                    if let Some(limit) = aggregate_statement.limit {
                        if let Some(row) = output.result_row.as_mut() {
                            if row.data.len() > limit {
                                row.data.drain(limit..);
                            }
                        }
                    }

                    Ok(output)
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

    fn execute_select(&mut self, select_statement: &SelectStatement, line: String) -> ExecutionResult<ExecutionOutput> {
        let table_definition = self.get_table(&select_statement.from)?;
        let select_execution_engine = SelectExecutionEngine::new();
        let row = table_definition.extract(&line);

        if row.any_result() {
            let line_value = Value::String(line);

            if let Some(joined_table_data) = self.joined_table_data.as_ref() {
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
                    )?
                )
            } else {
                Ok(
                    ExecutionOutput::new(
                        select_execution_engine.execute(
                            select_statement,
                            HashMapColumnProvider::with_table_keys(
                                ExecutionEngine::create_columns_mapping(&table_definition, &row, &line_value),
                                table_definition
                            )
                        )?
                    )
                )
            }
        } else {
            Ok(ExecutionOutput::empty())
        }
    }

    fn execute_aggregate(&mut self, aggregate_statement: &AggregateStatement, line: String) -> ExecutionResult<ExecutionOutput> {
        let table_definition = self.tables.get(&aggregate_statement.from)
            .ok_or_else(|| ExecutionError::TableNotFound(aggregate_statement.from.clone()))?;
        let row = table_definition.extract(&line);

        if row.any_result() {
            let line_value = Value::String(line);

            if let Some(joined_table_data) = self.joined_table_data.as_ref() {
                let aggregate_execution_engine = &mut self.aggregate_execution_engine;
                Ok(
                    execute_join(
                        table_definition,
                        &row,
                        &line_value,
                        aggregate_statement.join.as_ref().unwrap(),
                        joined_table_data,
                        false,
                        |column_provider| {
                            aggregate_execution_engine.execute(
                                aggregate_statement,
                                column_provider
                            )
                        }
                    )?
                )
            } else {
                let aggregate_execution_engine = &mut self.aggregate_execution_engine;
                Ok(
                    ExecutionOutput::new(
                        aggregate_execution_engine.execute(
                            aggregate_statement,
                            HashMapColumnProvider::with_table_keys(
                                ExecutionEngine::create_columns_mapping(&table_definition, &row, &line_value),
                                table_definition
                            )
                        )?
                    )
                )
            }
        } else {
            Ok(ExecutionOutput::empty())
        }
    }

    fn execute_aggregate_update(&mut self, aggregate_statement: &AggregateStatement, line: String) -> ExecutionResult<bool> {
        let table_definition = self.tables.get(&aggregate_statement.from)
            .ok_or_else(|| ExecutionError::TableNotFound(aggregate_statement.from.clone()))?;

        let row = table_definition.extract(&line);

        if row.any_result() {
            let line_value = Value::String(line);

            if let Some(joined_table_data) = self.joined_table_data.as_ref() {
                let aggregate_execution_engine = &mut self.aggregate_execution_engine;
                let joined = execute_join(
                    table_definition,
                    &row,
                    &line_value,
                    aggregate_statement.join.as_ref().unwrap(),
                    joined_table_data,
                    false,
                    |column_provider| {
                        aggregate_execution_engine.execute_update(
                            aggregate_statement,
                            column_provider
                        )?;

                        Ok(None)
                    }
                )?.joined;

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

    fn update_limit(&mut self, limit: Option<usize>, mut output: ExecutionOutput) -> ExecutionOutput {
        if let Some(row) = output.result_row.as_ref() {
            self.num_output_rows += row.data.iter().filter(|row| row.any_result()).count();
        }

        if let Some(limit) = limit {
            if self.num_output_rows >= limit {
                output = output.with_reached_limit();
            }
        }

        output
    }

    pub fn execute_joined_table(&mut self, running: Arc<AtomicBool>) -> ExecutionResult<()> {
        Ok(
            match self.statement.join_clause() {
                Some(join_clause) => {
                    self.joined_table_data = Some(JoinedTableData::execute(self.tables, running.clone(), join_clause)?);
                },
                None => {
                    self.joined_table_data = None;
                }
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
}

#[test]
fn test_regex_array1() {
    use std::io::{BufReader, BufRead};
    use std::fs::File;
    use crate::data_model::{ColumnDefinition, ColumnParsing, RegexResultReference, TableDefinition, Tables, RegexMode};
    use crate::model::{CompareOperator, ExpressionTree, SelectStatement, Statement, Value, ValueType};

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
        join: None,
        limit: None
    });

    let mut execution_engine = ExecutionEngine::new(&tables, &statement);

    let mut result_rows = Vec::new();
    for line in BufReader::new(File::open("testdata/ftpd_data.txt").unwrap()).lines() {
        let result = execution_engine.execute(
            line.unwrap(),
            &ExecutionConfig::default()
        ).unwrap().result_row;

        if let Some(result) = result {
            result_rows.extend(result.data);
        }
    }

    assert_eq!(22, result_rows.len());

    assert_eq!(Value::String("82.252.162.81".to_owned()), result_rows[0].columns[0]);
    assert_eq!(Value::String("lns-vlq-45-tou-82-252-162-81.adsl.proxad.net".to_owned()), result_rows[0].columns[1]);
    assert_eq!(
        Value::Array(
            ValueType::String,
            vec![Value::String("2005".to_owned()), Value::String("Jun".to_owned()), Value::String("17".to_owned()), Value::String("20".to_owned()), Value::String("55".to_owned()), Value::String("06".to_owned())]
        ),
        result_rows[0].columns[2]
    );

    assert_eq!(Value::String("82.252.162.81".to_owned()), result_rows[21].columns[0]);
    assert_eq!(Value::String("lns-vlq-45-tou-82-252-162-81.adsl.proxad.net".to_owned()), result_rows[21].columns[1]);
    assert_eq!(
        Value::Array(
            ValueType::String,
            vec![Value::String("2005".to_owned()), Value::String("Jun".to_owned()), Value::String("18".to_owned()), Value::String("02".to_owned()), Value::String("08".to_owned()), Value::String("12".to_owned())]
        ),
        result_rows[21].columns[2]
    );
}

#[test]
fn test_timestamp1() {
    use std::io::{BufReader, BufRead};
    use std::fs::File;
    use crate::data_model::{ColumnDefinition, ColumnParsing, RegexResultReference, TableDefinition, Tables, RegexMode};
    use crate::model::{CompareOperator, create_timestamp, ExpressionTree, SelectStatement, Statement, Value, ValueType};

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
        join: None,
        limit: None
    });
    let mut execution_engine = ExecutionEngine::new(&tables, &statement);

    let mut result_rows = Vec::new();
    for line in BufReader::new(File::open("testdata/ftpd_data.txt").unwrap()).lines() {
        let result = execution_engine.execute(
            line.unwrap(),
            &ExecutionConfig::default()
        ).unwrap().result_row;

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
    use std::io::{BufReader, BufRead};
    use std::fs::File;
    use crate::data_model::{ColumnDefinition, ColumnParsing, JsonAccess, TableDefinition, Tables};
    use crate::model::{ExpressionTree, SelectStatement, Statement, Value, ValueType, NullableCompareOperator};

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

    let statement = Statement::Select(
        SelectStatement {
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
            join: None,
            limit: None
        }
    );
    let mut execution_engine = ExecutionEngine::new(&tables, &statement);

    let mut result_rows = Vec::new();
    for line in BufReader::new(File::open("testdata/clients_data.json").unwrap()).lines() {
        let result = execution_engine.execute(
            line.unwrap(),
            &ExecutionConfig::default()
        ).unwrap().result_row;

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

#[test]
fn test_limit1() {
    use std::io::{BufReader, BufRead};
    use std::fs::File;
    use crate::data_model::{ColumnDefinition, ColumnParsing, RegexResultReference, TableDefinition, Tables, RegexMode};
    use crate::model::{CompareOperator, ExpressionTree, SelectStatement, Statement, Value, ValueType};

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
        join: None,
        limit: Some(10)
    });
    let mut execution_engine = ExecutionEngine::new(&tables, &statement);

    let mut result_rows = Vec::new();
    for line in BufReader::new(File::open("testdata/ftpd_data.txt").unwrap()).lines() {
        let output = execution_engine.execute(
            line.unwrap(),
            &ExecutionConfig::default()
        ).unwrap();

        if let Some(result) = output.result_row {
            result_rows.extend(result.data);
        }

        if output.reached_limit {
            break;
        }
    }

    assert_eq!(10, result_rows.len());

    assert_eq!(Value::String("82.252.162.81".to_owned()), result_rows[0].columns[0]);
    assert_eq!(Value::String("lns-vlq-45-tou-82-252-162-81.adsl.proxad.net".to_owned()), result_rows[0].columns[1]);
    assert_eq!(
        Value::Array(
            ValueType::String,
            vec![Value::String("2005".to_owned()), Value::String("Jun".to_owned()), Value::String("17".to_owned()), Value::String("20".to_owned()), Value::String("55".to_owned()), Value::String("06".to_owned())]
        ),
        result_rows[0].columns[2]
    );

    assert_eq!(Value::String("82.252.162.81".to_owned()), result_rows[9].columns[0]);
    assert_eq!(Value::String("lns-vlq-45-tou-82-252-162-81.adsl.proxad.net".to_owned()), result_rows[9].columns[1]);
    assert_eq!(
        Value::Array(
            ValueType::String,
            vec![Value::String("2005".to_owned()), Value::String("Jun".to_owned()), Value::String("18".to_owned()), Value::String("02".to_owned()), Value::String("08".to_owned()), Value::String("10".to_owned())]
        ),
        result_rows[9].columns[2]
    );
}