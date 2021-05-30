use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io::{BufReader, BufRead};
use std::fs::File;
use std::iter::FromIterator;

use fnv::FnvHasher;

use crate::data_model::{Row, TableDefinition, Tables, ColumnDefinition, ColumnParsing, JsonAccess};
use crate::execution::{ExecutionError, ExecutionResult, HashMapColumnProvider, ResultRow, ColumnProvider};
use crate::execution::aggregate_execution::AggregateExecutionEngine;
use crate::execution::select_execution::SelectExecutionEngine;
use crate::model::{AggregateStatement, SelectStatement, Statement, Value, JoinClause, ExpressionTree, CompareOperator, ValueType};

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
                   joined_table_data: Option<&JoinedTableData>) -> (ExecutionResult<Option<ResultRow>>, bool) {
        match statement {
            Statement::Select(select_statement) => {
                (self.execute_select(&select_statement, line, joined_table_data), false)
            }
            Statement::Aggregate(aggregate_statement) => {
                if config.update && config.result {
                    (self.execute_aggregate(&aggregate_statement, line, joined_table_data), true)
                } else if config.update && !config.result {
                    (self.execute_aggregate_update(&aggregate_statement, line, joined_table_data).map(|_| None), false)
                } else if !config.update && config.result {
                    (self.execute_aggregate_result(&aggregate_statement).map(|x| Some(x)), true)
                } else {
                    (Err(ExecutionError::NotSupportedOperation), false)
                }
            }
            Statement::CreateTable(_) => {
                (Err(ExecutionError::NotSupportedOperation), false)
            }
            Statement::Multiple(_) => {
                (Err(ExecutionError::NotSupportedOperation), false)
            }
        }
    }

    pub fn execute_select(&mut self,
                          select_statement: &SelectStatement,
                          line: String,
                          joined_table_data: Option<&JoinedTableData>) -> ExecutionResult<Option<ResultRow>> {
        let table_definition = self.get_table(&select_statement.from)?;
        let select_execution_engine = SelectExecutionEngine::new();
        let row = table_definition.extract(&line);

        if row.any_result() {
            let line_value = Value::String(line);

            if let Some(joined_table_data) = joined_table_data {
                let join_clause = select_statement.join.as_ref().unwrap();
                if let Some(joined_rows) = joined_table_data.get_joined_row(table_definition,
                                                                            &row,
                                                                            &join_clause.joiner_column)? {
                    let mut result_row = None;
                    for joined_row in joined_rows {
                        let column_provider = self.create_joined_column_mapping(
                            table_definition,
                            &row,
                            &line_value,
                            &joined_table_data,
                            joined_row
                        );

                        let result = select_execution_engine.execute(select_statement, column_provider)?;
                        extend_option_result_row(&mut result_row, result);
                    }

                    Ok(result_row)
                } else {
                    if join_clause.is_outer {
                        let null_row = Row::new(vec![Value::Null; joined_table_data.fully_qualified_column_names.len()]);
                        let column_provider = self.create_joined_column_mapping(
                            table_definition,
                            &row,
                            &line_value,
                            &joined_table_data,
                            &null_row
                        );

                        select_execution_engine.execute(select_statement, column_provider)
                    } else {
                        return Ok(None);
                    }
                }
            } else {
                select_execution_engine.execute(
                    select_statement,
                    HashMapColumnProvider::with_table_keys(
                        self.create_columns_mapping(&table_definition, &row, &line_value),
                        table_definition
                    )
                )
            }
        } else {
            Ok(None)
        }
    }

    pub fn execute_aggregate(&mut self,
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
                if let Some(joined_rows) = joined_table_data.get_joined_row(table_definition,
                                                                            &row,
                                                                            &aggregate_statement.join.as_ref().unwrap().joiner_column)? {
                    let mut result_row = None;
                    for joined_row in joined_rows {
                        let column_provider = self.create_joined_column_mapping(
                            table_definition,
                            &row,
                            &line_value,
                            &joined_table_data,
                            joined_row
                        );

                        let result = self.aggregate_execution_engine.execute(aggregate_statement, column_provider)?;
                        extend_option_result_row(&mut result_row, result);
                    }

                    Ok(result_row)
                } else {
                    return Ok(None);
                }
            } else {
                self.aggregate_execution_engine.execute(
                    aggregate_statement,
                    HashMapColumnProvider::with_table_keys(
                        self.create_columns_mapping(&table_definition, &row, &line_value),
                        table_definition
                    )
                )
            }
        } else {
            Ok(None)
        }
    }

    pub fn execute_aggregate_update(&mut self,
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
                if let Some(joined_rows) = joined_table_data.get_joined_row(table_definition,
                                                                            &row,
                                                                            &aggregate_statement.join.as_ref().unwrap().joiner_column)? {
                    for joined_row in joined_rows {
                        let column_provider = self.create_joined_column_mapping(
                            table_definition,
                            &row,
                            &line_value,
                            &joined_table_data,
                            joined_row
                        );

                        self.aggregate_execution_engine.execute_update(
                            aggregate_statement,
                            column_provider
                        )?;
                    }
                } else {
                    return Ok(false);
                }
            } else {
                self.aggregate_execution_engine.execute_update(
                    aggregate_statement,
                    HashMapColumnProvider::with_table_keys(
                        self.create_columns_mapping(&table_definition, &row, &line_value),
                        table_definition
                    )
                )?;
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn execute_aggregate_result(&self, aggregate_statement: &AggregateStatement) -> ExecutionResult<ResultRow> {
        self.aggregate_execution_engine.execute_result(aggregate_statement)
    }

    pub fn get_joined_data(&mut self, join: Option<&JoinClause>) -> ExecutionResult<Option<JoinedTableData>> {
        if let Some(join) = join {
            let join_statement = Statement::Select(SelectStatement {
                projections: vec![("wildcard".to_owned(), ExpressionTree::Wildcard)],
                from: join.joined_table.clone(),
                filename: None,
                filter: None,
                join: None
            });

            let joined_table = self.get_table(&join.joined_table)?.clone();
            let join_on_column_index = joined_table.index_for(&join.joined_column)
                .ok_or(ExecutionError::ColumnNotFound(join.joiner_column.clone()))?;

            let mut joined_table_data = JoinedTableData::new(&joined_table, join_on_column_index);

            let config = ExecutionConfig::default();

            let joined_file = File::open(&join.joined_filename)
                .map_err(|err| ExecutionError::FailOpenFile(format!("{}", err)))?;

            for line in BufReader::new(joined_file).lines() {
                if let Ok(line) = line {
                    let (result, _) = self.execute(&join_statement, line.clone(), &config, None);
                    if let Some(result) = result? {
                        for row in result.data {
                            joined_table_data.add_row(
                                row.columns[join_on_column_index].clone(),
                                row
                            );
                        }
                    }
                } else {
                    break;
                }
            }

            Ok(Some(joined_table_data))
        } else {
            Ok(None)
        }
    }

    fn create_joined_column_mapping(&self,
                                    table_definition: &'a TableDefinition,
                                    row: &'a Row,
                                    line_value: &'a Value,
                                    joined_table_data: &'a JoinedTableData,
                                    joined_row: &'a Row) -> HashMapColumnProvider<'a> {
        let mut columns_mapping = self.create_columns_mapping(table_definition, row, line_value);

        for (index, value) in joined_row.columns.iter().enumerate() {
            let name = &joined_table_data.column_names[index];
            if !columns_mapping.contains_key(name.as_str()) {
                columns_mapping.insert(name, value);
            }

            columns_mapping.insert(&joined_table_data.fully_qualified_column_names[index], value);
        }

        let mut column_provider = HashMapColumnProvider::with_table_keys(columns_mapping, table_definition);
        let keys = HashSet::<String>::from_iter(column_provider.keys.iter().cloned());
        for (column_index, column_name) in joined_table_data.column_names.iter().enumerate() {
            if keys.contains(column_name) {
                column_provider.add_key(&joined_table_data.fully_qualified_column_names[column_index]);
            } else {
                column_provider.add_key(column_name);
            }
        }

        column_provider
    }

    fn create_columns_mapping(&self,
                              table_definition: &'a TableDefinition,
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

pub struct JoinedTableData{
    pub column_names: Vec<String>,
    pub fully_qualified_column_names: Vec<String>,
    pub join_on_column_index: usize,
    pub rows: HashMap<Value, Vec<Row>>
}

impl JoinedTableData {
    pub fn new(table: &TableDefinition,
               join_on_column_index: usize) -> JoinedTableData {
        JoinedTableData {
            column_names: table.columns.iter().map(|column| column.name.clone()).collect(),
            fully_qualified_column_names: table.fully_qualified_column_names.clone(),
            join_on_column_index,
            rows: HashMap::new()
        }
    }

    pub fn add_row(&mut self, join_on_value: Value, row: Row) {
        self.rows
            .entry(join_on_value)
            .or_insert_with(|| Vec::new())
            .push(row);
    }

    pub fn get_joined_row(&self,
                          joiner_table: &TableDefinition,
                          joined_row: &Row,
                          joiner_column: &str) -> ExecutionResult<Option<&Vec<Row>>> {
        let joiner_on_column_index = joiner_table.index_for(joiner_column)
            .ok_or_else(|| ExecutionError::ColumnNotFound(joiner_column.to_owned()))?;
        let joiner_on_value = &joined_row.columns[joiner_on_column_index];
        Ok(self.rows.get(joiner_on_value))
    }
}

fn extend_option_result_row(result_row: &mut Option<ResultRow>, result: Option<ResultRow>) {
    if let Some(result) = result {
        match result_row {
            None => {
                *result_row = Some(result);
            }
            Some(result_row) => {
                result_row.data.extend(result.data);
            }
        }
    }
}

#[test]
fn test_json_array1() {
    let mut tables = Tables::new();
    tables.add_table(
        "clients",
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
        filter: Some(ExpressionTree::IsNot {
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
        );

        if let Some(result) = result.unwrap() {
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