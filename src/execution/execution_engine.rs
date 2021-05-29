use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, BufRead};
use std::fs::File;

use fnv::FnvHasher;

use crate::data_model::{Row, TableDefinition, Tables};
use crate::execution::{ExecutionError, ExecutionResult, HashMapColumnProvider, ResultRow};
use crate::execution::aggregate_execution::AggregateExecutionEngine;
use crate::execution::select_execution::SelectExecutionEngine;
use crate::model::{AggregateStatement, SelectStatement, Statement, Value, JoinClause, ExpressionTree};

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
        let select_execution_engine = SelectExecutionEngine::new(&self.tables);
        let row = table_definition.extract(&line);

        if row.any_result() {
            let line_value = Value::String(line);

            if let Some(joined_table_data) = joined_table_data {
                let joiner_on_column_index = table_definition.index_for(&select_statement.join.as_ref().unwrap().joiner_column).unwrap();
                let joiner_on_value = &row.columns[joiner_on_column_index];
                if let Some(joined_rows) = joined_table_data.rows.get(joiner_on_value) {
                    let mut result_row = None;
                    for joined_row in joined_rows {
                        let mut columns_mapping = self.create_columns_mapping(&table_definition, &row, &line_value);

                        for (index, value) in joined_row.columns.iter().enumerate() {
                            columns_mapping.insert(&joined_table_data.table.fully_qualified_column_names[index], value);
                        }

                        let result = select_execution_engine.execute(
                            select_statement,
                            HashMapColumnProvider::new(columns_mapping)
                        )?;

                        if let Some(result) = result {
                            match &mut result_row {
                                None => {
                                    result_row = Some(result);
                                }
                                Some(result_row) => {
                                    result_row.data.extend(result.data);
                                }
                            }
                        }
                    }

                    Ok(result_row)
                } else {
                    return Ok(None);
                }
            } else {
                select_execution_engine.execute(
                    select_statement,
                    HashMapColumnProvider::new(self.create_columns_mapping(&table_definition, &row, &line_value))
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
                let joiner_on_column_index = table_definition.index_for(&aggregate_statement.join.as_ref().unwrap().joiner_column).unwrap();
                let joiner_on_value = &row.columns[joiner_on_column_index];
                if let Some(joined_rows) = joined_table_data.rows.get(joiner_on_value) {
                    let mut result_row = None;
                    for joined_row in joined_rows {
                        let mut columns_mapping = self.create_columns_mapping(&table_definition, &row, &line_value);

                        for (index, value) in joined_row.columns.iter().enumerate() {
                            columns_mapping.insert(&joined_table_data.table.fully_qualified_column_names[index], value);
                        }

                        let result = self.aggregate_execution_engine.execute(
                            aggregate_statement,
                            HashMapColumnProvider::new(columns_mapping)
                        )?;

                        if let Some(result) = result {
                            match &mut result_row {
                                None => {
                                    result_row = Some(result);
                                }
                                Some(result_row) => {
                                    result_row.data.extend(result.data);
                                }
                            }
                        }
                    }

                    Ok(result_row)
                } else {
                    return Ok(None);
                }
            } else {
                self.aggregate_execution_engine.execute(
                    aggregate_statement,
                    HashMapColumnProvider::new(self.create_columns_mapping(&table_definition, &row, &line_value))
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
                let joiner_on_column_index = table_definition.index_for(&aggregate_statement.join.as_ref().unwrap().joiner_column).unwrap();
                let joiner_on_value = &row.columns[joiner_on_column_index];
                if let Some(joined_rows) = joined_table_data.rows.get(joiner_on_value) {
                    for joined_row in joined_rows {
                        let mut columns_mapping = self.create_columns_mapping(&table_definition, &row, &line_value);

                        for (index, value) in joined_row.columns.iter().enumerate() {
                            columns_mapping.insert(&joined_table_data.table.fully_qualified_column_names[index], value);
                        }

                        self.aggregate_execution_engine.execute_update(
                            aggregate_statement,
                            HashMapColumnProvider::new(columns_mapping)
                        )?;
                    }
                } else {
                    return Ok(false);
                }
            } else {
                self.aggregate_execution_engine.execute_update(
                    aggregate_statement,
                    HashMapColumnProvider::new(self.create_columns_mapping(&table_definition, &row, &line_value))
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

            let mut joined_table_data = JoinedTableData::new(joined_table, join_on_column_index);

            let config = ExecutionConfig::default();

            let joined_file = File::open(&join.joined_filename)
                .map_err(|err| ExecutionError::FailOpenFile(format!("{}", err)))?;

            for line in BufReader::new(joined_file).lines() {
                let line = line.unwrap();
                let (result, _) = self.execute(&join_statement, line.clone(), &config, None);

                if let Ok(Some(result)) = result {
                    for row in result.data {
                        let join_on_value = row.columns[join_on_column_index].clone();
                        joined_table_data.rows
                            .entry(join_on_value.clone())
                            .or_insert_with(|| Vec::new())
                            .push(row);
                    }
                }
            }

            Ok(Some(joined_table_data))
        } else {
            Ok(None)
        }
    }

    fn create_columns_mapping(&self, table_definition: &'a TableDefinition, row: &'a Row, line: &'a Value) -> HashMap<&'a str, &'a Value> {
        let mut columns_mapping = HashMap::new();
        for (column_index, column) in table_definition.columns.iter().enumerate() {
            columns_mapping.insert(column.name.as_str(), &row.columns[column_index]);
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
    pub table: TableDefinition,
    pub join_on_column_index: usize,
    pub rows: HashMap<Value, Vec<Row>>
}

impl JoinedTableData {
    pub fn new(table: TableDefinition,
               join_on_column_index: usize,) -> JoinedTableData {
        JoinedTableData {
            table,
            join_on_column_index,
            rows: HashMap::new()
        }
    }
}