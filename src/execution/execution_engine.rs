use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use fnv::FnvHasher;

use crate::data_model::{Row, TableDefinition, Tables};
use crate::execution::{ExecutionError, ExecutionResult, HashMapColumnProvider, ResultRow};
use crate::execution::aggregate_execution::AggregateExecutionEngine;
use crate::execution::select_execution::SelectExecutionEngine;
use crate::model::{AggregateStatement, SelectStatement, Statement, Value};

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

    pub fn execute(&mut self, statement: &Statement, line: String, config: &ExecutionConfig) -> (ExecutionResult<Option<ResultRow>>, bool) {
        match statement {
            Statement::Select(select_statement) => {
                (self.execute_select(&select_statement, line), false)
            }
            Statement::Aggregate(aggregate_statement) => {
                if config.update && config.result {
                    (self.execute_aggregate(&aggregate_statement, line), true)
                } else if config.update && !config.result {
                    (self.execute_aggregate_update(&aggregate_statement, line).map(|_| None), false)
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
                          line: String) -> ExecutionResult<Option<ResultRow>> {
        let table_definition = self.get_table(&select_statement.from)?;
        let select_execution_engine = SelectExecutionEngine::new(&self.tables);
        let row = table_definition.extract(&line);

        if row.any_result() {
            let line_value = Value::String(line);

            select_execution_engine.execute(
                select_statement,
                HashMapColumnProvider::new(self.create_columns_mapping(&table_definition, &row, &line_value))
            )
        } else {
            Ok(None)
        }
    }

    pub fn execute_aggregate(&mut self,
                             aggregate_statement: &AggregateStatement,
                             line: String) -> ExecutionResult<Option<ResultRow>> {
        self.try_clear_aggregate_state(aggregate_statement);

        let table_definition = self.tables.get(&aggregate_statement.from)
            .ok_or_else(|| ExecutionError::TableNotFound(aggregate_statement.from.clone()))?;
        let row = table_definition.extract(&line);

        if row.any_result() {
            let line_value = Value::String(line);

            self.aggregate_execution_engine.execute(
                aggregate_statement,
                HashMapColumnProvider::new(self.create_columns_mapping(&table_definition, &row, &line_value))
            )
        } else {
            Ok(None)
        }
    }

    pub fn execute_aggregate_update(&mut self,
                                    aggregate_statement: &AggregateStatement,
                                    line: String) -> ExecutionResult<bool> {
        self.try_clear_aggregate_state(aggregate_statement);

        let table_definition = self.tables.get(&aggregate_statement.from)
            .ok_or_else(|| ExecutionError::TableNotFound(aggregate_statement.from.clone()))?;

        let row = table_definition.extract(&line);

        if row.any_result() {
            let line_value = Value::String(line);

            self.aggregate_execution_engine.execute_update(
                aggregate_statement,
                HashMapColumnProvider::new(self.create_columns_mapping(&table_definition, &row, &line_value))
            )?;

            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn execute_aggregate_result(&self, aggregate_statement: &AggregateStatement) -> ExecutionResult<ResultRow> {
        self.aggregate_execution_engine.execute_result(aggregate_statement)
    }

    fn create_columns_mapping(&self, table_definition: &'a TableDefinition, row: &'a Row, line: &'a Value) -> HashMap<&'a str, &'a Value> {
        let mut columns_mapping = HashMap::new();
        for (column_index, column) in table_definition.columns.iter().enumerate() {
            columns_mapping.insert(column.name.as_str(), &row.columns[column_index]);
        }

        columns_mapping.insert("input", line);
        columns_mapping
    }

    fn get_table(&self, name: &str) -> ExecutionResult<&TableDefinition> {
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