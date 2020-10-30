use std::collections::HashMap;

use crate::data_model::{Tables, TableDefinition};
use crate::model::{SelectStatement, Value, AggregateStatement};
use crate::execution_model::{HashMapColumnProvider, ExecutionResult, ExecutionError, ResultRow};
use crate::aggregate_execution::AggregateExecutionEngine;
use crate::select_execution::SelectExecutionEngine;

pub struct ProcessEngine<'a> {
    tables: &'a Tables,
    aggregate_execution_engine: AggregateExecutionEngine
}

impl<'a> ProcessEngine<'a> {
    pub fn new(tables: &'a Tables) -> ProcessEngine<'a> {
        ProcessEngine {
            tables,
            aggregate_execution_engine: AggregateExecutionEngine::new()
        }
    }

    pub fn get_table(&self, name: &str) -> ExecutionResult<&TableDefinition> {
        self.tables.get(&name).ok_or(ExecutionError::TableNotFound)
    }

    pub fn process_select(&mut self,
                          select_statement: &SelectStatement,
                          line: String) -> ExecutionResult<Option<ResultRow>> {
        let table_definition = self.get_table(&select_statement.from)?;
        let select_execution_engine = SelectExecutionEngine::new();
        let row = table_definition.extract(&line);

        if row.any_result() {
            let mut columns_mapping = HashMap::new();
            for (column_index, column) in table_definition.columns.iter().enumerate() {
                columns_mapping.insert(column.name.as_str(), &row.columns[column_index]);
            }

            let line_value = Value::String(line);
            columns_mapping.insert("input", &line_value);

            return select_execution_engine.execute(
                select_statement,
                HashMapColumnProvider::new(columns_mapping)
            );
        }

        Ok(None)
    }

    pub fn process_aggregate(&mut self,
                             aggregate_statement: &AggregateStatement,
                             line: String) -> ExecutionResult<Option<ResultRow>> {
        let table_definition = self.tables.get(&aggregate_statement.from).ok_or(ExecutionError::TableNotFound)?;
        let row = table_definition.extract(&line);

        if row.any_result() {
            let mut columns_mapping = HashMap::new();
            for (column_index, column) in table_definition.columns.iter().enumerate() {
                columns_mapping.insert(column.name.as_str(), &row.columns[column_index]);
            }

            let line_value = Value::String(line);
            columns_mapping.insert("input", &line_value);

            return self.aggregate_execution_engine.execute(
                aggregate_statement,
                HashMapColumnProvider::new(columns_mapping)
            );
        }

        Ok(None)
    }
}