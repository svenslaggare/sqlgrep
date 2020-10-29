use std::collections::HashMap;

use crate::data_model::{Tables, TableDefinition, Row};
use crate::model::{SelectStatement, Value};
use crate::select_execution::{SelectExecutionResult, SelectExecutionEngine, SelectExecutionError};
use crate::execution_model::HashMapRefColumnProvider;

pub struct ProcessEngine<'a> {
    tables: &'a Tables
}

impl<'a> ProcessEngine<'a> {
    pub fn new(tables: &'a Tables) -> ProcessEngine<'a> {
        ProcessEngine {
            tables
        }
    }

    pub fn get_table(&self, name: &str) -> SelectExecutionResult<&TableDefinition> {
        self.tables.get(&name).ok_or(SelectExecutionError::TableNotFound)
    }

    pub fn process_select(&self,
                          table_definition: &TableDefinition,
                          select_statement: &SelectStatement,
                          line: String) -> SelectExecutionResult<Option<Row>> {
        let select_execution_engine = SelectExecutionEngine::new();
        let row = table_definition.extract(&line);

        if row.any_result() {
            let mut columns_mapping = HashMap::new();
            for (column_index, column) in table_definition.columns.iter().enumerate() {
                columns_mapping.insert(column.name.as_str(), &row.columns[column_index]);
            }

            let line_value = Value::String(line);
            columns_mapping.insert("input", &line_value);

            let rows_result = select_execution_engine.execute(
                &select_statement,
                HashMapRefColumnProvider::new(columns_mapping)
            )?;

            return Ok(rows_result);
        }

        Ok(None)
    }
}