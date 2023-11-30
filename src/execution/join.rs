use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::io::{BufReader, BufRead};
use std::fs::File;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::data_model::{Row, TableDefinition};
use crate::execution::{ColumnProvider, ExecutionError, ExecutionResult, ResultRow};
use crate::execution::column_providers::HashMapColumnProvider;
use crate::execution::execution_engine::{ExecutionEngine, ExecutionConfig, ExecutionOutput};
use crate::model::{JoinClause, Value, SelectStatement, ExpressionTree, Statement};
use crate::Tables;

pub struct JoinedTableData {
    pub column_names: Vec<String>,
    pub fully_qualified_column_names: Vec<String>,
    rows: HashMap<Value, Vec<Row>>
}

impl JoinedTableData {
    pub fn new(table: &TableDefinition) -> JoinedTableData {
        JoinedTableData {
            column_names: table.columns.iter().map(|column| column.name.clone()).collect(),
            fully_qualified_column_names: table.fully_qualified_column_names.clone(),
            rows: HashMap::new()
        }
    }

    pub fn execute(tables: &Tables,
                   running: Arc<AtomicBool>,
                   join: &JoinClause) -> ExecutionResult<JoinedTableData> {
        let join_statement = Statement::Select(SelectStatement {
            projections: vec![("wildcard".to_owned(), ExpressionTree::Wildcard)],
            from: join.joined_table.clone(),
            filename: None,
            filter: None,
            join: None,
            limit: None,
            distinct: false,
        });

        let mut execution_engine = ExecutionEngine::new(tables, &join_statement);

        let joined_table = execution_engine.get_table(&join.joined_table)?.clone();
        let join_on_column_index = joined_table.index_for(&join.joined_column)
            .ok_or(ExecutionError::ColumnNotFound(join.joiner_column.clone()))?;

        let mut joined_table_data = JoinedTableData::new(&joined_table);

        let config = ExecutionConfig::default();

        let joined_file = File::open(&join.joined_filename)
            .map_err(|err| ExecutionError::FailOpenFile(format!("{}", err)))?;

        for (line_number, line) in BufReader::new(joined_file).lines().enumerate() {
            if line_number > 0 && line_number % 10 == 0 {
                if !running.load(Ordering::SeqCst) {
                    break;
                }
            }

            if let Ok(line) = line {
                let result = execution_engine.execute(line.clone(), &config)?.result_row;
                if let Some(result) = result {
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

        Ok(joined_table_data)
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

pub fn execute_join<F: FnMut(HashMapColumnProvider) -> ExecutionResult<Option<ResultRow>>>(table_definition: &TableDefinition,
                                                                                           row: &Row,
                                                                                           line_value: &Value,
                                                                                           join_clause: &JoinClause,
                                                                                           joined_table_data: &JoinedTableData,
                                                                                           allow_outer: bool,
                                                                                           mut execute: F) -> ExecutionResult<ExecutionOutput> {
    if let Some(joined_rows) = joined_table_data.get_joined_row(table_definition,
                                                                &row,
                                                                &join_clause.joiner_column)? {
        let mut result_row = None;
        for joined_row in joined_rows {
            let column_provider = create_joined_column_mapping(
                table_definition,
                row,
                line_value,
                joined_table_data,
                joined_row
            );

            let result = execute(column_provider)?;
            extend_option_result_row(&mut result_row, result);
        }

        Ok(ExecutionOutput::joined(result_row))
    } else {
        if join_clause.is_outer && allow_outer {
            let null_row = Row::new(vec![Value::Null; joined_table_data.fully_qualified_column_names.len()]);
            let column_provider = create_joined_column_mapping(
                table_definition,
                row,
                line_value,
                joined_table_data,
                &null_row
            );

            Ok(ExecutionOutput::joined(execute(column_provider)?))
        } else {
            Ok(ExecutionOutput::empty())
        }
    }
}

fn create_joined_column_mapping<'a>(table_definition: &'a TableDefinition,
                                    row: &'a Row,
                                    line_value: &'a Value,
                                    joined_table_data: &'a JoinedTableData,
                                    joined_row: &'a Row) -> HashMapColumnProvider<'a> {
    let mut columns_mapping = ExecutionEngine::create_columns_mapping(table_definition, row, line_value);

    for (index, value) in joined_row.columns.iter().enumerate() {
        let name = &joined_table_data.column_names[index];
        if !columns_mapping.contains_key(name.as_str()) {
            columns_mapping.insert(name, value);
        }

        columns_mapping.insert(&joined_table_data.fully_qualified_column_names[index], value);
    }

    let mut column_provider = HashMapColumnProvider::with_table_keys(
        HashMapColumnProvider::create_table_scope(columns_mapping),
        table_definition
    );

    let keys = HashSet::<String>::from_iter(column_provider.keys().iter().cloned());
    for (column_index, column_name) in joined_table_data.column_names.iter().enumerate() {
        if keys.contains(column_name) {
            column_provider.add_key(&joined_table_data.fully_qualified_column_names[column_index]);
        } else {
            column_provider.add_key(column_name);
        }
    }

    column_provider
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