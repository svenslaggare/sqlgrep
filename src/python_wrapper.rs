use std::fs::File;
use std::io::{BufRead, BufReader, Lines};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use chrono::{Datelike, Timelike};
use fnv::FnvHashMap;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDateTime, PyDelta, PyDict, PyIterator};

use crate::{ExecutionEngine, parsing, Statement, Tables};
use crate::data_model::{Row, TableDefinition};
use crate::execution::execution_engine::{ExecutionConfig};
use crate::helpers::FollowFileIterator;
use crate::model::Value;

#[pymodule]
fn libsqlgrep(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(compile_query))?;
    m.add_class::<TablesWrapper>()?;
    m.add_class::<StatementWrapper>()?;
    m.add_class::<ReadLinesIterator>()?;
    m.add_class::<FollowFileIteratorWrapper>()?;
    Ok(())
}

macro_rules! map_py_err {
    ($var:expr, $message:expr) => {
        {
           $var.map_err(|err| PyValueError::new_err(format!($message, err)))
        }
    };
}

#[pyclass(name="Tables")]
struct TablesWrapper {
    tables: Tables
}

#[pymethods]
impl TablesWrapper {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(
            TablesWrapper {
                tables: Tables::new()
            }
        )
    }

    fn add_table(&mut self, text: String) -> PyResult<()> {
        let table_definition = map_py_err!(parsing::parse(&text), "Failed to parse table: {}")?;
        if self.tables.add_tables(table_definition) {
            Ok(())
        } else {
            Err(PyValueError::new_err("Expected 'CREATE TABLE' statement."))
        }
    }

    fn tables(&self) -> PyResult<Vec<TableWrapper>> {
        let mut tables = Vec::new();
        for table in self.tables.tables() {
            tables.push(TableWrapper::new(table)?);
        }

        Ok(tables)
    }

    fn table_names(&self) -> PyResult<Vec<String>> {
        Ok(self.tables.tables().map(|table| table.name.clone()).collect())
    }

    fn get_table<'a>(&self, _py: Python<'a>, name: &str) -> PyResult<TableWrapper> {
        let table = self.tables.get(name).ok_or_else(|| PyValueError::new_err("Table not found."))?;
        TableWrapper::new(table)
    }

    fn execute_query<'a>(&self,
                         py: Python<'a>,
                         lines_iterator: &PyIterator,
                         query: String) -> PyResult<Vec<&'a PyDict>> {
        let query = map_py_err!(parsing::parse(&query), "Failed to parse query: {}")?;
        self.execute_statement(py, lines_iterator, &query)
    }

    fn execute_compiled_query<'a>(&self,
                                  py: Python<'a>,
                                  lines_iterator: &PyIterator,
                                  query: &StatementWrapper) -> PyResult<Vec<&'a PyDict>> {
        self.execute_statement(py, lines_iterator, &query.statement)
    }

    fn execute_query_callback<'a>(&self,
                                  py: Python<'a>,
                                  lines_iterator: &PyIterator,
                                  query: String,
                                  callback: PyObject) -> PyResult<()> {
        let query = map_py_err!(parsing::parse(&query), "Failed to parse query: {}")?;
        self.execute_statement_callback(py, lines_iterator, &query, callback)
    }

    fn execute_compiled_query_callback<'a>(&self,
                                           py: Python<'a>,
                                           lines_iterator: &PyIterator,
                                           query: &StatementWrapper,
                                           callback: PyObject) -> PyResult<()> {
        self.execute_statement_callback(py, lines_iterator, &query.statement, callback)
    }
}

impl TablesWrapper {
    fn execute_statement<'a>(&self, py: Python<'a>,
                             mut lines_iterator: &PyIterator,
                             statement: &Statement) -> PyResult<Vec<&'a PyDict>> {
        let mut execution_engine = ExecutionEngine::new(&self.tables, statement);

        map_py_err!(execution_engine.execute_joined_table(Arc::new(AtomicBool::new(true))), "Failed to create joined data: {}")?;

        let config = execution_engine.execution_config();
        let mut output_rows = Vec::new();
        while let Some(line) = lines_iterator.next() {
            let line = line?.extract::<String>()?;

            let reached_limit = execute_query_line(
                py,
                &mut execution_engine,
                &config,
                line,
                &mut output_rows
            )?;

            if reached_limit {
                break;
            }
        }

        if execution_engine.is_aggregate() {
            execute_query_line(
                py,
                &mut execution_engine,
                &ExecutionConfig::aggregate_result(),
                String::new(),
                &mut output_rows
            )?;
        }

        Ok(output_rows)
    }

    fn execute_statement_callback<'a>(&self,
                                      py: Python<'a>,
                                      mut lines_iterator: &PyIterator,
                                      statement: &Statement,
                                      callback: PyObject) -> PyResult<()> {
        let mut execution_engine = ExecutionEngine::new(&self.tables, statement);

        map_py_err!(execution_engine.execute_joined_table(Arc::new(AtomicBool::new(true))), "Failed to create joined data: {}")?;

        let config = execution_engine.execution_config();
        while let Some(line) = lines_iterator.next() {
            let line = line?.extract::<String>()?;

            let mut output_rows = Vec::new();
            let reached_limit = execute_query_line(
                py,
                &mut execution_engine,
                &config,
                line,
                &mut output_rows
            )?;

            if !output_rows.is_empty() {
                let keep_processing = callback.call1(
                    py,
                    (output_rows, )
                )?.extract::<bool>(py)?;

                if !keep_processing {
                    break;
                }
            }

            if reached_limit {
                break;
            }
        }

        if execution_engine.is_aggregate() {
            let mut output_rows = Vec::new();
            execute_query_line(
                py,
                &mut execution_engine,
                &ExecutionConfig::aggregate_result(),
                String::new(),
                &mut output_rows
            )?;

            if !output_rows.is_empty() {
                callback.call1(
                    py,
                    (output_rows, )
                )?;
            }
        }

        Ok(())
    }
}

#[pyclass(name="Table")]
struct TableWrapper {
    name: String,
    columns: FnvHashMap<String, String>
}

impl TableWrapper {
    pub fn new(table: &TableDefinition) -> PyResult<Self> {
        let mut columns = FnvHashMap::default();
        for column in &table.columns {
            columns.insert(column.name.clone(), column.column_type.to_string());
        }

        Ok(
            TableWrapper {
                name: table.name.clone(),
                columns
            }
        )
    }
}

#[pymethods]
impl TableWrapper {
    fn name(&self) -> &str {
        &self.name
    }

    fn columns<'a>(&self, py: Python<'a>) -> PyResult<&'a PyDict> {
        let columns = PyDict::new(py);
        for (column_name, column_type) in self.columns.iter() {
            columns.set_item(column_name.clone(), column_type.clone())?;
        }

        Ok(columns)
    }
}

#[pyclass(name="Statement")]
struct StatementWrapper {
    original_statement: String,
    statement: Statement
}

#[pymethods]
impl StatementWrapper {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Statement(\"{}\")", self.original_statement))
    }
}

#[pyfunction]
fn compile_query(text: String) -> PyResult<StatementWrapper> {
    let statement = map_py_err!(parsing::parse(&text), "Failed to compile query: {}")?;
    Ok(StatementWrapper { original_statement: text, statement })
}

fn execute_query_line<'a>(py: Python<'a>,
                          execution_engine: &mut ExecutionEngine,
                          config: &ExecutionConfig,
                          line: String,
                          output_rows: &mut Vec<&'a PyDict>) -> PyResult<bool> {
    let output = map_py_err!(execution_engine.execute(line, &config), "Failed to execute query: {}")?;
    if let Some(result_row) = output.result_row {
        for row in result_row.data {
            output_rows.push(create_result_dict(py, &result_row.columns, row)?);
        }
    }

    Ok(output.reached_limit)
}

fn create_result_dict<'a>(py: Python<'a>, columns: &Vec<String>, row: Row) -> PyResult<&'a PyDict> {
    let row_dict = PyDict::new(py);
    for (name, value) in columns.iter().zip(row.columns.into_iter()) {
        row_dict.set_item(name, value)?;
    }

    Ok(row_dict)
}

impl ToPyObject for Value {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        match self {
            Value::Null => py.None(),
            Value::Int(value) => value.to_object(py),
            Value::Float(value) => (value.0).to_object(py),
            Value::Bool(value) => value.to_object(py),
            Value::String(value) => value.to_object(py),
            Value::Array(_, array) => array.to_object(py),
            Value::Timestamp(timestamp) => PyDateTime::new(
                py,
                timestamp.year(),
                timestamp.month() as u8,
                timestamp.day() as u8,
                timestamp.hour() as u8,
                timestamp.minute() as u8,
                timestamp.second() as u8,
                timestamp.nanosecond() / 1000,
                None
            ).unwrap().to_object(py),
            Value::Interval(interval) => PyDelta::new(
                py,
                0,
                interval.num_seconds() as i32,
                (interval.num_microseconds().unwrap_or(0) - interval.num_seconds() * 1000_000) as i32,
                false
            ).unwrap().to_object(py)
        }
    }
}

#[pyclass()]
struct ReadLinesIterator {
    iterator: Lines<BufReader<File>>
}

#[pymethods]
impl ReadLinesIterator {
    #[new]
    fn new(filename: &str) -> PyResult<Self> {
        Ok(
            ReadLinesIterator {
                iterator: BufReader::new(map_py_err!(File::open(filename), "Failed to open file: {}")?).lines()
            }
        )
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<String>> {
        if let Some(item) = self.iterator.next() {
            let item = map_py_err!(item, "Failed to read next line: {}")?;
            Ok(Some(item))
        } else {
            Ok(None)
        }
    }
}

#[pyclass(name="FollowFileIterator")]
struct FollowFileIteratorWrapper {
    iterator: FollowFileIterator
}

#[pymethods]
impl FollowFileIteratorWrapper {
    #[new]
    fn new(filename: &str) -> PyResult<Self> {
        let reader = BufReader::new(map_py_err!(File::open(filename), "Failed to open file: {}")?);

        Ok(
            FollowFileIteratorWrapper {
                iterator: FollowFileIterator::new(reader)
            }
        )
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<String>> {
        Ok(self.iterator.next())
    }
}