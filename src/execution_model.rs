use std::collections::HashMap;

use crate::model::Value;

pub trait ColumnProvider {
    fn get(&self, name: &str) -> Option<&Value>;
}

pub trait RowsProvider<'a, T1: Iterator<Item=T2>, T2: ColumnProvider> {
    fn get_rows(&'a self, table: &str) -> Option<T1>;
}

pub struct HashMapRowsProvider {
    tables: HashMap<String, Vec<HashMap<String, Value>>>
}

impl HashMapRowsProvider {
    pub fn new() -> HashMapRowsProvider {
        HashMapRowsProvider {
            tables: HashMap::new()
        }
    }

    pub fn add_table(&mut self, name: String, rows: Vec<HashMap<String, Value>>) {
        self.tables.insert(name, rows);
    }
}

impl<'a> RowsProvider<'a, HashMapRowsIterator<'a>, HashMapColumnProvider<'a>> for HashMapRowsProvider {
    fn get_rows(&'a self, table: &str) -> Option<HashMapRowsIterator<'a>> {
        let table = self.tables.get(table)?;
        Some(HashMapRowsIterator { table, index: 0 })
    }
}

pub struct HashMapRowsIterator<'a> {
    table: &'a Vec<HashMap<String, Value>>,
    index: usize
}

impl<'a> Iterator for HashMapRowsIterator<'a> {
    type Item = HashMapColumnProvider<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.table.len() {
            let item = HashMapColumnProvider { columns: &self.table[self.index] };
            self.index += 1;
            Some(item)
        } else {
            None
        }
    }
}

pub struct HashMapColumnProvider<'a> {
    columns: &'a HashMap<String, Value>
}

impl<'a> HashMapColumnProvider<'a> {
    pub fn new(columns: &'a HashMap<String, Value>) -> HashMapColumnProvider<'a> {
        HashMapColumnProvider {
            columns
        }
    }
}

impl<'a> ColumnProvider for HashMapColumnProvider<'a> {
    fn get(&self, name: &str) -> Option<&Value> {
        self.columns.get(name)
    }
}


pub struct HashMapRefColumnProvider<'a> {
    columns: HashMap<&'a str, &'a Value>
}

impl<'a> HashMapRefColumnProvider<'a> {
    pub fn new(columns: HashMap<&'a str, &'a Value>) -> HashMapRefColumnProvider<'a> {
        HashMapRefColumnProvider {
            columns
        }
    }
}

impl<'a> ColumnProvider for HashMapRefColumnProvider<'a> {
    fn get(&self, name: &str) -> Option<&Value> {
        self.columns.get(name).map(|x| *x)
    }
}