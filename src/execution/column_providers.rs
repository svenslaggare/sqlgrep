use std::collections::HashMap;

use crate::data_model::TableDefinition;
use crate::execution::{ColumnProvider, ColumnScope};
use crate::model::Value;

pub struct HashMapColumnProvider<'a> {
    columns: HashMap<ColumnScope, HashMap<&'a str, &'a Value>>,
    keys: Vec<String>
}

impl<'a> HashMapColumnProvider<'a> {
    pub fn new(columns: HashMap<ColumnScope, HashMap<&'a str, &'a Value>>) -> HashMapColumnProvider<'a> {
        HashMapColumnProvider {
            columns,
            keys: Vec::new()
        }
    }

    pub fn from_table_scope(columns: HashMap<&'a str, &'a Value>) -> HashMapColumnProvider<'a> {
        HashMapColumnProvider::new(HashMapColumnProvider::create_table_scope(columns))
    }

    pub fn create_table_scope(columns: HashMap<&'a str, &'a Value>) -> HashMap<ColumnScope, HashMap<&'a str, &'a Value>> {
        let mut scopes = HashMap::new();
        scopes.insert(ColumnScope::Table, columns);
        scopes
    }

    pub fn with_table_keys(columns: HashMap<ColumnScope, HashMap<&'a str, &'a Value>>,
                           table: &TableDefinition) -> HashMapColumnProvider<'a> {
        let mut provider = HashMapColumnProvider {
            columns,
            keys: Vec::new()
        };

        provider.add_keys_for_table(table);
        provider
    }

    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }
}

impl<'a> ColumnProvider for HashMapColumnProvider<'a> {
    fn get(&self, scope: ColumnScope, name: &str) -> Option<&Value> {
        self.columns.get(&scope).map(|columns| columns.get(name)).flatten().map(|x| *x)
    }

    fn add_key(&mut self, key: &str) {
        self.keys.push(key.to_owned());
    }

    fn keys(&self) -> &Vec<String> {
        &self.keys
    }
}

pub struct HashMapOwnedKeyColumnProvider<'a> {
    columns: HashMap<ColumnScope, HashMap<String, &'a Value>>,
    keys: Vec<String>
}

impl<'a> HashMapOwnedKeyColumnProvider<'a> {
    pub fn new(columns: HashMap<ColumnScope, HashMap<String, &'a Value>>) -> HashMapOwnedKeyColumnProvider<'a> {
        HashMapOwnedKeyColumnProvider {
            columns,
            keys: Vec::new()
        }
    }
}

impl<'a> ColumnProvider for HashMapOwnedKeyColumnProvider<'a> {
    fn get(&self, scope: ColumnScope, name: &str) -> Option<&Value> {
        self.columns.get(&scope).map(|columns| columns.get(name).map(|x| *x)).flatten()
    }

    fn add_key(&mut self, key: &str) {
        self.keys.push(key.to_owned());
    }

    fn keys(&self) -> &Vec<String> {
        &self.keys
    }
}

pub struct SingleColumnProvider<'a> {
    empty_keys: Vec<String>,
    scope: ColumnScope,
    key: &'a str,
    value: &'a Value
}

impl<'a> SingleColumnProvider<'a> {
    pub fn new(scope: ColumnScope, key: &'a str, value: &'a Value) -> SingleColumnProvider<'a> {
        SingleColumnProvider {
            empty_keys: Vec::new(),
            scope,
            key,
            value
        }
    }
}

impl<'a> ColumnProvider for SingleColumnProvider<'a> {
    fn get(&self, scope: ColumnScope, name: &str) -> Option<&Value> {
        if self.scope == scope && name == self.key {
            Some(self.value)
        } else {
            None
        }
    }

    fn add_key(&mut self, _: &str) {

    }

    fn keys(&self) -> &Vec<String> {
        &self.empty_keys
    }
}