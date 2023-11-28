use fnv::FnvHashSet;

use crate::model::Value;

pub struct DistinctValues {
    values: FnvHashSet<Vec<Value>>
}

impl DistinctValues {
    pub fn new() -> DistinctValues {
        DistinctValues {
            values: FnvHashSet::default()
        }
    }

    pub fn add(&mut self, value: &Vec<Value>) -> bool {
        let has_value = self.values.contains(value);
        if has_value {
            false
        } else {
            self.values.insert(value.clone());
            true
        }
    }
}