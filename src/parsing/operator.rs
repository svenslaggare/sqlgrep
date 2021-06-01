use std::collections::{HashMap, HashSet};
use std::fmt::Formatter;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Operator {
    Single(char),
    Dual(char, char)
}

impl std::fmt::Display for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Operator::Single(x) => write!(f, "{}", x),
            Operator::Dual(x, y) => write!(f, "{}{}", x, y)
        }
    }
}

pub struct BinaryOperator {
    pub precedence: i32
}

impl BinaryOperator {
    fn new(precedence: i32)-> BinaryOperator {
        BinaryOperator {
            precedence
        }
    }
}

pub struct BinaryOperators {
    operators: HashMap<Operator, BinaryOperator>
}

impl BinaryOperators {
    pub fn new() -> BinaryOperators {
        let mut operators = HashMap::new();

        operators.insert(Operator::Single('.'), BinaryOperator::new(6));
        operators.insert(Operator::Single('^'), BinaryOperator::new(5));
        operators.insert(Operator::Single('*'), BinaryOperator::new(5));
        operators.insert(Operator::Single('/'), BinaryOperator::new(5));
        operators.insert(Operator::Single('+'), BinaryOperator::new(4));
        operators.insert(Operator::Single('-'), BinaryOperator::new(4));
        operators.insert(Operator::Single('<'), BinaryOperator::new(3));
        operators.insert(Operator::Dual('<', '='), BinaryOperator::new(3));
        operators.insert(Operator::Single('>'), BinaryOperator::new(3));
        operators.insert(Operator::Dual('>', '='), BinaryOperator::new(3));
        operators.insert(Operator::Single('='), BinaryOperator::new(2));
        operators.insert(Operator::Dual('!', '='), BinaryOperator::new(2));

        BinaryOperators {
            operators
        }
    }

    pub fn get(&self, op: &Operator) -> Option<&BinaryOperator> {
        self.operators.get(op)
    }
}

pub struct UnaryOperators {
    operators: HashSet<Operator>
}

impl UnaryOperators {
    pub fn new() -> UnaryOperators {
        let mut operators = HashSet::<Operator>::new();

        operators.insert(Operator::Single('-'));

        UnaryOperators {
            operators
        }
    }

    pub fn exists(&self, op: &Operator) -> bool {
        self.operators.contains(op)
    }
}