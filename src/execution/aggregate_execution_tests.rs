use std::collections::HashMap;

use crate::execution::aggregate_execution::AggregateExecutionEngine;
use crate::execution::HashMapColumnProvider;
use crate::model::{Aggregate, AggregateStatement, AggregateStatementPart, ArithmeticOperator, BooleanOperator, CompareOperator, ExpressionTree, Float, Function, Value, ValueType};

fn create_test_columns<'a>(names: Vec<&'a str>, values: &'a Vec<Value>) -> HashMap<&'a str, &'a Value> {
    let mut columns = HashMap::new();
    for (name, value) in names.into_iter().zip(values.iter()) {
        columns.insert(name, value);
    }

    columns
}

#[test]
fn test_group_by_and_count() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "x".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("x".to_owned())), transform: None },
            AggregateStatementPart { name: "count".to_owned(), aggregate: Aggregate::Count(None, false), transform: None }
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("x".to_owned())]),
        having: None,
        join: None,
        limit: None
    };

    let column_values = vec![Value::Int(1000)];
    let columns = create_test_columns(vec!["x"], &column_values);

    for _ in 0..5 {
        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone())
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(columns.clone())
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(1, result.data.len());
    assert_eq!(Value::Int(1000), result.data[0].columns[0]);
    assert_eq!(Value::Int(6), result.data[0].columns[1]);

    // Add another group
    let column_values = vec![Value::Int(2000)];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::Int(1000), result.data[0].columns[0]);
    assert_eq!(Value::Int(6), result.data[0].columns[1]);

    assert_eq!(Value::Int(2000), result.data[1].columns[0]);
    assert_eq!(Value::Int(1), result.data[1].columns[1]);
}

#[test]
fn test_group_by_and_count2() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "x".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("x".to_owned())), transform: None },
            AggregateStatementPart { name: "count".to_owned(), aggregate: Aggregate::Count(Some("x".to_owned()), false), transform: None },
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("x".to_owned())]),
        having: None,
        join: None,
        limit: None
    };

    let column_values = vec![Value::Int(1000)];
    let columns = create_test_columns(vec!["x"], &column_values);

    for _ in 0..5 {
        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone())
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![Value::Null];
    let columns = create_test_columns(vec!["x"], &column_values);

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(columns.clone())
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(1, result.data.len());
    assert_eq!(Value::Int(1000), result.data[0].columns[0]);
    assert_eq!(Value::Int(5), result.data[0].columns[1]);
}

#[test]
fn test_group_by_and_count3() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "x".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("x".to_owned())), transform: None },
            AggregateStatementPart { name: "count".to_owned(), aggregate: Aggregate::Count(None, false), transform: None }
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("x".to_owned())]),
        having: None,
        join: None,
        limit: None
    };

    let column_values = vec![Value::Int(1000)];
    let columns = create_test_columns(vec!["x"], &column_values);

    for _ in 0..5 {
        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone())
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![Value::Null];
    let columns = create_test_columns(vec!["x"], &column_values);

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(columns.clone())
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::Null, result.data[0].columns[0]);
    assert_eq!(Value::Int(1), result.data[0].columns[1]);

    assert_eq!(Value::Int(1000), result.data[1].columns[0]);
    assert_eq!(Value::Int(5), result.data[1].columns[1]);
}


#[test]
fn test_group_by_and_count_and_filter() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "x".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("x".to_owned())), transform: None },
            AggregateStatementPart { name: "count".to_owned(), aggregate: Aggregate::Count(None, false), transform: None }
        ],
        from: "test".to_owned(),
        filename: None,
        filter: Some(
            ExpressionTree::Compare {
                left: Box::new(ExpressionTree::ColumnAccess("x".to_owned())),
                right: Box::new(ExpressionTree::Value(Value::Int(1500))),
                operator: CompareOperator::GreaterThan
            }
        ),
        group_by: Some(vec![ExpressionTree::ColumnAccess("x".to_owned())]),
        having: None,
        join: None,
        limit: None
    };

    let column_values = vec![Value::Int(1000)];
    let columns = create_test_columns(vec!["x"], &column_values);

    for _ in 0..5 {
        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone())
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_none());
    }

    // Add another group
    let column_values = vec![Value::Int(2000)];
    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(1, result.data.len());
    assert_eq!(Value::Int(2000), result.data[0].columns[0]);
    assert_eq!(Value::Int(1), result.data[0].columns[1]);
}

#[test]
fn test_group_by_and_max() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "name".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("name".to_owned())), transform: None },
            AggregateStatementPart { name: "max".to_owned(), aggregate: Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())), transform: None },
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("name".to_owned())]),
        having: None,
        join: None,
        limit: None
    };

    for i in 1..6 {
        let column_values = vec![
            Value::Int(i * 1000),
            Value::String("test".to_owned())
        ];

        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![
        Value::Int(0),
        Value::String("test2".to_owned())
    ];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::String("test".to_owned()), result.data[0].columns[0]);
    assert_eq!(Value::Int(5000), result.data[0].columns[1]);

    assert_eq!(Value::String("test2".to_owned()), result.data[1].columns[0]);
    assert_eq!(Value::Int(0), result.data[1].columns[1]);
}

#[test]
fn test_group_by_and_min() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "name".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("name".to_owned())), transform: None },
            AggregateStatementPart { name: "min".to_owned(), aggregate: Aggregate::Min(ExpressionTree::ColumnAccess("x".to_owned())), transform: None },
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("name".to_owned())]),
        having: None,
        join: None,
        limit: None
    };

    for i in 1..6 {
        let column_values = vec![
            Value::Int(i * 1000),
            Value::String("test".to_owned())
        ];

        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![
        Value::Int(0),
        Value::String("test2".to_owned())
    ];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::String("test".to_owned()), result.data[0].columns[0]);
    assert_eq!(Value::Int(1000), result.data[0].columns[1]);

    assert_eq!(Value::String("test2".to_owned()), result.data[1].columns[0]);
    assert_eq!(Value::Int(0), result.data[1].columns[1]);
}

#[test]
fn test_group_by_and_min_null() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "name".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("name".to_owned())), transform: None },
            AggregateStatementPart { name: "min".to_owned(), aggregate: Aggregate::Min(ExpressionTree::ColumnAccess("x".to_owned())), transform: None },
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("name".to_owned())]),
        having: None,
        join: None,
        limit: None
    };

    let column_values = vec![
        Value::Null,
        Value::String("test".to_owned())
    ];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();
    assert!(result.is_some());

    for i in 1..6 {
        let column_values = vec![
            Value::Int(i * 1000),
            Value::String("test".to_owned())
        ];

        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![
        Value::Int(0),
        Value::String("test2".to_owned())
    ];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::String("test".to_owned()), result.data[0].columns[0]);
    assert_eq!(Value::Int(1000), result.data[0].columns[1]);

    assert_eq!(Value::String("test2".to_owned()), result.data[1].columns[0]);
    assert_eq!(Value::Int(0), result.data[1].columns[1]);
}

#[test]
fn test_group_by_and_count_and_max() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "name".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("name".to_owned())), transform: None },
            AggregateStatementPart { name: "count".to_owned(), aggregate: Aggregate::Count(None, false), transform: None },
            AggregateStatementPart { name: "max".to_owned(), aggregate: Aggregate::Max(ExpressionTree::ColumnAccess("x".to_owned())), transform: None },
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("name".to_owned())]),
        having: None,
        join: None,
        limit: None
    };

    for i in 1..6 {
        let column_values = vec![
            Value::Int(i * 1000),
            Value::String("test".to_owned())
        ];

        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![
        Value::Int(0),
        Value::String("test2".to_owned())
    ];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::String("test".to_owned()), result.data[0].columns[0]);
    assert_eq!(Value::Int(5), result.data[0].columns[1]);
    assert_eq!(Value::Int(5000), result.data[0].columns[2]);

    assert_eq!(Value::String("test2".to_owned()), result.data[1].columns[0]);
    assert_eq!(Value::Int(1), result.data[1].columns[1]);
    assert_eq!(Value::Int(0), result.data[1].columns[2]);
}

#[test]
fn test_group_by_and_average() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "name".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("name".to_owned())), transform: None },
            AggregateStatementPart { name: "avg".to_owned(), aggregate: Aggregate::Average(ExpressionTree::ColumnAccess("x".to_owned())), transform: None }
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("name".to_owned())]),
        having: None,
        join: None,
        limit: None
    };

    for i in 1..6 {
        let column_values = vec![
            Value::Int(i * 1000),
            Value::String("test".to_owned())
        ];

        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![
        Value::Int(1000),
        Value::String("test2".to_owned())
    ];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::String("test".to_owned()), result.data[0].columns[0]);
    assert_eq!(Value::Int(3000), result.data[0].columns[1]);

    assert_eq!(Value::String("test2".to_owned()), result.data[1].columns[0]);
    assert_eq!(Value::Int(1000), result.data[1].columns[1]);
}

#[test]
fn test_group_by_and_average_with_null() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "name".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("name".to_owned())), transform: None },
            AggregateStatementPart { name: "avg".to_owned(), aggregate: Aggregate::Average(ExpressionTree::ColumnAccess("x".to_owned())), transform: None }
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("name".to_owned())]),
        having: None,
        join: None,
        limit: None
    };

    let column_values = vec![
        Value::Null,
        Value::String("test".to_owned())
    ];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();
    assert!(result.is_some());

    for i in 1..6 {
        let column_values = vec![
            Value::Int(i * 1000),
            Value::String("test".to_owned())
        ];

        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![
        Value::Int(1000),
        Value::String("test2".to_owned())
    ];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::String("test".to_owned()), result.data[0].columns[0]);
    assert_eq!(Value::Int(3000), result.data[0].columns[1]);

    assert_eq!(Value::String("test2".to_owned()), result.data[1].columns[0]);
    assert_eq!(Value::Int(1000), result.data[1].columns[1]);
}

#[test]
fn test_group_by_and_sum() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "name".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("name".to_owned())), transform: None },
            AggregateStatementPart { name: "sum".to_owned(), aggregate: Aggregate::Sum(ExpressionTree::ColumnAccess("x".to_owned())), transform: None },
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("name".to_owned())]),
        having: None,
        join: None,
        limit: None
    };

    for i in 1..6 {
        let column_values = vec![
            Value::Int(i * 1000),
            Value::String("test".to_owned())
        ];

        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![
        Value::Int(1000),
        Value::String("test2".to_owned())
    ];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::String("test".to_owned()), result.data[0].columns[0]);
    assert_eq!(Value::Int(15000), result.data[0].columns[1]);

    assert_eq!(Value::String("test2".to_owned()), result.data[1].columns[0]);
    assert_eq!(Value::Int(1000), result.data[1].columns[1]);
}

#[test]
fn test_group_by_and_stddev1() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "name".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("name".to_owned())), transform: None },
            AggregateStatementPart { name: "std".to_owned(), aggregate: Aggregate::StandardDeviation(ExpressionTree::ColumnAccess("x".to_owned())), transform: None },
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("name".to_owned())]),
        having: None,
        join: None,
        limit: None
    };

    for i in 1..6 {
        let column_values = vec![
            Value::Int(i),
            Value::String("test".to_owned())
        ];

        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![
        Value::Int(7),
        Value::String("test2".to_owned())
    ];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::String("test".to_owned()), result.data[0].columns[0]);
    assert_eq!(Value::Float(Float(2.0f64.sqrt())), result.data[0].columns[1]);

    assert_eq!(Value::String("test2".to_owned()), result.data[1].columns[0]);
    assert_eq!(Value::Float(Float(0.0)), result.data[1].columns[1]);
}

#[test]
fn test_group_by_and_stddev2() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "name".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("name".to_owned())), transform: None },
            AggregateStatementPart { name: "std".to_owned(), aggregate: Aggregate::StandardDeviation(ExpressionTree::ColumnAccess("x".to_owned())), transform: None },
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("name".to_owned())]),
        having: None,
        join: None,
        limit: None
    };

    for i in 1..6 {
        let column_values = vec![
            Value::Float(Float(i as f64)),
            Value::String("test".to_owned())
        ];

        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![
        Value::Float(Float(7.0)),
        Value::String("test2".to_owned())
    ];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::String("test".to_owned()), result.data[0].columns[0]);
    assert_eq!(Value::Float(Float(2.0f64.sqrt())), result.data[0].columns[1]);

    assert_eq!(Value::String("test2".to_owned()), result.data[1].columns[0]);
    assert_eq!(Value::Float(Float(0.0)), result.data[1].columns[1]);
}

#[test]
fn test_group_by_and_stddev_with_null() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "name".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("name".to_owned())), transform: None },
            AggregateStatementPart { name: "std".to_owned(), aggregate: Aggregate::StandardDeviation(ExpressionTree::ColumnAccess("x".to_owned())), transform: None }
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("name".to_owned())]),
        having: None,
        join: None,
        limit: None
    };

    let column_values = vec![
        Value::Null,
        Value::String("test".to_owned())
    ];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();
    assert!(result.is_some());

    for i in 1..6 {
        let column_values = vec![
            Value::Float(Float(i as f64)),
            Value::String("test".to_owned())
        ];

        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![
        Value::Float(Float(7.0)),
        Value::String("test2".to_owned())
    ];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::String("test".to_owned()), result.data[0].columns[0]);
    assert_eq!(Value::Float(Float(2.0f64.sqrt())), result.data[0].columns[1]);

    assert_eq!(Value::String("test2".to_owned()), result.data[1].columns[0]);
    assert_eq!(Value::Float(Float(0.0)), result.data[1].columns[1]);
}

#[test]
fn test_group_by_and_sum_and_transform() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "name".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("name".to_owned())), transform: None },
            (
                AggregateStatementPart {
                    name: "max".to_string(),
                    aggregate: Aggregate::Sum(ExpressionTree::ColumnAccess("x".to_owned())),
                    transform: Some(
                        ExpressionTree::Arithmetic {
                            operator: ArithmeticOperator::Multiply,
                            left: Box::new(ExpressionTree::ColumnAccess("$agg".to_owned())),
                            right: Box::new(ExpressionTree::Value(Value::Int(2)))
                        }
                    )
                }
            )
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("name".to_owned())]),
        having: None,
        join: None,
        limit: None
    };

    for i in 1..6 {
        let column_values = vec![
            Value::Int(i * 1000),
            Value::String("test".to_owned())
        ];

        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![
        Value::Int(1000),
        Value::String("test2".to_owned())
    ];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::String("test".to_owned()), result.data[0].columns[0]);
    assert_eq!(Value::Int(15000 * 2), result.data[0].columns[1]);

    assert_eq!(Value::String("test2".to_owned()), result.data[1].columns[0]);
    assert_eq!(Value::Int(1000 * 2), result.data[1].columns[1]);
}

#[test]
fn test_group_by_expression_and_average() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart {
                name: "name".to_owned(),
                aggregate: Aggregate::GroupKey(
                    ExpressionTree::Arithmetic {
                        operator: ArithmeticOperator::Multiply,
                        left: Box::new(ExpressionTree::ColumnAccess("name".to_owned())),
                        right: Box::new(ExpressionTree::Value(Value::Int(2))),
                    }
                ),
                transform: None
            },
            AggregateStatementPart { name: "avg".to_owned(), aggregate: Aggregate::Average(ExpressionTree::ColumnAccess("x".to_owned())), transform: None }
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(
            vec![
                ExpressionTree::Arithmetic {
                    operator: ArithmeticOperator::Multiply,
                    left: Box::new(ExpressionTree::ColumnAccess("name".to_owned())),
                    right: Box::new(ExpressionTree::Value(Value::Int(2))),
                }
            ]
        ),
        having: None,
        join: None,
        limit: None
    };

    for i in 1..6 {
        let column_values = vec![
            Value::Int(i * 1000),
            Value::Int(100_000)
        ];

        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![
        Value::Int(1000),
        Value::Int(1_000_000)
    ];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x", "name"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::Int(200_000), result.data[0].columns[0]);
    assert_eq!(Value::Int(3000), result.data[0].columns[1]);

    assert_eq!(Value::Int(2_000_000), result.data[1].columns[0]);
    assert_eq!(Value::Int(1000), result.data[1].columns[1]);
}

#[test]
fn test_count() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "count".to_owned(), aggregate: Aggregate::Count(None, false), transform: None }
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: None,
        having: None,
        join: None,
        limit: None
    };

    let column_values = vec![Value::Int(1000)];
    let columns = create_test_columns(vec!["x"], &column_values);

    for _ in 0..5 {
        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone())
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(columns.clone())
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(1, result.data.len());
    assert_eq!(Value::Int(6), result.data[0].columns[0]);
}

#[test]
fn test_group_by_and_count_and_having1() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "x".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("x".to_owned())), transform: None },
            AggregateStatementPart { name: "count".to_owned(), aggregate: Aggregate::Count(None, false), transform: None }
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("x".to_owned())]),
        having: Some(
            ExpressionTree::Compare {
                operator: CompareOperator::Equal,
                left: Box::new(ExpressionTree::Aggregate(0, Box::new(Aggregate::GroupKey(ExpressionTree::ColumnAccess("x".to_owned()))))),
                right: Box::new(ExpressionTree::Value(Value::Int(2000)))
            }
        ),
        join: None,
        limit: None
    };

    let column_values = vec![Value::Int(1000)];
    let columns = create_test_columns(vec!["x"], &column_values);

    for _ in 0..5 {
        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone())
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(columns.clone())
    );

    assert!(result.is_ok());
    let result = result.unwrap();
    assert!(result.is_some());

    // Add another group
    let column_values = vec![Value::Int(2000)];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(1, result.data.len());
    assert_eq!(Value::Int(2000), result.data[0].columns[0]);
    assert_eq!(Value::Int(1), result.data[0].columns[1]);
}

#[test]
fn test_group_by_and_count_and_having2() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "x".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("x".to_owned())), transform: None },
            AggregateStatementPart { name: "count".to_owned(), aggregate: Aggregate::Count(None, false), transform: None }
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("x".to_owned())]),
        having: Some(
            ExpressionTree::Compare {
                operator: CompareOperator::NotEqual,
                left: Box::new(ExpressionTree::Aggregate(0, Box::new(Aggregate::GroupKey(ExpressionTree::ColumnAccess("x".to_owned()))))),
                right: Box::new(ExpressionTree::Value(Value::Int(2000)))
            }
        ),
        join: None,
        limit: None
    };

    let column_values = vec![Value::Int(1000)];
    let columns = create_test_columns(vec!["x"], &column_values);

    for _ in 0..5 {
        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone())
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(columns.clone())
    );

    assert!(result.is_ok());
    let result = result.unwrap();
    assert!(result.is_some());

    // Add another group
    let column_values = vec![Value::Int(2000)];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(1, result.data.len());
    assert_eq!(Value::Int(1000), result.data[0].columns[0]);
    assert_eq!(Value::Int(6), result.data[0].columns[1]);
}

#[test]
fn test_group_by_and_count_and_having3() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "x".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("x".to_owned())), transform: None },
            AggregateStatementPart { name: "count".to_owned(), aggregate: Aggregate::Count(None, false), transform: None }
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("x".to_owned())]),
        having: Some(
            ExpressionTree::Compare {
                operator: CompareOperator::GreaterThan,
                left: Box::new(ExpressionTree::Aggregate(0, Box::new(Aggregate::Count(None, false)))),
                right: Box::new(ExpressionTree::Value(Value::Int(1)))
            }
        ),
        join: None,
        limit: None
    };

    let column_values = vec![Value::Int(1000)];
    let columns = create_test_columns(vec!["x"], &column_values);

    for _ in 0..5 {
        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone())
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(columns.clone())
    );

    assert!(result.is_ok());
    let result = result.unwrap();
    assert!(result.is_some());

    // Add another group
    let column_values = vec![Value::Int(3000)];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();
    assert!(result.is_some());

    // Add another group
    let column_values = vec![Value::Int(2000)];
    let columns = create_test_columns(vec!["x"], &column_values);

    for _ in 0..3 {
        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone())
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(columns.clone())
    );

    assert!(result.is_ok());
    let result = result.unwrap();
    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::Int(1000), result.data[0].columns[0]);
    assert_eq!(Value::Int(6), result.data[0].columns[1]);

    assert_eq!(Value::Int(2000), result.data[1].columns[0]);
    assert_eq!(Value::Int(4), result.data[1].columns[1]);
}

#[test]
fn test_group_by_and_count_and_having4() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "x".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("x".to_owned())), transform: None },
            AggregateStatementPart { name: "count".to_owned(), aggregate: Aggregate::Count(None, false), transform: None }
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("x".to_owned())]),
        having: Some(
            ExpressionTree::BooleanOperation {
                operator: BooleanOperator::And,
                left: Box::new(ExpressionTree::Compare {
                    operator: CompareOperator::GreaterThan,
                    left: Box::new(ExpressionTree::Aggregate(0, Box::new(Aggregate::Count(None, false)))),
                    right: Box::new(ExpressionTree::Value(Value::Int(1)))
                }),
                right: Box::new(ExpressionTree::Compare {
                    operator: CompareOperator::Equal,
                    left: Box::new(ExpressionTree::Aggregate(1, Box::new(Aggregate::GroupKey(ExpressionTree::ColumnAccess("x".to_owned()))))),
                    right: Box::new(ExpressionTree::Value(Value::Int(1000)))
                })
            }
        ),
        join: None,
        limit: None
    };

    let column_values = vec![Value::Int(1000)];
    let columns = create_test_columns(vec!["x"], &column_values);

    for _ in 0..5 {
        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone())
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(columns.clone())
    );

    assert!(result.is_ok());
    let result = result.unwrap();
    assert!(result.is_some());

    // Add another group
    let column_values = vec![Value::Int(3000)];

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(create_test_columns(vec!["x"], &column_values))
    );

    assert!(result.is_ok());
    let result = result.unwrap();
    assert!(result.is_some());

    // Add another group
    let column_values = vec![Value::Int(2000)];
    let columns = create_test_columns(vec!["x"], &column_values);

    for _ in 0..3 {
        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone())
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(columns.clone())
    );

    assert!(result.is_ok());
    let result = result.unwrap();
    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(1, result.data.len());
    assert_eq!(Value::Int(1000), result.data[0].columns[0]);
    assert_eq!(Value::Int(6), result.data[0].columns[1]);
}

#[test]
fn test_group_by_array_agg1() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "x".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("x".to_owned())), transform: None },
            AggregateStatementPart { name: "ys".to_owned(), aggregate: Aggregate::CollectArray(ExpressionTree::ColumnAccess("y".to_owned())), transform: None },
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("x".to_owned())]),
        having: None,
        join: None,
        limit: None
    };

    // Add first group
    for i in 0..5 {
        let column_values = vec![Value::Int(1000), Value::Int(100 + i)];
        let columns = create_test_columns(vec!["x", "y"], &column_values);
        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone())
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    // Add second group
    for i in 0..4 {
        let column_values = vec![Value::Int(2000), Value::Int(300 + i)];
        let columns = create_test_columns(vec!["x", "y"], &column_values);
        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone())
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![Value::Int(2000), Value::Int(300 + 4)];
    let columns = create_test_columns(vec!["x", "y"], &column_values);
    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(columns.clone())
    );

    assert!(result.is_ok());
    let result = result.unwrap();
    assert!(result.is_some());

    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::Int(1000), result.data[0].columns[0]);
    assert_eq!(
        Value::Array(ValueType::Int, vec![Value::Int(100), Value::Int(101), Value::Int(102), Value::Int(103), Value::Int(104)]),
        result.data[0].columns[1]
    );

    assert_eq!(Value::Int(2000), result.data[1].columns[0]);
    assert_eq!(
        Value::Array(ValueType::Int, vec![Value::Int(300), Value::Int(301), Value::Int(302), Value::Int(303), Value::Int(304)]),
        result.data[1].columns[1]
    );
}

#[test]
fn test_group_by_array_agg2() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "x".to_owned(), aggregate: Aggregate::GroupKey(ExpressionTree::ColumnAccess("x".to_owned())), transform: None },
            AggregateStatementPart {
                name: "ys".to_string(),
                aggregate: (Aggregate::CollectArray(ExpressionTree::ColumnAccess("y".to_owned()))),
                transform: Some(
                    ExpressionTree::Function {
                        function: Function::ArrayUnique,
                        arguments: vec![ExpressionTree::ColumnAccess("$agg".to_owned())]
                    }
                ),
            }
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: Some(vec![ExpressionTree::ColumnAccess("x".to_owned())]),
        having: None,
        join: None,
        limit: None
    };

    // Add first group
    for _ in 0..2 {
        for i in 0..5 {
            let column_values = vec![Value::Int(1000), Value::Int(100 + i)];
            let columns = create_test_columns(vec!["x", "y"], &column_values);
            let result = aggregate_execution_engine.execute(
                &aggregate_statement,
                HashMapColumnProvider::new(columns.clone())
            );

            assert!(result.is_ok());
            let result = result.unwrap();
            assert!(result.is_some());
        }
    }

    // Add second group
    for _ in 0..2 {
        for i in 0..4 {
            let column_values = vec![Value::Int(2000), Value::Int(300 + i)];
            let columns = create_test_columns(vec!["x", "y"], &column_values);
            let result = aggregate_execution_engine.execute(
                &aggregate_statement,
                HashMapColumnProvider::new(columns.clone())
            );

            assert!(result.is_ok());
            let result = result.unwrap();
            assert!(result.is_some());
        }
    }

    let column_values = vec![Value::Int(2000), Value::Int(300 + 4)];
    let columns = create_test_columns(vec!["x", "y"], &column_values);
    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(columns.clone())
    );

    assert!(result.is_ok());
    let result = result.unwrap();
    assert!(result.is_some());

    let result = result.unwrap();

    assert_eq!(2, result.data.len());
    assert_eq!(Value::Int(1000), result.data[0].columns[0]);
    assert_eq!(
        Value::Array(ValueType::Int, vec![Value::Int(100), Value::Int(101), Value::Int(102), Value::Int(103), Value::Int(104)]),
        result.data[0].columns[1]
    );

    assert_eq!(Value::Int(2000), result.data[1].columns[0]);
    assert_eq!(
        Value::Array(ValueType::Int, vec![Value::Int(300), Value::Int(301), Value::Int(302), Value::Int(303), Value::Int(304)]),
        result.data[1].columns[1]
    );
}

#[test]
fn test_count_distinct() {
    let mut aggregate_execution_engine = AggregateExecutionEngine::new();

    let aggregate_statement = AggregateStatement {
        aggregates: vec![
            AggregateStatementPart { name: "count".to_owned(), aggregate: Aggregate::Count(Some("x".to_owned()), true), transform: None },
        ],
        from: "test".to_owned(),
        filename: None,
        filter: None,
        group_by: None,
        having: None,
        join: None,
        limit: None
    };

    for i in 1..6 {
        let column_values = vec![Value::Int(i * 1000)];
        let columns = create_test_columns(vec!["x"], &column_values);

        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone()),
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    for i in 1..3 {
        let column_values = vec![Value::Int(i * 1000)];
        let columns = create_test_columns(vec!["x"], &column_values);

        let result = aggregate_execution_engine.execute(
            &aggregate_statement,
            HashMapColumnProvider::new(columns.clone()),
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
    }

    let column_values = vec![Value::Int(1000)];
    let columns = create_test_columns(vec!["x"], &column_values);

    let result = aggregate_execution_engine.execute(
        &aggregate_statement,
        HashMapColumnProvider::new(columns.clone()),
    );

    assert!(result.is_ok());
    let result = result.unwrap();

    assert!(result.is_some());
    let result = result.unwrap();

    assert_eq!(1, result.data.len());
    assert_eq!(Value::Int(5), result.data[0].columns[0]);
}