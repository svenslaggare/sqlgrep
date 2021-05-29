# sqlgrep
Combines SQL with regular expressions to provide a new way to filter and process text files.

## Build
* Requires cargo (https://rustup.rs/).
* Build with: `cargo build --release`
* Build output in `target/release/sqlgrep`

## Example
First, a schema needs to be defined that will transform text lines into structured data:
```
CREATE TABLE connections(
    line = 'connection from ([0-9.]+) \\((.+)?\\) at ([a-zA-Z]+) ([a-zA-Z]+) ([0-9]+) ([0-9]+):([0-9]+):([0-9]+) ([0-9]+)',

    line[1] => ip TEXT,
    line[2] => hostname TEXT,
    line[9] => year INT,
    line[4] => month TEXT,
    line[5] => day INT,
    line[6] => hour INT,
    line[7] => minute INT,
    line[8] => second INT
);
```

If we want to know the IP and hostname for all connections which have a hostname in the file `testdata/ftpd_data.txt` with the table definition above in `testdata/ftpd.txt`  we can do:

```
sqlgrep -d testdata/ftpd.txt testdata/ftpd_data.txt -c "SELECT ip, hostname FROM connections WHERE hostname IS NOT NULL"
```

We can also do it "live" by tail following the file (note the `-f` argument):

```
sqlgrep -d testdata/ftpd.txt testdata/ftpd_data.txt -f -c "SELECT ip, hostname FROM connections WHERE hostname IS NOT NULL"
```

If we want to know how many connection attempts we get per hostname (i.e. a group by query):

```
sqlgrep -d testdata/ftpd.txt testdata/ftpd_data.txt -c "SELECT hostname, COUNT() AS count FROM connections GROUP BY hostname"
```

See `testdata` folder and `src/integration_tests.rs` for more examples.

# Documentation
Tries to follow the SQL standard, so you should expect that normal SQL queries work. However, not every feature is supported yet.

## Queries
Supported features:
* Where
* Group by
* Aggregates
* Having
* Inner, outer joins. The joined table is loaded completely in memory.

Supported aggregates:
* Count
* Min
* Max
* Sum
* Avg

Supported functions:
* least
* greatest
* abs
* sqrt
* pow
* regex_matches

## Special features
The input filename can either be specified with the CLI or as an additional argument to the `FROM` statement as following:
```
SELECT * FROM connections::'file.log';
```

## Tables
### Syntax
```
CREATE TABLE <name>(
    Separate pattern and column definition. Pattern can be used in multiple column definitions.
    <pattern name> = '<regex patern>',
    <pattern name>[<group index>] => <column name> <column type>,

    Inline regex. Will be bound to the first group
    '<regex patern>' => <column name> <column type>
    
    Json pattern. Will access the given attribute.
    { .field1.field2 } => <column name> <column type>,
    { .field1[<array index>] } => <column name> <column type>,
);
```
Multiple tables can be defined in the same file.

### Supported types
* `TEXT`: String type.
* `INT`: 64-bits integer type.
* `REAL`: 64-bits float type.
* `BOOLEAN`: True/false type. When extracting data, it means the _existence_ of a group.

### Modifiers
Placed after the column type and add additional constraints/transforms.
* `NOT NULL`: The column cannot be `NULL`. If a not null column gets a null value, the row is _not_ inserted.
* `TRIM`: Trim string types for whitespaces.
* `CONVERT`: Tries to convert a string value into the value type.
