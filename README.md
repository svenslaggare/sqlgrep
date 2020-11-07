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

If we want to know the IP and hostname for all connections which have a hostname in the file `testdata/test1.log` with the table definition above in `testdata/definition1.txt`  we can do:

```
sqlgrep -d testdata/definition1.txt testdata/test1.log -c "SELECT ip, hostname FROM connections WHERE hostname != NULL"
```

We can also do it "live" by tail following the file (note the `-f` argument):

```
sqlgrep -d testdata/definition1.txt testdata/test1.log -f -c "SELECT ip, hostname FROM connections WHERE hostname != NULL"
```

If we want to know how many connection attempts we get per hostname (i.e. a group by query):

```
sqlgrep -d testdata/definition1.txt testdata/test1.log -c "SELECT hostname, COUNT() AS count FROM connections GROUP BY hostname"
```