import libsqlgrep

def main():
    tables = libsqlgrep.Tables()

    # with open("testdata/ftpd.txt", "r") as f:
    #     tables.add_table(f.read())

    with open("testdata/ftpd_timestamp.txt", "r") as f:
        tables.add_table(f.read())

    # with open("testdata/ftpd_array.txt", "r") as f:
    #     tables.add_table(f.read())

    for table in tables.tables():
        print(table.name())
        for name, column in table.columns().items():
            print("\t{}: {}".format(name, column))
        print("")

    print("=" * 120)

    for row in tables.execute_query(libsqlgrep.ReadLinesIterator("testdata/ftpd_data.txt"),
                                    "SELECT * FROM connections WHERE hostname IS NOT NULL;"):
        print(row)

    print("=" * 120)

    for row in tables.execute_query(libsqlgrep.ReadLinesIterator("testdata/ftpd_data.txt"),
                                    "SELECT hostname, COUNT() AS count FROM connections WHERE hostname IS NOT NULL GROUP BY hostname"):
        print(row)

    print("=" * 120)

    query = libsqlgrep.compile_query("SELECT hostname, COUNT() FROM connections WHERE hostname IS NOT NULL HAVING COUNT() > 22 GROUP BY hostname")
    print(query)
    for row in tables.execute_compiled_query(libsqlgrep.ReadLinesIterator("testdata/ftpd_data.txt"), query):
        print(row)

    print("=" * 120)

    for row in tables.execute_query(yield_lines("testdata/ftpd_data.txt"),
                                    "SELECT * FROM connections WHERE hostname IS NOT NULL;"):
        print(row)

def main_tail():
    tables = libsqlgrep.Tables()

    with open("testdata/ftpd_timestamp.txt", "r") as f:
        tables.add_table(f.read())

    iterator = libsqlgrep.FollowFileIterator("output.txt")
    count = {
        "value": 0
    }
    def callback(rows):
        print(rows)
        count["value"] += len(rows)
        return count["value"] < 100

    tables.execute_query_callback(
        iterator,
        "SELECT * FROM connections WHERE hostname IS NOT NULL;",
        callback
    )

def yield_lines(filename):
    with open(filename, "r") as f:
        for line in f.readlines():
            yield line

if __name__ == "__main__":
    # main()
    main_tail()
