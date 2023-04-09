import libsqlgrep

def main():
    tables = libsqlgrep.Tables()

    # with open("testdata/ftpd.txt", "r") as f:
    #     tables.add_table(f.read())

    with open("testdata/ftpd_timestamp.txt", "r") as f:
        tables.add_table(f.read())

    # with open("testdata/ftpd_array.txt", "r") as f:
    #     tables.add_table(f.read())

    for table in tables.table_names():
        print(table)
        for name, column in tables.get_table(table).items():
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

if __name__ == "__main__":
    main()
