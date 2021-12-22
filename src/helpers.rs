pub struct TablePrinter {
    column_names: Vec<String>,
    rows: Vec<Vec<String>>
}

impl TablePrinter {
    pub fn new(column_names: Vec<String>) -> TablePrinter {
        TablePrinter {
            column_names,
            rows: Vec::new()
        }
    }

    pub fn add_row(&mut self, row: Vec<String>) {
        assert_eq!(self.column_names.len(), row.len());
        self.rows.push(row);
    }

    pub fn print(&mut self) {
        let mut column_lengths = vec![0; self.column_names.len()];
        for (column_index, column) in self.column_names.iter().enumerate() {
            column_lengths[column_index] = column_lengths[column_index].max(column.len() + 2);
        }

        for row in &self.rows {
            for (column_index, column) in row.iter().enumerate() {
                column_lengths[column_index] = column_lengths[column_index].max(column.len() + 2);
            }
        }

        let print_chars = |c: char, n: usize| {
            for _ in 0..n {
                print!("{}", c);
            }
        };

        for (column_index, column) in self.column_names.iter().enumerate() {
            let length = (column_lengths[column_index] - (column.len() + 2));

            print_chars(' ', length / 2);
            print!(" {} ", column);
            print_chars(' ', length / 2 + length % 2);

            if column_index != self.column_names.len() - 1 {
                print!("|");
            }
        }

        println!();

        let mut first = true;
        for length in &column_lengths {
            if !first {
                print!("+");
            } else {
                first = false;
            }

            print_chars('-', *length);
        }
        println!();

        for row in &self.rows {
            for (column_index, column) in row.iter().enumerate() {
                print!(" {} ", column);

                print_chars(' ', column_lengths[column_index] - (column.len() + 2));

                if column_index != self.column_names.len() - 1 {
                    print!("|");
                }
            }

            println!();
        }
    }
}