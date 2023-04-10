use std::cell::RefCell;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::ops::DerefMut;
use std::path::Path;
use std::rc::Rc;
use cursive::Cursive;

use cursive::traits::*;
use cursive::utils::markup::StyledString;
use cursive::views::{Button, LinearLayout, ListView, NamedView, Panel, SelectView, TextArea, TextView};
use cursive::theme::{BaseColor, Color};

use crate::data_model::TableDefinition;
use crate::{parsing, Tables};

pub fn run(mut data_definition_file: Option<String>,
           input_file: Vec<String>,
           table_name: String) {
    let table_definition_filename = data_definition_file.take().expect("The data definition file must be specified.");
    let table_definition_content = if Path::new(&table_definition_filename).exists() {
        std::fs::read_to_string(&table_definition_filename).expect("Failed to read data definition file.")
    } else {
        let content = format!("CREATE TABLE {}(\n\n);", table_name);
        std::fs::write(&table_definition_filename, &content).expect("Failed to create data definition file.");
        content
    };

    let input_filename = input_file.get(0).expect("The input file must be specified.");
    let input_file = File::open(input_filename).expect("Failed to read input file.");

    let mut tables = Tables::new();
    if let Err(err) = define_table(&mut tables, table_definition_content.clone()) {
        println!("{}", err);
        std::process::exit(1);
    }

    let table_editor = TableEditor::new(
        tables,
        table_name,
        table_definition_filename,
        table_definition_content,
        BufReader::new(input_file).lines().take(1000).map(|line| line.unwrap()).collect::<Vec<_>>()
    );
    table_editor.verify_table_exists();

    let table_editor = Rc::new(RefCell::new(table_editor));

    let table_editor_extract_data = table_editor.clone();
    let table_editor_update_table = table_editor.clone();
    let table_editor_edit_table = table_editor.clone();
    let table_editor_save_table = table_editor.clone();

    let mut siv = cursive::default();
    let width = 140;

    let handle_selected_line = |siv: &mut Cursive, table_editor: Rc<RefCell<TableEditor>>, line_index: usize| {
        siv.call_on_name("extracted_data", |view: &mut TextView| {
            let table_editor = table_editor.borrow();
            view.set_content(table_editor.extract_columns_for_line(line_index).join(", "))
        });

        siv.call_on_name("selected_input_line", |view: &mut TextView| {
            let table_editor = table_editor.borrow();
            view.set_content(table_editor.input_lines[line_index].clone());
        });
    };

    siv.add_layer(
        LinearLayout::vertical()
            .child(
                Panel::new(
                    SelectView::new()
                        .with_all(table_editor.borrow().input_lines.iter().enumerate().map(|(i, line)| (line.to_owned(), i)))
                        .on_select(move |siv, line_index| {
                            handle_selected_line(siv, table_editor_extract_data.clone(), *line_index);
                            // siv.call_on_name("extracted_data", |view: &mut TextView| {
                            //     let table_editor = table_editor_extract_data.borrow();
                            //     view.set_content(table_editor.extract_columns_for_line(*line_index).join(", "))
                            // });
                            //
                            // siv.call_on_name("selected_input_line", |view: &mut TextView| {
                            //     let table_editor = table_editor_extract_data.borrow();
                            //     view.set_content(table_editor.input_lines[*line_index].clone());
                            // });
                        })
                        .with_name("input_lines")
                        .scrollable()
                        .max_height(10)
                )
                    .title("Input lines")
                    .fixed_width(width)
            )
            .child(
                Panel::new(
                    TextView::new("")
                        .with_name("selected_input_line")
                )
                    .title("Selected line")
                    .fixed_width(width)
            )
            .child(
                Panel::new(
                    ListView::new()
                        .child(
                            "",
                            TextArea::new()
                                .content(table_editor.borrow().table_definition_content.clone())
                                .with_name("edit_table")
                                .max_height(30)
                        )
                        .child(
                            "",
                            TextView::new("")
                                .with_name("compile_error")
                        )
                        .child(
                            "",
                            LinearLayout::horizontal()
                                .child(
                                    Button::new(
                                        "Update",
                                        move |siv| {
                                            let mut table_editor = table_editor_update_table.borrow_mut();

                                            let edit_table_view = siv.find_name::<TextArea>("edit_table").unwrap();
                                            match define_table(&mut table_editor.tables, edit_table_view.get_content().to_owned()) {
                                                Ok(()) => {
                                                    let input_lines_view = siv.find_name::<SelectView<usize>>("input_lines").unwrap();
                                                    let line_index = input_lines_view.selection().map(|selection| *selection).unwrap_or(0);

                                                    let mut extracted_data_view = siv.find_name::<TextView>("extracted_data").unwrap();
                                                    extracted_data_view.set_content(table_editor.extract_columns_for_line(line_index).join(", "));

                                                    siv.call_on_name("compile_error", |view: &mut TextView| {
                                                        view.set_content("")
                                                    });
                                                }
                                                Err(err) => {
                                                    siv.call_on_name("compile_error", |view: &mut TextView| {
                                                        view.set_content(StyledString::styled(err, Color::Light(BaseColor::Red)))
                                                    });
                                                }
                                            }
                                        }
                                    )
                                )
                                .child(TextView::new(" "))
                                .child(
                                    Button::new(
                                        "Restore",
                                        move |siv| {
                                            siv.call_on_name("edit_table", |view: &mut NamedView<TextArea>| {
                                                view.get_mut().set_content(table_editor_edit_table.borrow().table_definition_content.clone())
                                            });
                                        }
                                    )
                                )
                                .child(TextView::new(" "))
                                .child(
                                    Button::new(
                                        "Save",
                                        move |siv| {
                                            let edit_table_view = siv.find_name::<TextArea>("edit_table").unwrap();
                                            table_editor_save_table.borrow_mut().save_table(edit_table_view.get_content());
                                        }
                                    )
                                )
                        )
                )
                    .title("Table")
                    .fixed_width(width)
            )
            .child(
                Panel::new(TextView::new("").with_name("extracted_data"))
                    .title("Extracted data")
                    .fixed_width(width)
            )
    );

    handle_selected_line(siv.deref_mut(), table_editor.clone(), 0);

    siv.run();
}

struct TableEditor {
    tables: Tables,
    table_name: String,
    table_definition_filename: String,
    table_definition_content: String,
    input_lines: Vec<String>
}

impl TableEditor {
    pub fn new(tables: Tables,
               table_name: String,
               table_definition_filename: String,
               table_definition_content: String,
               input_lines: Vec<String>) -> TableEditor {
        TableEditor {
            tables,
            table_name,
            table_definition_filename,
            table_definition_content,
            input_lines
        }
    }

    pub fn verify_table_exists(&self) {
        if self.tables.get(&self.table_name).is_none() {
            println!("Table '{}' not found.", self.table_name);
            std::process::exit(1);
        }
    }

    pub fn table(&self) -> &TableDefinition {
        self.tables.get(&self.table_name).unwrap()
    }

    pub fn extract_columns_for_line(&self, line_index: usize) -> Vec<String> {
        let table = self.table();
        let row = table.extract(&self.input_lines[line_index]);
        if row.columns.is_empty() {
            return Vec::new();
        }

        table.columns
            .iter()
            .enumerate()
            .map(|(index, column)| format!("{}: {}", column.name, row.columns[index]))
            .collect::<Vec<_>>()
    }

    pub fn save_table(&mut self, content: &str) {
        if let Err(err) = std::fs::write(&self.table_definition_filename, content) {
            println!("Failed to save file due to: {}", err);
            std::process::exit(1);
        }

        self.table_definition_content = content.to_owned();
    }
}

fn define_table(tables: &mut Tables, text: String) -> Result<(), String> {
    match parsing::parse(&text) {
        Ok(create_table_statement) => {
            if tables.add_tables(create_table_statement) {
                Ok(())
            } else {
                Err("Expected CREATE TABLE statement.".to_owned())
            }
        }
        Err(err) => {
            Err(format!("Failed to create table: {}.", err))
        }
    }
}