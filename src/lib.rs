#![allow(unused)]
use pyo3::prelude::*;

use csv::{ReaderBuilder};
use polars::prelude::*;
use std::fs;
use std::path::Path;
use std::fs::OpenOptions;
use std::io::prelude::*;

#[pyfunction]
fn create_new_output_csv_with_header(folder: String) -> PyResult<String> {
    /* 
        delete file - necessary to create a new one file for every execution
    */
    if Path::new("output.csv").exists() {    
        fs::remove_file("output.csv").expect("remove .csv file");    
        println!("file output.csv deleted");
    }   
    println!("---------------------------------------------");
    /*
        read from several csv files and save it in a new csv with headers
     */
    let file_path = "./output.csv";
    let headers: String = String::from("dfa,dfa_toll_fk,dfa_update_timestamp,dfa_data_timestamp");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(file_path)
        .unwrap();

    // Write the header to the file
    writeln!(file, "{}", headers).unwrap();

    //let input_dir = "./data"; // path to input folder
    let mut input_dir = format!("./data/{}", &folder);
    for entry in fs::read_dir(input_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();

        if path.is_file() {
            println!("Processing file ... {:?}", path);
            let mut reader = ReaderBuilder::new()
                .from_path(path).unwrap();                
            for result in reader.records() {
                let record = result.unwrap();
                let value1: &str = record.get(0).unwrap_or("");
                let value2: &str = record.get(1).unwrap_or("");
                let value3: &str = record.get(2).unwrap_or("");
                let value4: &str = record.get(3).unwrap_or("");
                writeln!(file, "{}, {}, {}, {},", 
                    value1, value2, value3, value4,
                ).unwrap();
            }
        }
    }
    println!("Created a new file {}", file_path);
    Ok(file_path.to_string())
}

#[pyfunction]
fn delete_duplicated_rows_by_column() -> PyResult<String>  {
    println!("---------------------------------------------");
    println!("reading from a csv file and getting duplicated rows");
    let df = CsvReader::from_path("./output.csv").unwrap()
            .has_header(true)
            .finish()
            .unwrap();
    let df_unique = df.unique(Some(&["mvda_oid".to_string()]), UniqueKeepStrategy::First, None);
    println!("dataframe without duplicated values of a column {:?}", df_unique);
    Ok("done".to_string())
}

#[pymodule]
fn read_csvs(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(create_new_output_csv_with_header, m)?)?;
    m.add_function(wrap_pyfunction!(delete_duplicated_rows_by_column, m)?)?;
    Ok(())
}


