use pyo3::prelude::*;
#[allow(unused_imports)]
use pyo3::types::*;
use pyo3::wrap_pyfunction;
#[allow(unused_imports)]
use rayon::prelude::*;

use serde_json::Value;
use std::collections::HashMap;

mod pq;
use pq::{deep_write, generate_headers, merge_headers, write_to_file, Schema};

mod sample;
use sample::*;

#[pyfunction]
fn eat(
    data: &str,
    target_json_path: Option<String>,
    is_str_json: Option<bool>,
    is_records: Option<bool>,
    header_paths: Option<HashMap<String, String>>,
    loc: Option<&str>,
) -> String {
    let current_path = vec![];
    let value: Value = serde_json::from_str(data).expect("error");
    let is_records: bool = match is_records {
        None => false,
        Some(is_records) => is_records,
    };

    println!("IS RECORDS = {}", is_records);

    let mut _data: HashMap<String, Vec<Value>> = HashMap::new();
    let mut _types: HashMap<String, String> = HashMap::new();
    
    let headers = generate_headers(&value, header_paths.clone());


    match target_json_path {
        None => {
            if is_records && value.is_array() {
                // Make sure value is an array
                for (_i, v) in value.as_array().unwrap().iter().enumerate() {
                    let mut sample_obj = Sample::sample();
                    let headers = generate_headers(&v, header_paths.clone());
                    merge_headers(&mut sample_obj, &headers);

                    let current_path = vec![String::from("$root")];
                    deep_write(v, current_path, headers, &mut _data, &mut _types);
                }
                // Iterate through
            }

            deep_write(
                &value,
                current_path,
                headers,
                &mut _data, &mut _types
            );
        }
        Some(path) => {
            if is_records && value.is_array() {
                // Make sure value is an array

                for (_i, v) in value.as_array().unwrap().iter().enumerate() {


                    let target_value = v.pointer(path.as_str());
                    match target_value {
                        Some(target_value) => {
                            let current_path = vec![String::from("$root")];
                            let headers = generate_headers(&v, header_paths.clone());
                            
                            deep_write(
                                &target_value,
                                current_path,
                                headers,
                                &mut _data, &mut _types
                                
                            );
                        }
                        None => {
                            println!("No path found for[{}] {:?}", _i, path.as_str());
                            ()
                        }
                    }
                }
            // Iterate through
            } else {
                let target_value = value.pointer(path.as_str()).unwrap();

                match is_str_json {
                    None => {
                        let current_path = vec![String::from("$root")];
                            let headers = generate_headers(&target_value, header_paths.clone());
                        deep_write(
                            &target_value,
                            current_path,
                            headers,
                            &mut _data,
                            &mut _types
                        );
                    }
                    Some(is_str_json) => {
                        // If true we have to reparse this string
                        if is_str_json {
                            let v = serde_json::from_str(target_value.as_str().unwrap())
                                .expect("Invalid json_pointer target is not a json string");
                            let headers = generate_headers(&v, header_paths.clone());
            
                            deep_write(
                                &v,
                                current_path,
                                headers,
                                &mut _data,
                                &mut _types
                            );
                        } else {
                            deep_write(
                                &target_value,
                                current_path,
                                headers,
                                &mut _data,
                                &mut _types
                            );
                        }
                    }
                }
            }
        }
    }

    let loc: &str = match loc {
        Some(l) => l,
        None => "./out.pq",
    };

    let sample_schema = Schema::from_map(_types);
    let ret = String::from(&sample_schema.schema);
    write_to_file(loc, sample_schema, _data);
    return ret;
}

#[pymodule]
fn json_eater(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(eat, m)?)?;
    Ok(())
}
