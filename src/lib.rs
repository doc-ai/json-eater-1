use pyo3::prelude::*;
#[allow(unused_imports)]
use pyo3::types::*;
use pyo3::wrap_pyfunction;
#[allow(unused_imports)]
use rayon::prelude::*;

use std::collections::HashMap;
use serde_json::{Value, json};

mod pq;

mod sample;
use sample::*;

use std::path::*;
use std::path::PathBuf;



#[pyfunction]
fn pq_eat(
    data: &str,
    loc_path: &str,
    target_json_path: Option<String>,
    is_str_json: Option<bool>,
    is_records: Option<bool>,
    header_paths: Option<HashMap<String, String>>,

) -> PyResult<String> {

    let loc_path: &Path= Path::new(data);

    if !loc_path.is_dir() {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("Error message"));
    } 

    let value: Value = serde_json::from_str(data).expect("error");
    let is_records: bool = match is_records {
        None => false,
        Some(is_records) => is_records,
    };

    let headers: Value = Value::Null;

    // if is_records we should also copy over headers to the sample?
    match target_json_path {
        None => {
            if is_records && value.is_array() {
                // Make sure value is an array
                for (_i, v) in value.as_array().unwrap().iter().enumerate() {
                    
                    let headers = generate_headers(&v, header_paths.clone());
                    let current_path = vec![String::from("$root")];
                    deep_keys_v(py, v, current_path, headers.clone(), &mut output);
                }
                // Iterate through
            }

            deep_keys_v(py, &value, current_path, headers.clone(), &mut output);
        }
        Some(path) => {
            if is_records && value.is_array() {
                // Make sure value is an array

                for (_i, v) in value.as_array().unwrap().iter().enumerate() {
                    
                    let headers = generate_headers(&v, header_paths.clone());
                    let target_value = v.pointer(path.as_str());
                    match target_value {
                        Some(target_value) => {
                            let current_path = vec![String::from("$root")];
                            deep_keys_v(py, target_value, current_path, headers.clone(), &mut output);
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
                        deep_keys_v(py, &target_value, current_path, headers.clone(), &mut output);
                    }
                    Some(is_str_json) => {
                        // If true we have to reparse this string
                        if is_str_json {
                            let v = serde_json::from_str(target_value.as_str().unwrap())
                                .expect("Invalid json_pointer target is not a json string");

                            deep_keys_v(py, &v, current_path, headers.clone(), &mut output);
                        } else {
                            deep_keys_v(py, &target_value, current_path, headers.clone(), &mut output);
                        }
                    }
                }
            }
        }
    }
    

    Ok(String::from("Ok"))

    
}

#[pyfunction]
fn eat(
    data: &str,
    target_json_path: Option<String>,
    is_str_json: Option<bool>,
    is_records: Option<bool>,
    header_paths: Option<HashMap<String, String>>,
    
) -> Vec<pyo3::Py<pyo3::PyAny>> {
    let gil = Python::acquire_gil();
    let py = gil.python();
    let mut output: Vec<pyo3::Py<pyo3::PyAny>> = vec![];

    let current_path = vec![];
    let value: Value = serde_json::from_str(data).expect("error");
    let is_records: bool = match is_records {
        None => false,
        Some(is_records) => is_records,
    };

    let headers: Value = Value::Null;

    // if is_records we should also copy over headers to the sample?
    match target_json_path {
        None => {
            if is_records && value.is_array() {
                // Make sure value is an array
                for (_i, v) in value.as_array().unwrap().iter().enumerate() {
                    
                    let headers = generate_headers(&v, header_paths.clone());
                    let current_path = vec![String::from("$root")];
                    deep_keys_v(py, v, current_path, headers.clone(), &mut output);
                }
                // Iterate through
            }

            deep_keys_v(py, &value, current_path, headers.clone(), &mut output);
        }
        Some(path) => {
            if is_records && value.is_array() {
                // Make sure value is an array

                for (_i, v) in value.as_array().unwrap().iter().enumerate() {
                    
                    let headers = generate_headers(&v, header_paths.clone());
                    let target_value = v.pointer(path.as_str());
                    match target_value {
                        Some(target_value) => {
                            let current_path = vec![String::from("$root")];
                            deep_keys_v(py, target_value, current_path, headers.clone(), &mut output);
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
                        deep_keys_v(py, &target_value, current_path, headers.clone(), &mut output);
                    }
                    Some(is_str_json) => {
                        // If true we have to reparse this string
                        if is_str_json {
                            let v = serde_json::from_str(target_value.as_str().unwrap())
                                .expect("Invalid json_pointer target is not a json string");

                            deep_keys_v(py, &v, current_path, headers.clone(), &mut output);
                        } else {
                            deep_keys_v(py, &target_value, current_path, headers.clone(), &mut output);
                        }
                    }
                }
            }
        }
    }

    return output;
}

#[pymodule]
fn json_eater(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(eat, m)?)?;
    Ok(())
}
