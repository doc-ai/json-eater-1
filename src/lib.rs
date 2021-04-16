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
) {
    let current_path = vec![];
    let value: Value = serde_json::from_str(data).expect("error");
    let is_records: bool = match is_records {
        None => false,
        Some(is_records) => is_records,
    };

    let mut _data: HashMap<String, Vec<Value>> = HashMap::new();

    let mut sample_obj = Sample::default()
        .with_vbool(Some(true))
        .with_vfloat(Some(0.0))
        .with_vint(Some(0))
        .with_vstr(Some(String::default()))
        .with_vuint(Some(0))
        .to_value();
    let headers = generate_headers(&value, header_paths.clone());
    merge_headers(&mut sample_obj, &headers);
    let sample_schema = Schema::from_value(sample_obj);

    match target_json_path {
        None => {
            if is_records && value.is_array() {
                // Make sure value is an array
                for (_i, v) in value.as_array().unwrap().iter().enumerate() {
                    let headers = generate_headers(&v, header_paths.clone());
                    let current_path = vec![String::from("$root")];
                    deep_write(sample_schema.clone(), v, current_path, headers, &mut _data);
                }
                // Iterate through
            }

            deep_write(
                sample_schema.clone(),
                &value,
                current_path,
                headers,
                &mut _data,
            );
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

                            deep_write(
                                sample_schema.clone(),
                                &target_value,
                                current_path,
                                headers,
                                &mut _data,
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
                        deep_write(
                            sample_schema.clone(),
                            &target_value,
                            current_path,
                            headers,
                            &mut _data,
                        );
                    }
                    Some(is_str_json) => {
                        // If true we have to reparse this string
                        if is_str_json {
                            let v = serde_json::from_str(target_value.as_str().unwrap())
                                .expect("Invalid json_pointer target is not a json string");

                            deep_write(
                                sample_schema.clone(),
                                &v,
                                current_path,
                                headers,
                                &mut _data,
                            );
                        } else {
                            deep_write(
                                sample_schema.clone(),
                                &target_value,
                                current_path,
                                headers,
                                &mut _data,
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

    write_to_file(loc, sample_schema, _data);
}

#[pymodule]
fn json_eater(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(eat, m)?)?;
    Ok(())
}
