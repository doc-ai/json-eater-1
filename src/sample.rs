
use pyo3::prelude::*;
#[allow(unused_imports)]
use pyo3::types::*;
#[allow(unused_imports)]
use rayon::prelude::*;


use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use pythonize::pythonize;


#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Sample {
    path: String,
    vuint: Option<u64>,
    vint: Option<i64>,
    vfloat: Option<f64>,
    vstr: Option<String>,
    vbool: Option<bool>,
}

pub struct SampleCollection{
    col: String,
    samples: Vec<Value>   
}

impl SampleCollection {

}

impl Sample {
    pub fn new() -> Self {
        Sample {
            path: String::from("$root"),
            vuint: None,
            vint: None,
            vfloat: None,
            vstr: None,
            vbool: None,
        }
    }

    pub fn default() -> Self {
        Sample::new()
    }

    pub fn with_path(self, path: Vec<String>) -> Self {
        Sample {
            path: path.clone().join("/"),
            ..self
        }
    }

    pub fn with_vuint(self, vuint: Option<u64>) -> Self {
        Sample { vuint, ..self }
    }

    pub fn with_vint(self, vint: Option<i64>) -> Self {
        Sample { vint, ..self }
    }

    pub fn with_vfloat(self, vfloat: Option<f64>) -> Self {
        Sample { vfloat, ..self }
    }

    pub fn with_vstr(self, vstr: Option<String>) -> Self {
        Sample { vstr, ..self }
    }

    pub fn with_vbool(self, vbool: Option<bool>) -> Self {
        Sample { vbool, ..self }
    }
}

pub fn merge_headers(py: Python, headers: Value, sample: pyo3::Py<pyo3::PyAny>) -> Py<PyAny> {
    let any: &PyAny = sample.into_ref(py);

    if headers.is_object() {
        for (k, v) in headers.as_object().unwrap() {
            match v {
                Value::Number(number) => {
                    if number.is_i64() {
                        any.set_item(k, number.as_i64()).unwrap();
                    } else if number.is_f64() {
                        any.set_item(k, number.as_f64()).unwrap();
                    } else if number.is_u64() {
                        any.set_item(k, number.as_u64()).unwrap();
                    }
                }
                Value::Bool(boolean) => {
                    any.set_item(k, boolean).unwrap();
                }
                Value::String(string) => {
                    any.set_item(k, string).unwrap();
                }
                _ => {}
            }
        }
    }

    let any: Py<PyAny> = Py::from(any);

    return any;
}

pub fn save_sample(
    py: Python,
    sample: &Sample,
    headers: Value,
    output: &mut Vec<pyo3::Py<pyo3::PyAny>>,
) {
    let sample = pythonize(py, sample).unwrap();
    if headers.is_object() {
        let any = merge_headers(py, headers, sample.clone());
        output.push(any);
    } else {
        output.push(sample);
    }
}

pub fn deep_keys_v(
    py: Python,
    value: &Value,
    current_path: Vec<String>,
    headers: Value,
    output: &mut Vec<pyo3::Py<pyo3::PyAny>>,
) {

    match value {
        Value::Object(map) => {

            if map.values().len() == 0 {
                let sample = Sample::default();
                let sample = sample.with_path(current_path.clone());
                save_sample(py, &sample, headers.clone(), output);
            }
            for (k, v) in map {
                let mut new_path = current_path.clone();
                new_path.push(k.to_owned());
                deep_keys_v(py, v, new_path, headers.clone(), output);
            }
        }
        Value::Array(array) => {
            if array.len() == 0 {
                let sample = Sample::default();
                let sample = sample.with_path(current_path.clone());
                save_sample(py, &sample, headers.clone(), output);
            }
            for (i, v) in array.iter().enumerate() {
                let mut new_path = current_path.clone();
                new_path.push(i.to_string().to_owned());
                deep_keys_v(py, v, new_path, headers.clone(), output);
            }
        }
        Value::Number(number) => {
            let sample = Sample::default();
            if number.is_i64() {
                let sample = sample.with_path(current_path).with_vint(number.as_i64());
                save_sample(py, &sample, headers, output);
            } else if number.is_u64() {
                let sample = sample.with_path(current_path).with_vuint(number.as_u64());
                save_sample(py, &sample, headers, output);
            } else if number.is_f64() {
                let sample = sample.with_path(current_path).with_vfloat(number.as_f64());
                save_sample(py, &sample, headers, output);
            }

            return ();
        }
        Value::String(string) => {
            let sample = Sample::default()
                .with_path(current_path)
                .with_vstr(Some(String::from(string)));
            save_sample(py, &sample, headers, output);

            return ();
        }
        Value::Bool(boolean) => {
            let mut v: bool = false;

            if *boolean {
                v = true;
            }

            let sample = Sample::default()
                .with_path(current_path)
                .with_vbool(Some(v));
            save_sample(py, &sample, headers, output);
            return ();
        }
        _ => (),
    }
}

// Extract top level leaf keys from parent of target
pub fn generate_headers(
   root: &Value,
   header_paths: Option<HashMap<String, String>>, 
) -> Value {
   
    let header_paths = match header_paths {
        Some(hp) => hp,
        _ => HashMap::new()
    };

    let mut header_value: Value = json!({});

    let header_value: &mut serde_json::Map<std::string::String, serde_json::Value> = 
        header_value.as_object_mut().unwrap();
    
    for (header_key,header_path) in header_paths {
        let header: &Value = match root.pointer(header_path.as_str()) {
            Some(header) => header,
            None => &Value::Null
        };

        let header = serde_json::to_string(header).unwrap();
        let header: Value = serde_json::from_str(header.as_str()).unwrap();

        header_value.insert(header_key, header);
        // get header name? 
    }

    let header_value = serde_json::to_value(header_value).unwrap();


    return header_value;
}
