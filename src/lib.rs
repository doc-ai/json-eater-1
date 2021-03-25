

use serde::{Serialize, Deserialize};
use serde_json::{Value};
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
#[allow(unused_imports)]
use rayon::prelude::*;


use pythonize::{pythonize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Sample {
    path: Vec<String>,
    vuint: Option<u64>,
    vint: Option<i64>,
    vfloat: Option<f64>,
    vstr: Option<String>,
    vbool: Option<bool>
}

impl Sample {
    pub fn new() -> Self {
        Sample {
            path: vec![String::from("$root")],
            vuint:  None,
            vint:   None,
            vfloat: None,
            vstr:   None,
            vbool:  None
        }
    }

    pub fn default() -> Self { Sample::new() }

    pub fn with_path(self, path: Vec<String>) -> Self {
        Sample {
            path: path.clone(),
            ..self
        }
    }

    pub fn with_vuint(self, vuint: Option<u64>) -> Self {
        Sample {
            vuint,
            ..self
        }
    }

    pub fn with_vint(self, vint: Option<i64>) -> Self {
        Sample {
            vint,
            ..self
        }
    }

    pub fn with_vfloat(self, vfloat: Option<f64>) -> Self {
        Sample {
            vfloat,
            ..self
        }
    }

    pub fn with_vstr(self, vstr: Option<String>) -> Self {
        Sample {
            vstr,
            ..self
        }
    }

    pub fn with_vbool(self, vbool: Option<bool>) -> Self {
        Sample {
            vbool,
            ..self
        }
    }
}




fn deep_keys_v(py: Python, value: &Value, current_path: Vec<String>, output: &mut Vec<pyo3::Py<pyo3::PyAny>>) {
    // if current_path.len() > 0 {
    //     output.push(current_path.clone());
    // }

    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let mut new_path = current_path.clone();
                new_path.push(k.to_owned());
                deep_keys_v(py, v,  new_path, output);

            }
        },
        Value::Array(array) => {
            for (i, v) in array.iter().enumerate() {
                let mut new_path = current_path.clone();
                new_path.push(i.to_string().to_owned());
                deep_keys_v(py, v,  new_path, output);
            }
        },
        Value::Number(number) => {
            let sample = Sample::default();
            if number.is_i64(){
                let sample = sample
                    .with_path(current_path)
                    .with_vint(number.as_i64());
                let sample = pythonize(py, &sample).unwrap();
                output.push(sample);

            } else if number.is_u64(){
                  let sample = sample
                    .with_path(current_path)
                    .with_vuint(number.as_u64());
                    let sample = pythonize(py, &sample).unwrap();
                    output.push(sample);
            } else if number.is_f64(){
                let sample = sample
                    .with_path(current_path)
                    .with_vfloat(number.as_f64());
                    let sample = pythonize(py, &sample).unwrap();
                    output.push(sample);
                
            }
            
            return ()
        },
        Value::String(string) => {
            let sample = Sample::default()
                    .with_path(current_path)
                    .with_vstr(Some(String::from(string)));
                    let sample = pythonize(py, &sample).unwrap();
                    output.push(sample);
                
            return ()
        },
        Value::Bool(boolean) => {
            let mut v: bool = false;

            if *boolean {
                v = true;
            }

            let sample = Sample::default()
            .with_path(current_path)
            .with_vbool(Some(v));
            let sample = pythonize(py, &sample).unwrap();
            output.push(sample);
            return ()
        },
        _ => ()
    }
}




#[pyfunction]
fn eat(data: &str, target_json_path: Option<String>, is_str_json: Option<bool>) -> Vec<pyo3::Py<pyo3::PyAny>> {

    let gil = Python::acquire_gil();
    let py = gil.python();
    let mut output: Vec<pyo3::Py<pyo3::PyAny>> = vec![];

    let current_path = vec![];
    let mut value:Value = serde_json::from_str(data).expect("error");
    match target_json_path {
        None => (),
        Some(path) => {
            let target_value = value.pointer(path.as_str()).unwrap();
            match is_str_json {
                None => (),
                Some(is_str_json) => {
                    // If true we have to reparse this string 
                    if is_str_json {
                        value = serde_json::from_str(target_value.as_str().unwrap())
                                .expect("Invalid json_pointer target is not a json string");
                    }
                }
            }
        }
    }
    deep_keys_v(py, &value, current_path, &mut output);
    
    return output
}



#[pymodule]
fn json_eater(_py: Python<'_>, m: &PyModule) -> PyResult<()> {

    m.add_function(wrap_pyfunction!(eat, m)?)?;
    Ok(())
}