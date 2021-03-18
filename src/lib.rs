
use serde_json::{Value};
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use rayon::prelude::*;


fn deep_keys(value: &Value, current_path: Vec<String>, output: &mut Vec<Vec<String>>) {
    if current_path.len() > 0 {
        output.push(current_path.clone());
    }

    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let mut new_path = current_path.clone();
                new_path.push(k.to_owned());
                deep_keys(v,  new_path, output);

            }
        },
        Value::Array(array) => {
            for (i, v) in array.iter().enumerate() {
                let mut new_path = current_path.clone();
                new_path.push(i.to_string().to_owned());
                deep_keys(v,  new_path, output);
            }
        },
        _ => ()
    }
}

#[pyfunction]
fn eat(data: &str) -> Vec<Vec<String>> {
    let mut output = vec![vec![]];
    let current_path = vec![];
    let value:Value = serde_json::from_str(data).expect("error");
    deep_keys(&value, current_path, &mut output);
    return output
}


#[pymodule]
fn json_eater(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(eat, m)?)?;

    Ok(())
}