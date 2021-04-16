
use serde::{Deserialize, Serialize};
use serde_json::{Value};




#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Sample {
    path: String,
    vuint: Option<u64>,
    vint: Option<i64>,
    vfloat: Option<f64>,
    vstr: Option<String>,
    vbool: Option<bool>,
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

    pub fn to_value(self) -> Value {
        match serde_json::value::to_value(self) {
            Ok(v) => v,
            _ => Value::default()
        }
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

