use std::fs;
use std::path::Path;

use std::collections::HashMap;

use parquet::file::properties::WriterProperties;
use parquet::file::writer::{FileWriter, SerializedFileWriter};
use parquet::schema::parser::parse_message_type;

use serde_json::{json, Value};

use crate::sample::*;

// Extract top level leaf keys from parent of target
pub fn generate_headers(root: &Value, header_paths: Option<HashMap<String, String>>) -> Value {
    let header_paths = match header_paths {
        Some(hp) => hp,
        _ => HashMap::new(),
    };

    let mut header_value: Value = json!({});

    let header_value: &mut serde_json::Map<std::string::String, serde_json::Value> =
        header_value.as_object_mut().unwrap();

    for (header_key, header_path) in header_paths {
        let header: &Value = match root.pointer(header_path.as_str()) {
            Some(header) => header,
            None => &Value::Null,
        };

        let header = serde_json::to_string(header).unwrap();
        let header: Value = serde_json::from_str(header.as_str()).unwrap();

        header_value.insert(header_key, header);
        // get header name?
    }

    let header_value = serde_json::to_value(header_value).unwrap();

    return header_value;
}
#[derive(Debug, Clone)]
pub struct Schema {
    pub schema: String,
    pub cols_map: HashMap<String, usize>,
    pub cols: Vec<String>,
}

pub fn value_to_str(v: &Value, schema_key: &String) -> String {
    let mut finalstr = format!("\n \t OPTIONAL");
    match v {
        Value::Number(number) => {
            let num_type: String;
            if number.is_i64() {
                num_type = String::from("INT64");
            } else if number.is_u64() {
                num_type = String::from("INT64");
            } else if number.is_f64() {
                num_type = String::from("DOUBLE");
            } else {
                num_type = String::from("DOUBLE");
            }

            finalstr = format!("{} {} {};", finalstr, num_type, schema_key);
        }
        Value::String(_v_str) => {
            finalstr = format!("{} BINARY {} (UTF8);", finalstr, schema_key);
        }
        Value::Bool(_boolean) => finalstr = format!("{} BOOLEAN {};", finalstr, schema_key),
        _ => (),
    };
    return finalstr;
}

impl Schema {
    pub fn from_map(types: HashMap<String, String>) -> Self {
        let mut schema: String = String::from("message schema {");
        let mut cols_map: HashMap<String, usize> = HashMap::new();
        let mut cols: Vec<String> = vec![];
        let mut row_number: usize = 0;
        for (k, v) in types {
            let schema_key: String = String::from(k.to_owned());
            cols_map.insert(k, row_number);
            cols.push(schema_key.clone());
            row_number += row_number;

            let finalstr = v;
            schema.push_str(&finalstr.to_owned());
        }
        schema.push_str(&"\n}".to_owned());

        Schema {
            schema,
            cols,
            cols_map,
        }
    }
    pub fn from_value(value: Value) -> Self {
        let mut schema: String = String::from("message schema {");
        let mut cols_map: HashMap<String, usize> = HashMap::new();
        let mut cols: Vec<String> = vec![];

        if value.is_object() {
            match value {
                Value::Object(map) => {
                    let mut row_number: usize = 0;
                    for (k, v) in map {
                        let schema_key: String = String::from(k.to_owned());
                        cols_map.insert(k, row_number);
                        cols.push(schema_key.clone());
                        row_number += row_number;
                        let finalstr = value_to_str(&v, &schema_key);

                        schema.push_str(&finalstr.to_owned());
                    }
                }
                _ => {}
            }
        }
        schema.push_str(&"\n}".to_owned());

        Schema {
            schema,
            cols,
            cols_map,
        }
    }
}

fn gen_definition_levels(data: Vec<Value>) -> Vec<i16> {
    let mut res: Vec<i16> = vec![];
    for v in data {
        match v {
            Value::Null => res.push(0),
            _ => res.push(1),
        }
    }
    return res;
}

fn gen_rep_levels(data: Vec<Value>) -> Vec<i16> {
    let mut res: Vec<i16> = vec![];
    for v in data {
        match v {
            _ => res.push(1),
        }
    }
    return res;
}

fn gen_bool_slice(data: Vec<Value>) -> Vec<bool> {
    let mut res: Vec<bool> = vec![];
    for v in data {
        match v {
            Value::Bool(boolean) => {
                res.push(boolean);
            }
            _ => {}
        }
    }
    return res;
}

fn gen_int64_slice(data: Vec<Value>) -> Vec<i64> {
    let mut res: Vec<i64> = vec![];
    for v in data {
        match v {
            Value::Number(number) => {
                if number.is_i64() {
                    res.push(number.as_i64().unwrap());
                }
            }
            _ => {}
        }
    }
    return res;
}

fn gen_f64_slice(data: Vec<Value>) -> Vec<f64> {
    let mut res: Vec<f64> = vec![];
    for v in data {
        match v {
            Value::Number(number) => {
                if number.is_f64() {
                    res.push(number.as_f64().unwrap());
                }
            }
            _ => {}
        }
    }
    return res;
}

fn gen_utf8_slice(data: Vec<Value>) -> Vec<String> {
    let mut res: Vec<String> = vec![];
    for v in data {
        match v {
            Value::String(string) => {
                res.push(string);
            }
            _ => {}
        }
    }
    return res;
}

pub fn merge_headers(sample: &mut Value, headers: &Value) {
    match sample {
        Value::Object(sample_obj) => {
            match headers {
                Value::Object(header_obj) => {
                    for (k, v) in header_obj {
                        // Copy over keys.
                        let mut key = String::from(k);
                        if sample_obj.contains_key(k) {
                            key = format!("{}_{}", key.to_owned(), "from_header");
                        }
                        sample_obj.insert(String::from(key), v.clone());
                    }
                }
                _ => {}
            }
        }
        _ => {}
    }
}

fn save_sample(
    mut sample: Value,
    headers: Value,
    _data: &mut HashMap<String, Vec<Value>>,
    _types: &mut HashMap<String, String>,
) {
    merge_headers(&mut sample, &headers);

    match sample {
        Value::Object(sample_obj) => {
            for (k, v) in sample_obj {
                // Confirm single type and add to type

                let key = k.to_string();
                let val = v.clone();
                match _data.get_mut(&k) {
                    Some(vec_for_k) => {
                        vec_for_k.push(v);
                    }
                    _ => {
                        _data.insert(k, vec![v]);
                    }
                }

                if ! &val.is_null() {
                    let type_candidate_str = value_to_str(&val, &key);
                    match _types.get(&key) {
                        Some(type_str) => {
                            if String::from(type_str) != type_candidate_str {
                                // println!("Inconsistent schema item! {}={} {}", key, type_candidate_str, type_str);
                            }
                        }
                        None => {
                            println!("Inserting {}  {}", key, type_candidate_str);
                            _types.insert(String::from(&key), value_to_str(&val, &key));
                        }
                    }
                }
            }
        }
        _ => (),
    }
}

pub fn deep_write(
    value: &Value,
    current_path: Vec<String>,
    headers: Value,
    _data: &mut HashMap<String, Vec<Value>>,
    _types: &mut HashMap<String, String>,
) {
    match value {
        Value::Object(map) => {
            if map.values().len() == 0 {
                let sample = Sample::default();
                let sample = sample.with_path(current_path.clone());
                save_sample(sample.to_value(), headers.clone(), _data, _types);
            }
            for (k, v) in map {
                let mut new_path = current_path.clone();
                new_path.push(k.to_owned());
                deep_write(v, new_path, headers.clone(), _data, _types)
            }
        }
        Value::Array(array) => {
            if array.len() == 0 {
                let sample = Sample::default();
                let sample = sample.with_path(current_path.clone());
                save_sample(sample.to_value(), headers.clone(), _data, _types);
            }
            for (i, v) in array.iter().enumerate() {
                let mut new_path = current_path.clone();
                new_path.push(i.to_string().to_owned());
                deep_write(v, new_path, headers.clone(), _data, _types)
            }
        }
        Value::Number(number) => {
            let sample = Sample::default();
            if number.is_i64() {
                let sample = sample.with_path(current_path).with_vint(number.as_i64());
                save_sample(sample.to_value(), headers.clone(), _data, _types)
            } else if number.is_u64() {
                let sample = sample.with_path(current_path).with_vuint(number.as_u64());
                save_sample(sample.to_value(), headers.clone(), _data, _types)
            } else if number.is_f64() {
                let sample = sample.with_path(current_path).with_vfloat(number.as_f64());
                save_sample(sample.to_value(), headers.clone(), _data, _types);
            }

            return ();
        }
        Value::String(string) => {
            let sample = Sample::default()
                .with_path(current_path)
                .with_vstr(Some(String::from(string)));
            save_sample(sample.to_value(), headers.clone(), _data, _types);

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
            save_sample(sample.to_value(), headers.clone(), _data, _types);
            return ();
        }
        _ => (),
    }
}

pub fn write_to_file(loc: &str, message_type: Schema, mut _data: HashMap<String, Vec<Value>>) {
    let path = Path::new(loc);

    // Generate Schema
    let schema = std::sync::Arc::new(parse_message_type(message_type.schema.as_str()).unwrap());
    let props = std::sync::Arc::new(WriterProperties::builder().build());
    let file = fs::File::create(&path).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();

    let mut row_id = 0;

    while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        // ... write values to a column writer
        let col = &message_type.cols[row_id];

        let data: Vec<Value> = match _data.get(col) {
            Some(d) => d.to_vec(),
            None => vec![],
        };

        let def_levels = gen_definition_levels(data.clone());
        let def_levels = &def_levels[..];
        let rep_levels = gen_rep_levels(data.clone());
        let rep_levels = &rep_levels[..];

        match col_writer {
            parquet::column::writer::ColumnWriter::Int64ColumnWriter(ref mut _tw) => {
                let slice = gen_int64_slice(data);
                _tw.write_batch(&slice, Some(def_levels), Some(rep_levels))
                    .unwrap();
            }

            parquet::column::writer::ColumnWriter::DoubleColumnWriter(ref mut _tw) => {
                let slice = gen_f64_slice(data);
                _tw.write_batch(&slice, Some(def_levels), Some(rep_levels))
                    .unwrap();
            }
            parquet::column::writer::ColumnWriter::ByteArrayColumnWriter(ref mut _tw) => {
                let slice = gen_utf8_slice(data);
                let buf: Vec<parquet::data_type::ByteArray> = slice
                    .into_iter()
                    .map(|x: String| {
                        let s = x.as_str();
                        let b: &[u8] = s.as_bytes();
                        parquet::data_type::ByteArray::from(b.to_vec())
                    })
                    .collect();

                _tw.write_batch(&buf[..], Some(def_levels), Some(rep_levels))
                    .unwrap();
            }
            parquet::column::writer::ColumnWriter::BoolColumnWriter(ref mut _tw) => {
                let slice = gen_bool_slice(data);
                _tw.write_batch(&slice, Some(def_levels), Some(rep_levels))
                    .unwrap();
            }
            _ => {}
        }

        row_id = row_id + 1;
        // col_writer.
        row_group_writer.close_column(col_writer).unwrap();
    }
    writer.close_row_group(row_group_writer).unwrap();
    writer.close().unwrap();
}
