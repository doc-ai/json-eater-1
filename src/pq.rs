use std::fs;
use std::path::Path;

use std::collections::HashMap;

use parquet::file::properties::WriterProperties;
use parquet::file::writer::{FileWriter, SerializedFileWriter};
use parquet::schema::parser::parse_message_type;

use serde_json::{Value, json};

use crate::sample::*;
#[derive(Debug, Clone)]
pub struct Schema {
    pub schema: String,
    pub cols_map: HashMap<String, usize>,
    pub cols: Vec<String>
}

impl Schema {
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
                        let mut finalstr = format!("\n \trequired");
                        match v {
                            Value::Number(number) => {
                                let mut num_type: String = String::default();
                                    if number.is_i64() {
                                      num_type = String::from("INT64");
                                    } else if number.is_u64() {
                                       num_type = String::from("INT64");
                                      
                                    } else if number.is_f64() {
                                       num_type = String::from("DOUBLE");
                                    } else {
                                        num_type = String::from("DOUBLE");
                                    }
                                
                                finalstr = format!("{} {} {};", finalstr, num_type,  schema_key);
                 
                            },
                            Value::String(_v_str) => {
                                finalstr = format!("{} BINARY {} (UTF8);", finalstr, schema_key);
                                
                            },
                            Value::Bool(_boolean) => { finalstr = format!("{} BOOLEAN {};", finalstr, schema_key) },
                            _ => ()

                        };
                        
                    
                        
             
                    
                    schema.push_str(&finalstr.to_owned());
                    }
                },
                _=>{}
            }
        } 
        
        schema.push_str(&"\n}".to_owned());

        Schema{
            schema,
            cols,
            cols_map
        }
    }
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
                    },
                    _ => {}
                }
        }
    return res;
}

fn gen_u64_slice(data: Vec<Value>) -> Vec<u64> {
    let mut res: Vec<u64> = vec![];
    for v in data {
            match v {
                    Value::Number(number) => {
                        if number.is_u64() {
                            res.push(number.as_u64().unwrap());
                        } 
                    },
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
                    },
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
                    },
                    _ => {}
                }
        }
    return res;
}

fn merge_headers(sample: &mut Value,headers: &Value) {
    match sample {
        Value::Object(sample_obj) => {
            match headers {
                Value::Object(header_obj) => {
                    for (k, v) in header_obj {
                        // Copy over keys. 
                        if sample_obj.contains_key(k) {
                            let k = format!("{}_{}", k, "from_header");
                        } 
                            sample_obj.insert(String::from(k), v.clone());
                        
                    }
                },
                _ => {}
            }
        },
        _ => {}
    }
}

fn save_sample(mut sample: Value, headers: Value, _data: &mut HashMap<String, Vec<Value>>) {
    merge_headers(& mut sample, &headers);
    match sample {
        Value::Object(sample_obj) => {
            for (k,v) in sample_obj {
                match _data.get_mut(&k) {
                    Some(vec_for_k) => {
                        vec_for_k.push(v);
                    },
                    _ => {
                        _data.insert(k, vec![v]);
                    }
                }
            }
        },
        _ => ()
    }
}


pub fn deep_write(schema: Schema, value: &Value, current_path: Vec<String>, headers: Value, _data: &mut HashMap<String, Vec<Value>>) {

    match value {
        Value::Object(map) => {
            if map.values().len() == 0 {
                let sample = Sample::default();
                let sample = sample.with_path(current_path.clone());
                
                save_sample(sample.to_value(), headers.clone(), _data);
            }
            for (k, v) in map {
                let mut new_path = current_path.clone();
                new_path.push(k.to_owned());
               deep_write(schema.clone(), v, new_path, headers.clone(), _data)
            }
        }
        Value::Array(array) => {
            if array.len() == 0 {
                let sample = Sample::default();
                let sample = sample.with_path(current_path.clone());
                save_sample(sample.to_value(), headers.clone(), _data);
            }
            for (i, v) in array.iter().enumerate() {
                let mut new_path = current_path.clone();
                new_path.push(i.to_string().to_owned());
                deep_write(schema.clone(), v, new_path, headers.clone(), _data)
           
            }
        }
        Value::Number(number) => {
            let sample = Sample::default();
            if number.is_i64() {
                let sample = sample.with_path(current_path).with_vint(number.as_i64());
                save_sample(sample.to_value(), headers.clone(), _data);
            } else if number.is_u64() {
                let sample = sample.with_path(current_path).with_vuint(number.as_u64());
                save_sample(sample.to_value(), headers.clone(), _data);
            } else if number.is_f64() {
                let sample = sample.with_path(current_path).with_vfloat(number.as_f64());
                save_sample(sample.to_value(), headers.clone(), _data);
            }

            return ();
        }
        Value::String(string) => {
            let sample = Sample::default()
                .with_path(current_path)
                .with_vstr(Some(String::from(string)));
                save_sample(sample.to_value(), headers.clone(), _data);

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
                save_sample(sample.to_value(), headers.clone(), _data);
            return ();
        }
        _ => ()
    }
}

fn pq_eat(data: &str, target_json_path: Option<String>,
    is_str_json: Option<bool>,
    is_records: Option<bool>,
    header_paths: Option<HashMap<String, String>>, loc: Option<&str>) {

    let current_path = vec![];
    let value: Value = serde_json::from_str(data).expect("error");
    let is_records: bool = match is_records {
        None => false,
        Some(is_records) => is_records,
    };

    let headers: Value = Value::Null;

    let mut _data: HashMap<String, Vec<Value>> = HashMap::new();

    let schema_obj = Schema::from_value(value);

    let loc: &str = match loc {
        Some(l) => l,
        None => "./"
    };

    let value: &mut std::vec::Vec<serde_json::Value> = _data.get_mut(&String::from("a")).unwrap();
    value.push(json!({"a": 1}));

    let message_type = Schema::from_value(json!({"a": 1}));

    write_to_file(loc, schema_obj, _data);

}

fn write_to_file(loc: &str,  message_type: Schema, mut _data: HashMap<String, Vec<Value>>) {

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
        match col_writer {
            parquet::column::writer::ColumnWriter::Int32ColumnWriter(ref mut typed_writer) => {
                typed_writer.write_batch(&[1], None, None).unwrap();
            },
            parquet::column::writer::ColumnWriter::Int64ColumnWriter(ref mut _tw) => {

            },
            parquet::column::writer::ColumnWriter::Int96ColumnWriter(ref mut _tw) => {

            },
            parquet::column::writer::ColumnWriter::FloatColumnWriter(ref mut _tw) => {

            },
            parquet::column::writer::ColumnWriter::DoubleColumnWriter(ref mut _tw) => {

            },
            parquet::column::writer::ColumnWriter::ByteArrayColumnWriter(ref mut _typed_writer) => {
                println!("BYTE ARRAY");
                let buf: Vec<u8> = String::from("hi").as_bytes().to_vec();
                _typed_writer.write_batch(&[parquet::data_type::ByteArray::from(buf)], None, None).unwrap();
            },
            parquet::column::writer::ColumnWriter::FixedLenByteArrayColumnWriter(ref mut _typed_writer) => {
                println!("FIXED BYTE ARRAY");
            },
            parquet::column::writer::ColumnWriter::BoolColumnWriter(ref mut _tw ) => {},
            
            
        }
        println!("COL IS {}", message_type.cols[row_id]);

        row_id = row_id + 1;
        // col_writer.
        row_group_writer.close_column(col_writer).unwrap();
    }
}