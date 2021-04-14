use std::fs;
use std::path::Path;

use std::collections::HashMap;

use parquet::file::properties::WriterProperties;
use parquet::file::writer::{FileWriter, SerializedFileWriter};
use parquet::schema::parser::parse_message_type;

use serde_json::{Value, json};



#[derive(Debug)]
struct Schema {
    pub schema: String,
    pub cols: Vec<String>
}


use serde_json::{Value};


#[derive(Debug)]
struct Schema {
    pub schema: String,
    pub cols: Vec<String>
}

impl Schema {
    pub fn from_value(value: Value) -> Self {
        let mut schema: String = String::from("message schema {");
        let mut cols: Vec<String> = vec![];

        if value.is_object() {
            match value {
                Value::Object(map) => {
                    for (k, v) in map {
                        let schema_key: String = String::from(k.to_owned());
                        cols.push(k.clone());
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
            cols
        }
    }
}






fn gen_bool_slice(data: Vec<Value>) -> Vec<bool> {
    let res: Vec<bool> = vec![];
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
    let res: Vec<i64> = vec![];
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
    let res: Vec<u64> = vec![];
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
    let res: Vec<f64> = vec![];
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
    let res: Vec<String> = vec![];
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



fn deep_write(schema: Schema, value: &Value, current_path: Vec<String>, headers: Value, output: &Vec<Value>) {

}



fn write_to_file(loc: &str) {

    let path = Path::new(loc);

    // Generate Schema
    let message_type = Schema::from_value(json!({"a": 1}));

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