use std::{fs, io};
use std::time::{SystemTime, UNIX_EPOCH};
use bplustree::GenericBPlusTree;
use bson::{doc, Bson, Document};
use bson::spec::ElementType;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use chrono::format::ParseError;

pub const FAN_OUT : usize = 10;
pub const DATA_PATH : &str = "data";

pub struct Database {
    bptree: GenericBPlusTree<u128, bson::Document, FAN_OUT, FAN_OUT>,
    schema: bson::Document,
    min_timestamp: u128,
    max_timestamp: u128,
}

fn type_string_to_bson_element (string: &str) -> Option<ElementType> {
    match string {
        "string" => Some(ElementType::String),
        "number" => Some(ElementType::Double),
        "boolean" => Some(ElementType::Boolean),
        _ => { eprintln!("INVALID TYPE : {}", string); None }
    }
}

impl Database {
    pub fn new(fields : Vec<String>, types : Vec<String>) -> Database {
        assert_eq!(fields.len(), types.len());

        let mut db = Database {
            bptree: bplustree::GenericBPlusTree::new(),
            schema: Document::new(),
            min_timestamp: u128::MAX,
            max_timestamp: u128::MIN,
        };

        for i in 0..fields.len() {
            db.schema.insert(fields[i].clone(), types[i].clone());
        }

        db
    }

    pub fn insert_to_database(&mut self, key : u128, val : bson::Document) {
        if self.min_timestamp > key {
            self.min_timestamp = key;
        }

        if self.max_timestamp < key {
            self.max_timestamp = key;
        }

        let already_in_tree = self.bptree.lookup(&key, |value| value.clone()).is_some();

        // If the key timestamp is already taken, the database will keep attempting
        // inserts until it finds the closest available timestamp.
        if already_in_tree {
            self.insert_to_database(key+1, val);
        }

        else {
            let entry_keys = val.keys();
            for key in entry_keys {
                if !self.schema.contains_key(&key) {
                    eprintln!("Missing key {} in database schema! Must be one of:", key);
                    for k in self.schema.keys() {
                        eprintln!("{}", k);
                    }

                    return;
                }
            }

            self.bptree.insert(key, val);
        }

    }

    pub fn remove(&mut self, key : u128) {
        self.bptree.remove(&key);
    }

    // Returns document laid out as so:
    // Document {
    //    "labels" : vec<strings>,
    //    "rows : Document...
    // }
    pub fn query(&mut self, query_string : String) -> Document {

        // Split fields using :: delimiter.
        let options = query_string.trim().split("::").collect::<Vec<&str>>();

        match options[0] {
            "LIST" => {
                match options[1] {
                    "ALL" => {
                        let results = self.get_range(u128::MIN, u128::MAX);

                        return doc![
                            "labels" : self.schema.keys().cloned().collect::<Vec<String>>(),
                            "rows" : results,
                        ]
                    }

                    "RANGE" => {
                        let bounds = options[2].split(",").collect::<Vec<&str>>();
                        let mut lower = u128::MIN;
                        let mut upper = u128::MAX;

                        let bound_0 = bounds[0].parse::<u128>().unwrap();
                        let bound_1 = bounds[1].parse::<u128>().unwrap();

                        if bound_0 < bound_1 {
                            lower = bound_0;
                            upper = bound_1;
                        }

                        else {
                            lower = bound_1;
                            upper = bound_0;
                        }

                        let results = self.get_range(lower, upper);
                        return doc![
                            "labels" : self.schema.keys().cloned().collect::<Vec<String>>(),
                            "rows" : results,
                        ]
                    }

                    "SAVED" => {
                        let file_list = list_files().unwrap_or(vec!["No saved databases found.".parse().unwrap()]);
                        let mut file_list_doc = bson::Document::new();
                        for (i, f) in file_list.iter().enumerate() {
                            file_list_doc.insert(&i.to_string(), f.clone());
                        }

                        return doc![
                            "labels" : ["Saved Databases"],
                            "rows" : file_list_doc,
                        ]
                    }

                    // METADATA:
                    _ => {
                        doc![
                            "labels" : ["size", "schema"],
                            "rows" : doc![
                                "size" : self.bptree.len() as i64,
                                "schema" : self.schema.clone(),
                            ]
                        ]
                    }
                }
            }

            "AGGREGATE" => {
                println!("{} {}", options[2], options[1]);
                let result = self.aggregate(options[2].parse().unwrap(), options[1].parse().unwrap());

                doc![
                    "labels" : [options[1]],
                    "rows" : doc![
                        options[1] : result as i64,
                    ]
                ]
            }

            "INSERT" => {
                let key_value_pairs = options[1]
                    .split(",")
                    .collect::<Vec<&str>>();

                let mut row_document = Document::new();

                for pair in key_value_pairs {
                    let (k, v) = pair.split_once("=").unwrap();

                    // If the key exists, and the data is valid, insert to database.
                    if self.schema.contains_key(k) {
                        let bson_value = match self.schema.get_str(k).unwrap_or("string") {
                            "bool" => Bson::Boolean(v.parse::<bool>().unwrap()),
                            "number" => Bson::Double(v.parse::<f64>().unwrap()),
                            "string" | _ => Bson::String(v.to_string()),
                        };

                        row_document.insert(k.to_string(), bson_value);
                    }
                }

                let mut timestamp= SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();

                if options.len() > 2 {
                    let t =
                        options[2].split("=")
                            .last()
                            .unwrap()
                            .parse::<String>()
                            .unwrap()
                            .parse::<u128>();

                    if t.is_ok() {
                        timestamp = t.unwrap();
                    }
                }

                if row_document.len() > 0 {
                    self.insert_to_database(timestamp, row_document);
                }

                // After inserting, list all.
                return self.query("LIST::ALL".to_string());
            }

            "REMOVE" => {
                match String::from(options[1]) {
                    String::from("ALL") => {
                        for k in self.bptree.raw_iter() {
                            self.remove(k);
                        }

                        return self.query("LIST::ALL".to_string());
                    }

                    String::from("ONE") => {
                        if options[2].contains("TIME") {
                            let timestamp = options[2].split("=")[1].parse::<u128>();

                            if timestamp.is_some() {
                                self.bptree.remove(&timestamp.unwrap());
                            }
                        }

                        return self.query("LIST::ALL".to_string());
                    }

                    _ => {
                        // If invalid, just do nothing, and list whole database.
                        return self.query("LIST::ALL".to_string());
                    }
                }
            }

            "TIME" | _ => {
                println!("{}", options[1]);

                let timestamp = NaiveDateTime::parse_from_str(&options[1].replace("%20", " "), "%Y-%m-%d %H:%M:%S")
                    .unwrap()
                    .and_utc()
                    .timestamp();

                return doc![
                    "labels" : ["time"],
                    "rows" : doc![
                        "time" : timestamp.to_string(),
                    ]
                ]
            }
        }
    }

    pub fn get_one(&self, key : u128) -> Option<bson::Document> {
        self.bptree.lookup(&key, |value| value.clone() )
    }

    pub fn get_range(&self, start_key : u128, end_key : u128) -> Document {
        let mut result = doc![];
        let mut iter = self.bptree.raw_iter();

        // Put the cursor immediately at the given start index, or the next available spot.
        iter.seek(&start_key);
        let mut cursor = iter.next();

        while cursor.is_some() && cursor.unwrap().0 <= &end_key {
            let (current_key, current_row) = cursor.unwrap();
            result.insert(current_key.to_string(), current_row);

            cursor = iter.next();
        }

        result
    }

    pub fn aggregate(&self, operation: String, field_name: String) -> f64 {
        let mut result : f64 = 0.0;

        if operation == "MIN" {
            result = f64::MAX;
        }

        let mut iter = self.bptree.raw_iter();

        // Put the cursor immediately at the given start index, or the next available spot.
        iter.seek_to_first();
        let mut cursor = iter.next();

        while cursor.is_some() {
            let (current_key, current_row) = cursor.unwrap();
            let value = current_row.get(&field_name);

            if value.is_some() {
                let parsed_value = value.unwrap();
                print!("{:?}", parsed_value);


                let converted = match parsed_value.element_type() {
                    ElementType::Double => parsed_value.as_f64().unwrap(),
                    ElementType::Int32 => (parsed_value.as_i32().unwrap()) as f64,
                    ElementType::Int64 => (parsed_value.as_i64().unwrap()) as f64,
                    _ => 0.0
                };

                match operation.as_str() {
                    "MIN" => {
                        if converted < result {
                            result = converted;
                        }
                    }

                    "MAX" => {
                        if converted > result {
                            result = converted;
                        }
                    }

                    "SUM" | "AVG" | _ => {
                        result += converted;
                    }
                }
            }

            cursor = iter.next();
        }

        if operation == "AVG" {
            result /= self.bptree.len() as f64;
        }

        result
    }
}


fn list_files() -> Result<Vec<String>, io::Error> {
    let paths = fs::read_dir(DATA_PATH)?;

    let mut result = Vec::new();

    for path in paths {
        let path = path?;
        let filename = path.file_name();
        let filename_str = filename.to_str().unwrap_or("unknown");
        result.push(String::from(filename_str));
    }

    Ok(result)
}