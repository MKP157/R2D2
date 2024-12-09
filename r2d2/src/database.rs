use bplustree::GenericBPlusTree;
use bson::spec::ElementType;
use bson::{doc, Bson, Document};
use chrono::NaiveDateTime;
use std::fs::File;
use std::io::Write;
use std::io::{Error};
use std::time::{SystemTime, UNIX_EPOCH};
use std::fs;

pub const FAN_OUT : usize = 2000;
pub const DATA_PATH : &str = "./data";

pub struct Database {
    bptree: Box<GenericBPlusTree<u128, bson::Document, FAN_OUT, FAN_OUT>>,
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

fn document_to_csv_row(doc : Document, time : u128) -> String {
    let mut csv = String::new();
    csv.push_str(format!("{}", time).as_str());

    for (_, content) in doc {
        // Don't want label! Ignore it
        let converted = match content.element_type() {
            ElementType::Double => content.as_f64().unwrap().to_string(),
            ElementType::Int32 => content.as_i32().unwrap().to_string(),
            ElementType::Int64 => content.as_i64().unwrap().to_string(),
            ElementType::String => content.as_str().unwrap().to_string(),
            ElementType::Boolean => content.as_bool().unwrap().to_string(),
            _ => String::from("None")
        };

        csv.push_str(format!(",{}", converted).as_str());
    }

    csv.push_str("\n");

    return csv;
}

fn notice_page(notice: String) -> Document {
    return doc![
        "labels" : ["Notice"],
        "rows" : doc![
            "Notice" : notice,
        ]
    ]
}

impl Database {
    pub fn new(fields : Vec<String>, types : Vec<String>) -> Database {
        assert_eq!(fields.len(), types.len());

        let mut db = Database {
            bptree: Box::new(bplustree::GenericBPlusTree::new()),
            schema: Document::new(),
            min_timestamp: u128::MAX,
            max_timestamp: u128::MIN,
        };

        for i in 0..fields.len() {
            db.schema.insert(fields[i].clone(), types[i].clone());
        }

        db
    }

    pub fn insert_to_database(&mut self, mut key : u128, val : bson::Document) {
        if self.min_timestamp > key {
            self.min_timestamp = key;
        }

        if self.max_timestamp < key {
            self.max_timestamp = key;
        }

        let mut already_in_tree = self.bptree.lookup(&key, |value| value.clone()).is_some();
        while already_in_tree {
            key = key + 1;
            already_in_tree = self.bptree.lookup(&key, |value| value.clone()).is_some();
        }


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

                        return if query_string.contains("HIDE") {
                            notice_page(String::from("Success!"))
                        } else {
                            doc![
                                "labels" : self.schema.keys().cloned().collect::<Vec<String>>(),
                                "rows" : results,
                            ]
                        }
                    }

                    "ONE" => {
                        let key = options[2].parse::<u128>().unwrap();
                        let result = self.bptree.lookup(&key, |value| value.clone());

                        if result.is_some() {
                            return if query_string.contains("HIDE") {
                                notice_page(String::from("Requested value found."))
                            } else {
                                doc![
                                "labels" : self.schema.keys().cloned().collect::<Vec<String>>(),
                                "rows" : doc![
                                        options[2] : result.unwrap()
                                    ]
                                ]
                            }
                        }

                        else {
                            notice_page(String::from("Requested value could not be found."))
                        }
                    }

                    "RANGE" => {
                        let bounds = options[2].split(",").collect::<Vec<&str>>();
                        let lower;
                        let upper;

                        let bound_0 = bounds[0].parse::<u128>().unwrap();
                        let bound_1 = bounds[1].parse::<u128>().unwrap();

                        if bound_0 <= bound_1 {
                            lower = bound_0;
                            upper = bound_1;
                        }

                        else {
                            lower = bound_1;
                            upper = bound_0;
                        }

                        let results = self.get_range(lower, upper);

                        return if query_string.contains("HIDE") {
                            notice_page(String::from("Success!"))
                        } else {
                            doc![
                                "labels" : self.schema.keys().cloned().collect::<Vec<String>>(),
                                "rows" : results,
                            ]
                        }
                    }

                    "SAVED" => {
                        let file_list = list_files(DATA_PATH.parse().unwrap()).unwrap_or(vec!["No saved databases found.".parse().unwrap()]);
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

                return notice_page(String::from("Success!"));
            }

            "REMOVE" => {
                match options[1] {

                   "ONE" => {
                        if options[2].contains("TIME") {
                            let timestamp = options[2].split("=").collect::<Vec<&str>>()[1].parse::<u128>();

                            if timestamp.is_ok() {
                                self.bptree.remove(&timestamp.unwrap());
                                return notice_page(String::from("Success!"));
                            }
                        }

                       return notice_page(String::from("Entry not found."));
                    }

                    _ => {
                        return notice_page(String::from("Invalid query. Currently, only one entry can be removed at a time, by timestamp."));
                    }
                }
            }

            "TIME" => {
                println!("{}", options[1]);

                let timestamp = NaiveDateTime::parse_from_str(&options[1].replace("%20", " "), "%Y-%m-%d %H:%M:%S")
                    .unwrap()
                    .and_utc()
                    .timestamp() * 1000;

                return doc![
                    "labels" : ["time"],
                    "rows" : doc![
                        "time" : timestamp.to_string(),
                    ]
                ]
            }

            "SAVE" => {
                if options.len() < 2 {
                    return notice_page(String::from("Provide a filename to save the database. Example: 'SAVE::database_name'"));
                }

                else if options[1].contains("CSV") {
                    self.data_to_csv();
                    return notice_page(String::from("Dumped database to CSV."));
                }
                else {
                    let sanitized : String = sanitize_filename::sanitize(options[1].to_string());
                    self.save(sanitized);
                    return self.query("LIST::SAVED".to_string());
                }
            }

            "LOAD" => {
                if options.len() < 2 {
                    return notice_page(String::from("Provide a filename to load the database. Example: 'LOAD::<database name>'"));
                }

                else {
                    let result = self.load(
                        sanitize_filename::sanitize(options[1].to_string())
                    );

                    return if result.is_ok() {
                        self.query("LIST::ALL".to_string())
                    } else {
                        notice_page(String::from("LOAD operation failed. See server logs."))
                    }
                }
            }

            _ => return notice_page(format!("Invalid query: {}", query_string)),
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
            let (_, current_row) = cursor.unwrap();
            let value = current_row.get(&field_name);

            if value.is_some() {
                let parsed_value = value.unwrap();
                //print!("parsed -> {:?}", parsed_value);

                let converted;
                if parsed_value.as_str().is_some() && parsed_value.as_str().unwrap().parse::<f64>().is_ok() {
                    converted = parsed_value.as_str().unwrap().parse::<f64>().unwrap();
                }

                else {
                    converted = match parsed_value.element_type() {
                        ElementType::Double => parsed_value.as_f64().unwrap(),
                        ElementType::Int32 => (parsed_value.as_i32().unwrap()) as f64,
                        ElementType::Int64 => (parsed_value.as_i64().unwrap()) as f64,
                        _ => 0.0
                    };
                }

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

    pub fn save(&self, filename: String) {
        // Because the Rust BSON package itself does not support u128, we will need
        // to convert them to strings, and parse them when loading the database.

        let mut serialized = Document::new();
        let mut serialized_rows = Document::new();
        serialized.insert("schema", self.schema.clone());
        serialized.insert("min_timestamp", self.min_timestamp.clone().to_string());
        serialized.insert("max_timestamp", self.max_timestamp.clone().to_string());

        let mut cursor = self.bptree.raw_iter();
        cursor.seek_to_first();

        let mut current_row = cursor.next();
        while current_row.is_some() {
            let (key, row) = current_row.unwrap();

            serialized_rows.insert(key.clone().to_string(), row.clone());
            current_row = cursor.next();
        }

        serialized.insert("rows", serialized_rows);
        save_file_unique(filename, serialized).unwrap();
    }

    pub fn load (&mut self, filename: String) -> Result<(), Error>{
        if list_files(DATA_PATH.parse().unwrap())?.contains(&filename) {
            let file_document = Document::from_reader(File::open(format!("{DATA_PATH}/{filename}"))?);

            if file_document.is_err() {
                println!("{}", file_document.unwrap_err());
            }

            else {
                let deserialized_document = file_document.unwrap();

                let schema = deserialized_document.get("schema").unwrap().as_document().unwrap();
                let schema_keys = schema.keys().collect::<Vec<&String>>();
                let schema_types = schema.values();

                let schema_keys_s = schema_keys.iter().map(|k| k.to_string()).collect::<Vec<String>>();
                let schema_types_s = schema_types.into_iter().map(|k| k.to_string()).collect::<Vec<String>>();

                *self = Database::new(
                    schema_keys_s, schema_types_s
                );

                self.min_timestamp = deserialized_document.get("min_timestamp").unwrap().as_str().unwrap().parse::<u128>().unwrap();
                self.max_timestamp = deserialized_document.get("max_timestamp").unwrap().as_str().unwrap().parse::<u128>().unwrap();

                let rows = deserialized_document.get("rows").unwrap().as_document().unwrap();
                for (key, row) in rows.iter() {
                    self.insert_to_database(key.parse::<u128>().unwrap(), row.as_document().unwrap().clone());
                }
            }
        }

        Ok(())
    }

    pub fn data_to_csv(&mut self) {
        let mut f = File::create(format!("{}/dump.csv", DATA_PATH)).expect("Unable to create file");
        let mut header = String::from("timestamp");
        for k in self.schema.keys() {
            header.push_str(format!(",{}", &k).as_str());
        }

        header.push_str("\n");
        f.write(header.as_bytes()).expect("Unable to write header");

        let mut cursor = self.bptree.raw_iter();
        cursor.seek_to_first();

        let mut current_row = cursor.next();
        while current_row.is_some() {
            let (key, row) = current_row.unwrap();
            let s = document_to_csv_row(row.clone(), key.clone());
            f.write(s.as_bytes()).expect("Unable to write data");

            current_row = cursor.next();
        }
    }
}


fn list_files(path: String) -> Result<Vec<String>, Error> {
    let paths = fs::read_dir(path)?;

    let mut result = Vec::new();

    for path in paths {
        let path = path?;
        let filename = path.file_name();
        let filename_str = filename.to_str().unwrap_or("unknown");
        result.push(String::from(filename_str));
    }

    Ok(result)
}

fn save_file_unique(mut filename: String, data: Document) -> Result<(), Error> {
    let directory_list = list_files(DATA_PATH.parse().unwrap())?;

    let mut new_filename = filename.clone().split(".").collect::<Vec<&str>>()[0].to_string();
    new_filename.push_str(".r2d2");

    if !directory_list.is_empty() {
        let mut postfix = 1;
        let mut exists = directory_list.contains(&new_filename);

        while exists {
            println!("{} exists...", new_filename);

            new_filename = filename.clone().split(".").collect::<Vec<&str>>()[0].to_string();
            new_filename = new_filename.replace(".r2d2", "");
            new_filename.push_str(format!("_{}.r2d2", postfix).as_str());

            exists = directory_list.contains(&new_filename);
            postfix += 1;
        }
    }

    let mut output_file = File::create(format!("{}/{}", DATA_PATH, new_filename))?;
    let mut v : Vec<u8> = Vec::new();
    data.to_writer(&mut v).unwrap();

    output_file.write_all(&v)?;

    Ok(())
}
