use bplustree::GenericBPlusTree;
use bson::spec::ElementType;
use bson::{doc, Bson, Document};
use chrono::NaiveDateTime;
use std::fs::File;
use std::io::Write;
use std::io::{Error, BufRead, BufReader};
use std::time::{SystemTime, UNIX_EPOCH};
use std::fs;

pub const FAN_OUT : usize = 2000;
pub const DATA_PATH : &str = "./data";

// Arbitrary limits to how large schema files may be.
pub const SCHEMA_MAX_NUM_COLUMNS : usize = 50000;
pub const SCHEMA_MAX_COLUMN_LABEL_LEN : usize = 128;

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

    pub fn load_schema_from_file(&mut self, filename: String) -> Result<(), String> {
        let full_path = format!("{}/{}", DATA_PATH, filename);

        // Check if file exists
        if !std::path::Path::new(&full_path).exists() {
            return Err(format!("Schema file '{}' not found", filename));
        }

        // Edge case: handle schema files that contain too many columns.
        // Open and read the file
        let line_count_file = File::open(&full_path)
            .map_err(|e| format!("Failed to open schema file: {}", e))?;

        let mut line_count_reader = BufReader::new(line_count_file);

        let file_line_count = line_count_reader.lines().count();
        if file_line_count > SCHEMA_MAX_NUM_COLUMNS {
            return Err(format!("Schema file '{}' is too long; this database supports a maximum of {} columns, the loaded schema contains {}", filename, SCHEMA_MAX_NUM_COLUMNS, file_line_count));
        }

        // Open and read the file
        let file = File::open(&full_path)
            .map_err(|e| format!("Failed to open schema file: {}", e))?;

        let mut reader = BufReader::new(file);

        let mut fields = Vec::new();
        let mut types = Vec::new();

        // Parse CSV content
        for (line_num, line) in reader.lines().enumerate() {
            let line = line.map_err(|e| format!("Error reading line {}: {}", line_num + 1, e))?;
            let line = line.trim();

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Parse CSV line
            let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
            if parts.len() != 2 {
                return Err(format!("Invalid format on line {}: expected 'column,type', got '{}'", line_num + 1, line));
            }

            // Edge case: truncate columns with names that are too long
            let column_name = if parts[0].to_string().len() < SCHEMA_MAX_COLUMN_LABEL_LEN {
                parts[0].to_string()

            } else {
                parts[0].to_string().chars().take(SCHEMA_MAX_COLUMN_LABEL_LEN).collect()

            };

            // Edge case: column name collision (already in fields)
            if fields.contains(&column_name) {
                return Err(format!("Column {} defined more than once (new value on line {})", column_name, line_num + 1));
            }

            let column_type = parts[1].to_string();

            // Validate column type
            match column_type.as_str() {
                "string" | "number" | "boolean" => {
                    fields.push(column_name);
                    types.push(column_type);
                }
                _ => {
                    return Err(format!("Invalid column type '{}' on line {}: must be 'string', 'number', or 'boolean'", column_type, line_num + 1));
                }
            }
        }

        if fields.is_empty() {
            return Err("Schema file contains no valid column definitions".to_string());
        }

        // Create new database with the loaded schema
        *self = Database::new(fields, types);

        Ok(())
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
    pub fn query(&mut self, query_string: String) -> Document {
        let options = query_string.trim().split("::").collect::<Vec<&str>>();

        match options[0] {
            "LIST" => self.handle_list_query(&options, &query_string),
            "AGGREGATE" => self.handle_aggregate_query(&options),
            "INSERT" => self.handle_insert_query(&options),
            "REMOVE" => self.handle_remove_query(&options),
            "TIME" => self.handle_time_query(&options),
            "SAVE" => self.handle_save_query(&options),
            "LOAD" => self.handle_load_query(&options),
            _ => notice_page(format!("Invalid query: {}", query_string)),
        }
    }

    fn handle_list_query(&mut self, options: &[&str], query_string: &str) -> Document {
        if options.len() < 2 {
            return notice_page("LIST command requires a category".to_string());
        }

        match options[1] {
            "ALL" => {
                let results = self.get_range(u128::MIN, u128::MAX);
                if query_string.contains("HIDE") {
                    notice_page(String::from("Success!"))
                } else {
                    doc![
                        "labels" : self.schema.keys().cloned().collect::<Vec<String>>(),
                        "rows" : results,
                    ]
                }
            }
            "ONE" => {
                if options.len() < 3 {
                    return notice_page("LIST::ONE requires a timestamp".to_string());
                }

                let key = match options[2].parse::<u128>() {
                    Ok(k) => k,
                    Err(_) => return notice_page("Invalid timestamp format".to_string()),
                };

                let result = self.bptree.lookup(&key, |value| value.clone());
                if result.is_some() {
                    if query_string.contains("HIDE") {
                        notice_page(String::from("Requested value found."))
                    } else {
                        doc![
                            "labels" : self.schema.keys().cloned().collect::<Vec<String>>(),
                            "rows" : doc![options[2] : result.unwrap()]
                        ]
                    }
                } else {
                    notice_page(String::from("Requested value could not be found."))
                }
            }
            "RANGE" => {
                if options.len() < 3 {
                    return notice_page("LIST::RANGE requires bounds in format 'lower,upper'".to_string());
                }

                let bounds = options[2].split(",").collect::<Vec<&str>>();
                if bounds.len() != 2 {
                    return notice_page("Invalid range format. Use 'lower,upper'".to_string());
                }

                let bound_0 = match bounds[0].parse::<u128>() {
                    Ok(b) => b,
                    Err(_) => return notice_page("Invalid lower bound".to_string()),
                };
                let bound_1 = match bounds[1].parse::<u128>() {
                    Ok(b) => b,
                    Err(_) => return notice_page("Invalid upper bound".to_string()),
                };

                let (lower, upper) = if bound_0 <= bound_1 { (bound_0, bound_1) } else { (bound_1, bound_0) };
                let results = self.get_range(lower, upper);

                if query_string.contains("HIDE") {
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

                doc![
                    "labels" : ["Saved Databases"],
                    "rows" : file_list_doc,
                ]
            }
            _ => {
                // METADATA or any other option
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

    fn handle_aggregate_query(&self, options: &[&str]) -> Document {
        if options.len() < 3 {
            return notice_page("AGGREGATE command requires field name and operation".to_string());
        }

        let field_name = options[1].to_string();
        let operation = options[2].to_string();
        let result = self.aggregate(operation.clone(), field_name.clone());

        doc![
            "labels" : [operation.as_str()],
            "rows" : doc![
                operation.as_str() : result as i64,
            ]
        ]
    }

    fn handle_insert_query(&mut self, options: &[&str]) -> Document {
        if options.len() < 2 {
            return notice_page("INSERT command requires key-value pairs".to_string());
        }

        let key_value_pairs = options[1].split(",").collect::<Vec<&str>>();
        let mut row_document = Document::new();

        for pair in key_value_pairs {
            if let Some((k, v)) = pair.split_once("=") {
                if self.schema.contains_key(k) {
                    let bson_value = match self.schema.get_str(k).unwrap_or("string") {
                        "bool" => Bson::Boolean(v.parse::<bool>().unwrap_or(false)),
                        "number" => Bson::Double(v.parse::<f64>().unwrap_or(0.0)),
                        "string" | _ => Bson::String(v.to_string()),
                    };
                    row_document.insert(k.to_string(), bson_value);
                }
            }
        }

        let mut timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();

        if options.len() > 2 {
            if let Some(t_str) = options[2].split("=").last() {
                if let Ok(t) = t_str.parse::<u128>() {
                    timestamp = t;
                }
            }
        }

        if row_document.len() > 0 {
            self.insert_to_database(timestamp, row_document);
        }

        notice_page(String::from("Success!"))
    }

    fn handle_remove_query(&mut self, options: &[&str]) -> Document {
        if options.len() < 2 {
            return notice_page("REMOVE command requires a category".to_string());
        }

        match options[1] {
            "ONE" => {
                if options.len() < 3 {
                    return notice_page("REMOVE::ONE requires timestamp parameter".to_string());
                }

                if options[2].contains("TIME") {
                    if let Some(timestamp_str) = options[2].split("=").nth(1) {
                        if let Ok(timestamp) = timestamp_str.parse::<u128>() {
                            self.bptree.remove(&timestamp);
                            return notice_page(String::from("Success!"));
                        }
                    }
                }
                notice_page(String::from("Entry not found."))
            }
            "ALL" => {
                self.bptree = Box::new(bplustree::GenericBPlusTree::new());
                self.min_timestamp = u128::MAX;
                self.max_timestamp = u128::MIN;
                notice_page(String::from("Success!"))
            }
            _ => {
                notice_page(String::from("Invalid query. Currently, only one entry can be removed at a time, by timestamp."))
            }
        }
    }

    fn handle_time_query(&self, options: &[&str]) -> Document {
        if options.len() < 2 {
            return notice_page("TIME command requires a timestamp string".to_string());
        }

        let timestamp = match NaiveDateTime::parse_from_str(&options[1].replace("%20", " "), "%Y-%m-%d %H:%M:%S") {
            Ok(dt) => dt.and_utc().timestamp() * 1000,
            Err(_) => return notice_page("Invalid timestamp format. Use YYYY-MM-DD HH:MM:SS".to_string()),
        };

        doc![
            "labels" : ["time"],
            "rows" : doc![
                "time" : timestamp.to_string(),
            ]
        ]
    }

    fn handle_save_query(&mut self, options: &[&str]) -> Document {
        if options.len() < 2 {
            return notice_page(String::from("Provide a filename to save the database. Example: 'SAVE::database_name'"));
        }

        if options[1].contains("CSV") {
            self.data_to_csv();
            notice_page(String::from("Dumped database to CSV."))
        } else {
            let sanitized = sanitize_filename::sanitize(options[1].to_string());
            self.save(sanitized);
            self.query("LIST::SAVED".to_string())
        }
    }

    fn handle_load_query(&mut self, options: &[&str]) -> Document {
        if options.len() < 2 {
            return notice_page(String::from("Provide a filename to load. Example: 'LOAD::database_name' or 'LOAD::SCHEMA::schema_file.schema.r2d2'"));
        }

        match options[1] {
            "SCHEMA" => {
                if options.len() < 3 {
                    return notice_page(String::from("Provide a schema filename. Example: 'LOAD::SCHEMA::schema_file.schema.r2d2'"));
                }

                match self.load_schema_from_file(options[2].to_string()) {
                    Ok(_) => {
                        // Return metadata with success message
                        let mut metadata_doc = doc![
                            "labels" : ["message", "schema"],
                            "rows" : doc![
                                "message" : "Successfully loaded schema",
                                "schema" : self.schema.clone(),
                            ]
                        ];
                        metadata_doc
                    }
                    Err(error_msg) => {
                        notice_page(format!("Error loading schema: {}", error_msg))
                    }
                }
            }
            _ => {
                // Regular database load
                let result = self.load(sanitize_filename::sanitize(options[1].to_string()));
                if result.is_ok() {
                    self.query("LIST::ALL".to_string())
                } else {
                    notice_page(String::from("LOAD operation failed. See server logs."))
                }
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
