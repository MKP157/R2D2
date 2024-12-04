use bplustree::GenericBPlusTree;
use bson::{doc, Document};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use chrono::format::ParseError;

const FAN_OUT : usize = 10;

pub struct Database {
    bptree: GenericBPlusTree<u128, bson::Document, FAN_OUT, FAN_OUT>,
    schema: bson::Document,
    min_timestamp: u128,
    max_timestamp: u128,
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

    pub fn insert(&mut self, key : u128, val : bson::Document) {
        if self.min_timestamp > key {
            self.min_timestamp = key;
        }

        if self.max_timestamp < key {
            self.max_timestamp = key;
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

    pub fn remove(&mut self, key : u128) {
        self.bptree.remove(&key);
    }

    // Returns document laid out as so:
    // Document {
    //    "labels" : vec<strings>,
    //    "rows : Document...
    // }
    pub fn query(&self, query_string : String) -> Document {
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

                    // METADATA:
                    _ => {
                        doc![
                            "labels" : ["size"],
                            "rows" : doc![
                                "size" : self.bptree.len() as i64,
                            ]
                        ]
                    }
                }
            }

            "AGGREGATE" => {
                let result = self.aggregate(options[2].parse().unwrap(), options[1].parse().unwrap());

                doc![
                    "labels" : [options[1]],
                    "rows" : doc![
                        options[1] : result as i64,
                    ]
                ]
            }

            "TIME" | _ => {
                let timestamp = NaiveDateTime::parse_from_str(options[1], "%Y-%m-%d %H:%M:%S")
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
        let mut iter = self.bptree.raw_iter();

        // Put the cursor immediately at the given start index, or the next available spot.
        iter.seek_to_first();
        let mut cursor = iter.next();

        while cursor.is_some() {
            let (current_key, current_row) = cursor.unwrap();

            // TODO: Fix aggregates
            match operation.as_str() {
                "MIN" => {
                    let temp = current_row.get(&field_name).unwrap().as_f64().unwrap();
                    if temp < result {
                        result = temp;
                    }
                }

                "MAX" => {
                    let temp = current_row.get(&field_name).unwrap().as_f64().unwrap();
                    if temp > result {
                        result = temp;
                    }
                }

                "SUM" | "AVG" | _ => {
                    result += current_row.get(&field_name).unwrap().as_f64().unwrap();
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