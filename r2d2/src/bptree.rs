use std::path::Component::ParentDir;
use bson;
use bson::Bson;

pub struct LeafNode {
    min_size: usize,
    max_size: usize,
    keys: Vec<u128>,
    values: Vec<bson::Document>,
    // next_leaf may be null, need option
    next_leaf: Option<Box<LeafNode>>
}

impl LeafNode {
    pub fn new(num_keys: usize) -> LeafNode {
        LeafNode {
            // Create a vector of keys and a vector of BSON documents.
            // Each BSON represents a row of the table.
            min_size: num_keys.div_ceil(2) - 1,
            max_size: num_keys,
            keys: Vec::new(),
            values: Vec::new(),
            next_leaf: None
        }
    }

    pub fn print(self: &mut LeafNode) {
        print!("[");
        for i in 0..self.keys.len() {
            print!("{} : {}, ", self.keys[i], self.values[i]);
        }
        print!("]");
    }

    // find:
    // Returns index within node, if key is found.
    pub fn find(self: &mut LeafNode, key: u128) -> Option<usize> {
        for i in 0..self.keys.len() {
            if self.keys[i] == key {
                // Return index when found
                return Some(i);
            }
        }

        // No value if not found.
        return None;
    }

    // insert:
    // Insert key-value pair.
    // If operation is successful, return 0.
    // If node is full, return 1.
    // If key already exists, return 2.
    // If for some reason

    pub fn insert(self: &mut LeafNode, key: u128, value: bson::Document) -> i32 {
        // If node is full:
        if self.keys.len() == self.max_size {
            return 1;
        }

        else if self.keys.len() == 0 {
            self.keys.push(key);
            self.values.push(value);
            return 0;
        }

        else {
            let mut i = 0;
            while i < (self.keys.len() - 1) {
                if key == self.keys[i] {
                    return 2;
                }

                else if key < self.keys[i] {
                    break;
                }

                i += 1;
            }

            self.keys.insert(i, key);
            self.values.insert(i, value);
            return 0;
        }
    }


    // remove:
    // Return's a status code, and optionally the removed key-value pair if found.
    // If operation successful, return 0.
    // If key was found, but the resulting LeafNode after removing the key-value pair
    //      would be smaller than half its maximum length, return 1.
    // If key not found, return 2.
    pub fn remove(self: &mut LeafNode, key: u128) -> (i32, Option<(u128, bson::Document)>) {
        let found = self.find(key);
        // If key exists in LeafNode:
        if found.is_some() {
            // If result would be too small:
            if self.keys.len() == self.min_size {
                return (1, None);
            }

            else {
                let i = found.unwrap();
                return (0, Some( (self.keys.remove(i), self.values.remove(i) )));
            }
        }

        // If key not in LeafNode;
        else {
            return (2, None);
        }
    }

    // TODO: Set next
}


pub struct BPTree {
    root: LeafNode,
    // Keys per node.
    fan_out: usize,
    // Details for each column.
    // Column structure: {name:string, type:("int" | "string" | "double" | "bool")}
    schema: bson::Document,
}

impl BPTree {
    // TODO: new
    // TODO: insert key-value pair (includes split-and-push)
    // TODO: delete key-value pair (includes combine-and-pull)

    // TODO: find
    // TODO: print

    // TODO: query
    // TODO: serialize to file
    // TODO: deserialize from file


}