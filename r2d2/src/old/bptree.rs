use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::ops::Deref;
use std::path::Component::ParentDir;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use bson;
use bson::Bson;

fn is_internal<T: ?Sized + Any>(_s: &T) -> bool {
    TypeId::of::<InternalNode>() == TypeId::of::<T>()
}

fn is_leaf<T: ?Sized + Any>(_s: &T) -> bool {
    TypeId::of::<LeafNode>() == TypeId::of::<T>()
}

type NodePointer = Arc<Mutex<dyn Any>>;

pub struct LeafNode {
    min_size: usize,
    max_size: usize,
    keys: Vec<u128>,
    values: Vec<bson::Document>,
    // next_leaf may be null, need option
    next_leaf: Option<NodePointer>
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

    pub fn print(self: &LeafNode) {
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
    pub fn set_next_leaf(self: &mut LeafNode, next_leaf: NodePointer) {
        self.next_leaf = Some(next_leaf.clone());
    }
}

// ChildNodeType can be either InternalNode or LeafNode.
pub struct InternalNode {
    min_size: usize,
    max_size: usize,
    child_type: TypeId,
    keys: Vec<u128>,
    // Vector of pointers to leaf nodes, or
    pointers: Vec<NodePointer>
}


impl InternalNode {
    // TODO: new
    pub fn new(max_size: usize, child_type: TypeId) -> InternalNode {
        assert!(
            child_type == TypeId::of::<InternalNode>()
                || child_type == TypeId::of::<LeafNode>()
        );


        InternalNode {
            min_size: 1,
            max_size,
            child_type,
            keys: Vec::new(),
            pointers: Vec::new()
        }
    }

    // TODO: print
    pub fn print(self: &mut InternalNode) {

        // Process the node's value
        print!("<< ");
        for i in 0..self.keys.len() {
            print!("({}), ", self.keys[i]);
        }
        print!(">>");


    }

    // TODO: find
    pub fn find(self: &mut InternalNode, key: u128) -> Option<usize> {
        for i in 0..self.keys.len() {
            if self.keys[i] == key {
                // Return index when found
                return Some(i);
            }
        }

        // No value if not found.
        return None;
    }

    // TODO: insert
    pub fn insert(self: &mut InternalNode, key: u128, pointer: NodePointer) -> i32 {
        // If node is full:
        if self.keys.len() == self.max_size {
            return 1;
        }

        else if self.keys.len() == 0 {
            self.keys.push(key);
            self.pointers.push(pointer);
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
            self.pointers.insert(i, pointer);
            return 0;
        }
    }

    // TODO: remove
    pub fn remove(self: &mut InternalNode, key: u128) -> (i32, Option<(u128, NodePointer)>) {
        let found = self.find(key);

        // If key exists in LeafNode:
        if found.is_some() {
            // If result would be too small:
            if self.keys.len() == self.min_size {
                return (1, None);
            }

            else {
                let i = found.unwrap();
                return (0, Some( (self.keys.remove(i), self.pointers.remove(i) )));
            }
        }

        // If key not in LeafNode;
        else {
            return (2, None);
        }
    }

    // TODO:
}

pub struct BPlusTree {
    // Mutable, atomic, shared pointer to a single internal node.
    root: NodePointer,
    // Keys per node.
    fan_out: usize,
    // Details for each column.
    // Column structure: {name:string, type:("int" | "string" | "double" | "bool")}
    schema: bson::Document,
}

impl BPlusTree {
    // TODO: new

    // TODO: insert key-value pair (includes split-and-push)
    // insert:
    // Insert key-value pair.
    // If operation is successful, return 0.
    // If node is full, return 1.
    // If key already exists, return 2.
    pub fn insert(self: &mut BPlusTree, key: u128, value: bson::Document) -> i32 {
        let mut z = self.root;

        while z.child_type == TypeId::of::<InternalNode>() {
            for i in 0..z.keys.len() {
                if z.keys[i] > key {
                    z = z.pointers.get(i).unwrap().as_ref();
                    break;
                }

                else if z.keys[i] == key {
                    return 2;
                }
            }
        }

        assert_eq!(z.child_type, TypeId::of::<LeafNode>());
        for i in 0..z.keys.len() {
            if z.keys[i] > key {
                z.insert();
                break;
            }

            else if z.keys[i] == key {
                return 2;
            }
        }


        return 696969
    }

    // TODO: delete key-value pair (includes combine-and-pull)

    // TODO: find
    // TODO: print

    // TODO: select query
    // TODO: range query

    // TODO: serialize to file
    // TODO: deserialize from file


}