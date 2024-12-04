use std::sync::{Arc, Mutex};
use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct Node {
    keys: Vec<i32>,
    parent: Option<Arc<Mutex<Node>>>,
    children: Vec<Arc<Mutex<Node>>>,
    values: Vec<i32>,
    next: Option<Arc<Mutex<Node>>>,
    prev: Option<Arc<Mutex<Node>>>,
    is_leaf: bool,
}

impl Node {
    pub fn new(parent: Option<Arc<Mutex<Node>>>, is_leaf: bool, prev: Option<Arc<Mutex<Node>>>, next: Option<Arc<Mutex<Node>>>) -> Self {
        let node = Node {
            keys: Vec::new(),
            parent,
            children: Vec::new(),
            values: Vec::new(),
            next: next.clone(),
            prev: prev.clone(),
            is_leaf,
        };

        if let Some(ref next_node) = next {
            next_node.lock().unwrap().prev = Some(Arc::new(Mutex::new(node.clone())));
        }

        if let Some(ref prev_node) = prev {
            prev_node.lock().unwrap().next = Some(Arc::new(Mutex::new(node.clone())));
        }

        return node;
    }

    pub fn index_of_child(&self, key: i32) -> usize {
        self.keys.iter().position(|&k| key < k).unwrap_or(self.keys.len())
    }

    pub fn index_of_key(&self, key: i32) -> isize {
        self.keys.iter().position(|&k| k == key).map(|i| i as isize).unwrap_or(-1)
    }

    pub fn get_child(&self, key: i32) -> Arc<Mutex<Node>> {
        self.children[self.index_of_child(key)].clone()
    }

    pub fn set_child(&mut self, key: i32, value: Vec<Arc<Mutex<Node>>>) {
        let i = self.index_of_child(key);
        self.keys.insert(i, key);
        self.children.splice(i..i, value);
    }

    pub fn split_internal(&mut self) -> (i32, Arc<Mutex<Node>>, Arc<Mutex<Node>>) {
        let mid = self.keys.len() / 2;
        let left = Arc::new(Mutex::new(Node::new(self.parent.clone(), false, None, None)));

        left.lock().unwrap().keys.extend_from_slice(&self.keys[..mid]);
        left.lock().unwrap().children.extend_from_slice(&self.children[..=mid]);

        for child in left.lock().unwrap().children.iter() {
            child.lock().unwrap().parent = Some(left.clone());
        }

        let key = self.keys[mid];
        self.keys.remove(mid);
        self.children.drain(..=mid);

        (key, left, Arc::new(Mutex::new(self.clone())))
    }

    pub fn get(&self, key: i32) -> i32 {
        match self.index_of_key(key) {
            -1 => {
                println!("key {} not found", key);
                -1
            }
            index => self.values[index as usize],
        }
    }

    pub fn set(&mut self, key: i32, value: i32) {
        let i = self.index_of_child(key);
        if !self.keys.contains(&key) {
            self.keys.insert(i, key);
            self.values.insert(i, value);
        } else {
            self.values[i] = value;
        }
    }

    pub fn split_leaf(&mut self) -> (i32, Arc<Mutex<Node>>, Arc<Mutex<Node>>) {
        let mid = self.keys.len() / 2;
        let left = Arc::new(Mutex::new(Node::new(self.parent.clone(), true, self.prev.clone(), Some(Arc::new(Mutex::new(self.clone()))))));

        left.lock().unwrap().keys.extend_from_slice(&self.keys[..mid]);
        left.lock().unwrap().values.extend_from_slice(&self.values[..mid]);

        self.keys.drain(..mid);
        self.values.drain(..mid);

        (self.keys[0], left, Arc::new(Mutex::new(self.clone())))
    }
}

#[derive(Debug)]
pub struct BPlusTree {
    root: Arc<Mutex<Node>>,
    max_capacity: usize,
    min_capacity: usize,
    depth: usize,
}

impl BPlusTree {
    pub fn new(max_capacity: usize) -> Self {
        let root = Arc::new(Mutex::new(Node::new(None, true, None, None)));
        let max_capacity = if max_capacity > 2 { max_capacity } else { 2 };
        let min_capacity = max_capacity / 2;

        BPlusTree {
            root,
            max_capacity,
            min_capacity,
            depth: 0,
        }
    }

    pub fn find_leaf(&self, key: i32) -> Arc<Mutex<Node>> {
        let mut node = self.root.clone();
        while !node.lock().unwrap().is_leaf {
            node = node.clone().lock().unwrap().get_child(key);
        }
        node
    }

    pub fn get(&self, key: i32) -> i32 {
        self.find_leaf(key).lock().unwrap().get(key)
    }

    pub fn set(&mut self, key: i32, value: i32) {
        let leaf = self.find_leaf(key);
        leaf.lock().unwrap().set(key, value);
        if leaf.lock().unwrap().keys.len() > self.max_capacity {
            self.insert(leaf.lock().unwrap().split_leaf());
        }
    }

    pub fn insert(&mut self, result: (i32, Arc<Mutex<Node>>, Arc<Mutex<Node>>)) {
        let (key, left, right) = result;
        let parent = right.lock().unwrap().parent.clone();
        if parent.is_none() {
            let new_root = Arc::new(Mutex::new(Node::new(None, false, None, None)));
            left.lock().unwrap().parent = Some(new_root.clone());
            right.lock().unwrap().parent = Some(new_root.clone());
            self.depth += 1;
            println!("KEY!! {}", key);
            new_root.lock().unwrap().keys.push(key);
            new_root.lock().unwrap().children.push(left);
            new_root.lock().unwrap().children.push(right);
            self.root = new_root;
            return;
        }
        parent.as_ref().unwrap().lock().unwrap().set_child(key, vec![left, right]);
        if parent.as_ref().unwrap().lock().unwrap().keys.len() > self.max_capacity {
            self.insert(parent.as_ref().unwrap().lock().unwrap().split_internal());
        }
    }

    pub fn remove_from_leaf(&mut self, key: i32, node: Arc<Mutex<Node>>) {
        let index = node.lock().unwrap().index_of_key(key);
        if index == -1 {
            println!("Key {} not found! Exiting ...", key);
            std::process::exit(0);
        }
        node.lock().unwrap().keys.remove(index as usize);
        node.lock().unwrap().values.remove(index as usize);
        if let Some(ref parent) = node.lock().unwrap().parent {
            let index_in_parent = parent.lock().unwrap().index_of_child(key);
            if index_in_parent != 0 {
                parent.lock().unwrap().keys[index_in_parent as usize - 1] = node.lock().unwrap().keys[0];
            }
        }
    }

    pub fn remove_from_internal(&mut self, key: i32, node: Arc<Mutex<Node>>) {
        let index = node.lock().unwrap().index_of_key(key);
        if index != -1 {
            let left_most_leaf = node.lock().unwrap().children[index as usize + 1].clone();
            let mut left_most_leaf = left_most_leaf.lock().unwrap();
            while !left_most_leaf.is_leaf {
                left_most_leaf = left_most_leaf.children[0].lock().unwrap();
            }
            node.lock().unwrap().keys[index as usize] = left_most_leaf.keys[0];
        }
    }

    pub fn borrow_key_from_right_leaf(&mut self, node: Arc<Mutex<Node>>, next: Arc<Mutex<Node>>) {
        node.lock().unwrap().keys.push(next.lock().unwrap().keys.remove(0));
        node.lock().unwrap().values.push(next.lock().unwrap().values.remove(0));
        if let Some(ref parent) = node.lock().unwrap().parent {
            for (i, child) in parent.lock().unwrap().children.iter().enumerate() {
                if Arc::ptr_eq(child, &next) {
                    parent.lock().unwrap().keys[i - 1] = next.lock().unwrap().keys[0];
                    break;
                }
            }
        }
    }

    pub fn borrow_key_from_left_leaf(&mut self, node: Arc<Mutex<Node>>, prev: Arc<Mutex<Node>>) {
        node.lock().unwrap().keys.insert(0, prev.lock().unwrap().keys.pop().unwrap());
        node.lock().unwrap().values.insert(0, prev.lock().unwrap().values.pop().unwrap());
        if let Some(ref parent) = node.lock().unwrap().parent {
            for (i, child) in parent.lock().unwrap().children.iter().enumerate() {
                if Arc::ptr_eq(child, &node) {
                    parent.lock().unwrap().keys[i - 1] = node.lock().unwrap().keys[0];
                    break;
                }
            }
        }
    }

    pub fn merge_node_with_right_leaf(&mut self, node: Arc<Mutex<Node>>, next: Arc<Mutex<Node>>) {
        node.lock().unwrap().keys.extend(next.lock().unwrap().keys.iter().cloned());
        node.lock().unwrap().values.extend(next.lock().unwrap().values.iter().cloned());
        node.lock().unwrap().next = next.lock().unwrap().next.clone();
        if let Some(ref next_node) = node.lock().unwrap().next {
            next_node.lock().unwrap().prev = Some(node.clone());
        }
        if let Some(ref parent) = node.lock().unwrap().parent {
            for (i, child) in parent.lock().unwrap().children.iter().enumerate() {
                if Arc::ptr_eq(child, &next) {
                    parent.lock().unwrap().keys.remove(i - 1);
                    parent.lock().unwrap().children.remove(i);
                    break;
                }
            }
        }
    }

    pub fn merge_node_with_left_leaf(&mut self, node: Arc<Mutex<Node>>, prev: Arc<Mutex<Node>>) {
        prev.lock().unwrap().keys.extend(node.lock().unwrap().keys.iter().cloned());
        prev.lock().unwrap().values.extend(node.lock().unwrap().values.iter().cloned());
        prev.lock().unwrap().next = node.lock().unwrap().next.clone();
        if let Some(ref next_node) = prev.lock().unwrap().next {
            next_node.lock().unwrap().prev = Some(prev.clone());
        }
        if let Some(ref parent) = node.lock().unwrap().parent {
            for (i, child) in parent.lock().unwrap().children.iter().enumerate() {
                if Arc::ptr_eq(child, &node) {
                    parent.lock().unwrap().keys.remove(i - 1);
                    parent.lock().unwrap().children.remove(i);
                    break;
                }
            }
        }
    }

    pub fn borrow_key_from_right_internal(&mut self, my_position_in_parent: usize, node: Arc<Mutex<Node>>, next: Arc<Mutex<Node>>) {
        node.lock().unwrap().keys.push(node.lock().unwrap().parent.as_ref().unwrap().lock().unwrap().keys[my_position_in_parent]);
        node.lock().unwrap().parent.as_ref().unwrap().lock().unwrap().keys[my_position_in_parent] = next.lock().unwrap().keys.remove(0);
        node.lock().unwrap().children.push(next.lock().unwrap().children.remove(0));
        node.lock().unwrap().children.last().unwrap().lock().unwrap().parent = Some(node.clone());
    }

    pub fn borrow_key_from_left_internal(&mut self, my_position_in_parent: usize, node: Arc<Mutex<Node>>, prev: Arc<Mutex<Node>>) {
        node.lock().unwrap().keys.insert(0, node.lock().unwrap().parent.as_ref().unwrap().lock().unwrap().keys[my_position_in_parent - 1]);
        node.lock().unwrap().parent.as_ref().unwrap().lock().unwrap().keys[my_position_in_parent - 1] = prev.lock().unwrap().keys.pop().unwrap();
        node.lock().unwrap().children.insert(0, prev.lock().unwrap().children.pop().unwrap());
        node.lock().unwrap().children[0].lock().unwrap().parent = Some(node.clone());
    }

    pub fn merge_node_with_right_internal(&mut self, my_position_in_parent: usize, node: Arc<Mutex<Node>>, next: Arc<Mutex<Node>>) {
        node.lock().unwrap().keys.push(node.lock().unwrap().parent.as_ref().unwrap().lock().unwrap().keys[my_position_in_parent]);
        node.lock().unwrap().parent.as_ref().unwrap().lock().unwrap().keys.remove(my_position_in_parent);
        node.lock().unwrap().parent.as_ref().unwrap().lock().unwrap().children.remove(my_position_in_parent + 1);
        node.lock().unwrap().keys.extend(next.lock().unwrap().keys.iter().cloned());
        node.lock().unwrap().children.extend(next.lock().unwrap().children.iter().cloned());
        for child in node.lock().unwrap().children.iter() {
            child.lock().unwrap().parent = Some(node.clone());
        }
    }

    pub fn merge_node_with_left_internal(&mut self, my_position_in_parent: usize, node: Arc<Mutex<Node>>, prev: Arc<Mutex<Node>>) {
        prev.lock().unwrap().keys.push(node.lock().unwrap().parent.as_ref().unwrap().lock().unwrap().keys[my_position_in_parent - 1]);
        node.lock().unwrap().parent.as_ref().unwrap().lock().unwrap().keys.remove(my_position_in_parent - 1);
        node.lock().unwrap().parent.as_ref().unwrap().lock().unwrap().children.remove(my_position_in_parent);
        prev.lock().unwrap().keys.extend(node.lock().unwrap().keys.iter().cloned());
        prev.lock().unwrap().children.extend(node.lock().unwrap().children.iter().cloned());
        for child in prev.lock().unwrap().children.iter() {
            child.lock().unwrap().parent = Some(prev.clone());
        }
    }

    pub fn remove(&mut self, key: i32, node: Option<Arc<Mutex<Node>>>) {
        let node = node.unwrap_or_else(|| self.find_leaf(key));
        if node.lock().unwrap().is_leaf {
            self.remove_from_leaf(key, node.clone());
        } else {
            self.remove_from_internal(key, node.clone());
        }

        if node.lock().unwrap().keys.len() < self.min_capacity {
            if Arc::ptr_eq(&node, &self.root) {
                if node.lock().unwrap().keys.is_empty() && !node.lock().unwrap().children.is_empty() {
                    self.root = node.lock().unwrap().children[0].clone();
                    self.root.lock().unwrap().parent = None;
                    self.depth -= 1;
                }
                return;
            } else if node.lock().unwrap().is_leaf {
                let next = node.lock().unwrap().next.clone();
                let prev = node.lock().unwrap().prev.clone();

                if let Some(next_node) = next {
                    if Arc::ptr_eq(next_node.lock().unwrap().parent.as_ref().unwrap(), node.lock().unwrap().parent.as_ref().unwrap()) && next_node.lock().unwrap().keys.len() > self.min_capacity {
                        self.borrow_key_from_right_leaf(node.clone(), next_node);
                    } else if let Some(prev_node) = prev {
                        if Arc::ptr_eq(prev_node.lock().unwrap().parent.as_ref().unwrap(), node.lock().unwrap().parent.as_ref().unwrap()) && prev_node.lock().unwrap().keys.len() > self.min_capacity {
                            self.borrow_key_from_left_leaf(node.clone(), prev_node);
                        } else if next_node.lock().unwrap().keys.len() <= self.min_capacity {
                            self.merge_node_with_right_leaf(node.clone(), next_node);
                        } else if prev_node.lock().unwrap().keys.len() <= self.min_capacity {
                            self.merge_node_with_left_leaf(node.clone(), prev_node);
                        }
                    }
                }
            } else {
                let my_position_in_parent = node.lock().unwrap().parent.as_ref().unwrap().lock().unwrap().children.iter().position(|child| Arc::ptr_eq(child, &node)).unwrap();
                let next = if my_position_in_parent + 1 < node.lock().unwrap().parent.as_ref().unwrap().lock().unwrap().children.len() {
                    Some(node.lock().unwrap().parent.as_ref().unwrap().lock().unwrap().children[my_position_in_parent + 1].clone())
                } else {
                    None
                };

                let prev = if my_position_in_parent > 0 {
                    Some(node.lock().unwrap().parent.as_ref().unwrap().lock().unwrap().children[my_position_in_parent - 1].clone())
                } else {
                    None
                };

                if let Some(next_node) = next {
                    if Arc::ptr_eq(next_node.lock().unwrap().parent.as_ref().unwrap(), node.lock().unwrap().parent.as_ref().unwrap()) && next_node.lock().unwrap().keys.len() > self.min_capacity {
                        self.borrow_key_from_right_internal(my_position_in_parent, node.clone(), next_node);
                    }
                    else if next_node.lock().unwrap().keys.len() <= self.min_capacity {
                        self.merge_node_with_right_internal(my_position_in_parent, node.clone(), next_node);
                    }
                }

                if let Some(prev_node) = prev {
                    if Arc::ptr_eq(prev_node.lock().unwrap().parent.as_ref().unwrap(), node.lock().unwrap().parent.as_ref().unwrap()) && prev_node.lock().unwrap().keys.len() > self.min_capacity {
                        self.borrow_key_from_left_internal(my_position_in_parent, node.clone(), prev_node);
                    }  else if prev_node.lock().unwrap().keys.len() <= self.min_capacity {
                        self.merge_node_with_left_internal(my_position_in_parent, node.clone(), prev_node);
                    }
                }
            }
        }
        if let Some(ref parent) = node.lock().unwrap().parent {
            self.remove(key, Some(parent.clone()));
        }
    }

    pub fn print_tree(&mut self) {
        self.print(self.root.lock().unwrap().clone(), String::from(""), false);
    }

    pub fn print(&self, mut node : Node, mut prefix : String, last : bool ) {
        print!("{}├ [", prefix);
        for (i, key) in node.keys.iter().enumerate() {
            print!("{} ", key);
            if i != node.keys.len() - 1 {
                print!(", ");
            }
        }

        println!("]");
        prefix += if last { "   " } else { "|  " };

        if !node.is_leaf {
            for i in 0..node.children.len() {
                let _last = (i == node.children.len() - 1);
                self.print(node.children[i].lock().unwrap().clone(), prefix.clone(), _last);
            }
        }
    }
}