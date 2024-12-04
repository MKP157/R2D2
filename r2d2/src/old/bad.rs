use std::sync::{Arc, Mutex};
use std::ptr;
use std::cmp::Ordering;
use std::ops::{Deref, Index};

fn print_vec<T: ToString>(vec: Vec<T>) {
    print!("[");
    for item in vec {
        print!("{}, ", item.to_string());
    }
    println!("]");
}


#[derive(Clone)]
pub struct Node {
    parent: Option<Arc<Mutex<Node>>>,
    is_leaf: bool,
    prev: Option<Arc<Mutex<Node>>>,
    next: Option<Arc<Mutex<Node>>>,
    keys: Vec<i32>,
    children: Vec<Arc<Mutex<Node>>>,
    values: Vec<i32>,
}

impl Node {
    fn new(parent: Option<Arc<Mutex<Node>>>, is_leaf: bool, prev_: Option<Arc<Mutex<Node>>>, next_: Option<Arc<Mutex<Node>>>) -> Self {
        let mut node = Node {
            parent,
            is_leaf,
            prev: prev_.clone(),
            next: next_.clone(),
            keys: Vec::new(),
            children: Vec::new(),
            values: Vec::new(),
        };

        if let Some(next) = &next_ {
            let mut next_lock = next.lock().unwrap();
            next_lock.prev = Some(Arc::new(Mutex::new(node.clone())));
        }

        if let Some(prev) = &prev_ {
            let mut prev_lock = prev.lock().unwrap();
            prev_lock.next = Some(Arc::new(Mutex::new(node.clone())));
        }

        node
    }

    fn index_of_child(&self, key: i32) -> usize {
        println!(">>> DOING INDEX CHILD");
        self.keys.iter().position(|&x| x > key).unwrap_or(self.keys.len())
    }

    fn index_of_key(&self, key: i32) -> Option<usize> {
        println!(">>> DOING INDEX KEY");
        self.keys.iter().position(|&x| x == key)
    }

    fn get_child(&self, key: i32) -> Arc<Mutex<Node>> {
        println!(">>> DOING GET CHILD");
        self.children[self.index_of_child(key)].clone()
    }

    fn set_child(&mut self, key: i32, value: Vec<Arc<Mutex<Node>>>) {
        println!(">>> DOING SET CHILD");

        let i = self.index_of_child(key);
        self.keys.insert(i, key);
        self.children.remove(i);
        self.children.splice(i..i, value.into_iter());
    
        println!("=== RESULT SET CHILD");
        print_vec(self.keys.clone());
        for n in self.children.iter() {
            print_vec(n.lock().unwrap().keys.clone());
        }
    }


    fn split_internal(&mut self) -> (i32, Arc<Mutex<Node>>, Arc<Mutex<Node>>) {
        println!(">>> DOING INTERNAL SPLIT");

        let mut left = Node::new(self.parent.clone(), false, None, None);

        let mid =  self.keys.len() / 2;

        left.keys.extend_from_slice(&self.keys[..mid]);
        left.children.extend_from_slice(&self.children[..mid+1]);

        for child in &left.children {
            let mut child_lock = child.lock().unwrap();
            child_lock.parent = Some(Arc::new(Mutex::new(left.clone())));
        }

        let key : i32 = self.keys[mid];

        self.keys.drain(..=mid);
        self.children.drain(..=mid);

        (key, Arc::new(Mutex::new(left.clone())), Arc::new(Mutex::new(self.clone())))
    }

    fn get(&self, key: i32) -> Option<i32> {
        println!(">>> DOING NODE GET");

        let index = self.index_of_key(key);
        if index.is_none() {
            Some(self.values[index.unwrap()])
        } else {
            println!("key {} not found", key);
            None
        }
    }

    fn set(&mut self, key: i32, value: i32) {
        println!(">>> DOING NODE SET");

        let i = self.index_of_child(key);

        if self.index_of_key(key).is_none() {
            self.keys.insert(i, key);
            self.values.insert(i, value);
        }

        else {
            self.values[i - 1] = value;
        }

    }

    fn split_leaf(&mut self) -> (i32, Arc<Mutex<Node>>, Arc<Mutex<Node>>) {
        println!(">>> DOING SPLIT LEAF");

        let mid = self.keys.len() / 2;
        let mut left = Node::new(self.parent.clone(), true, self.prev.clone(), Some(Arc::new(Mutex::new(self.clone()))));

        left.keys.extend_from_slice(&self.keys[..mid]);
        left.values.extend_from_slice(&self.values[..mid]);

        self.keys.drain(..mid);
        self.values.drain(..mid);

        (self.keys[0], Arc::new(Mutex::new(left)), Arc::new(Mutex::new(self.clone())))
    }
}


pub struct BPlusTree {
    root: Arc<Mutex<Node>>,
    max_capacity: usize,
    min_capacity: usize,
    depth: usize,
}

impl BPlusTree {
    pub fn new(max_capacity: usize) -> Self {
        BPlusTree {
            root: Arc::new(Mutex::from(Node::new(None, true, None, None))),
            max_capacity: if max_capacity > 2 { max_capacity } else { 2 },
            min_capacity: max_capacity / 2,
            depth: 0,
        }
    }

    pub fn find_leaf(&mut self, key: i32) -> Arc<Mutex<Node>> {
        let mut node = self.root.clone();
        while !node.lock().unwrap().is_leaf {
            let child_node = node.lock().unwrap().get_child(key);
            node = Arc::clone(&child_node);
        }
        node
    }

    pub fn get(&mut self, key: i32) -> Option<i32> {
        let leaf_node = self.find_leaf(key);
        let result = leaf_node.lock().unwrap().get(key);
        result
    }

    pub fn set(&mut self, key: i32, value: i32) {
        let leaf_node = self.find_leaf(key);
        leaf_node.lock().unwrap().set(key, value);

        if leaf_node.lock().unwrap().keys.len() > self.max_capacity {

            print!("SPLITTING LEAF : ");
            print_vec(leaf_node.lock().unwrap().keys.clone());

            let split_result = leaf_node.clone().lock().unwrap().split_leaf();

            println!("RESULTS:");
            println!("KEY:{}", split_result.0);
            print_vec(split_result.1.lock().unwrap().keys.clone());
            print_vec(split_result.2.lock().unwrap().keys.clone());

            self.insert(split_result.0.clone(), split_result.1.clone(), split_result.2.clone());
        }
    }

    pub fn insert(&mut self, key: i32, left: Arc<Mutex<Node>>, right: Arc<Mutex<Node>>) {
        println!("!!INSERT CALLED!! ");
        println!("KEY : {}", key);
        print_vec(left.clone().lock().unwrap().keys.clone());
        print_vec(right.clone().lock().unwrap().keys.clone());

        let mut parent = right.clone().lock().unwrap().clone().parent;

        if parent.is_none() {
            /*let new_parent = Arc::new(Mutex::new(Node::new(None, false, None, None)));
            left.lock().unwrap().parent = Some(Arc::clone(&new_parent));
            right.lock().unwrap().parent = Some(Arc::clone(&new_parent));
            self.root = Arc::clone(&new_parent);

            self.depth += 1;

            self.root.lock().unwrap().keys = Vec::from([key]);
            self.root.lock().unwrap().children = Vec::from([left.clone(), right.clone()]);
            return;*/

            self.root = Arc::new(Mutex::new(Node::new(None, false, None, None)));
            left.lock().unwrap().parent = Option::from(self.root.clone());
            right.lock().unwrap().parent = Option::from(self.root.clone());

            self.depth += 1;
            self.root.lock().unwrap().keys = vec![key];
            self.root.lock().unwrap().children = vec![left, right];
            return;
        }


        print!("!!!! PARENT BEFORE SET CHILD:");
        print_vec(parent.clone().unwrap().lock().unwrap().keys.clone());
        for n in parent.clone().unwrap().lock().unwrap().children.iter() {
            print_vec(n.lock().unwrap().keys.clone());
        }

        parent.as_ref().unwrap().lock().unwrap().set_child(key, vec![left, right]);

        print!("!!!! PARENT AFTER SET CHILD:");
        print_vec(parent.clone().unwrap().lock().unwrap().keys.clone());
        for n in parent.clone().unwrap().lock().unwrap().children.iter() {
            print_vec(n.lock().unwrap().keys.clone());
        }



        if parent.as_ref().unwrap().lock().unwrap().keys.len() > self.max_capacity {
            let split_result = parent.clone().unwrap().lock().unwrap().split_internal();

            print!("SPLITTING LEAF (FROM INSERT): ");
            print_vec(parent.clone().unwrap().lock().unwrap().keys.clone());

            println!("RESULTS:");
            println!("KEY:{}", split_result.0);
            print_vec(split_result.1.lock().unwrap().keys.clone());
            print_vec(split_result.2.lock().unwrap().keys.clone());

            self.insert(split_result.0.clone(), split_result.1.clone(), split_result.2.clone());
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

    // TODO: remove_from_leaf
    pub fn remove_from_leaf(&self, key: i32, node: Arc<Mutex<Node>>) -> Result<(), String> {
        let mut node = node.lock().unwrap();

        let found_index = node.index_of_key(key);
        if found_index.is_none() {
            return Err(format!("Key {} not found! Exiting...", key));
        }

        let index = found_index.unwrap();
        node.keys.remove(index);
        node.values.remove(index);

        if let Some(parent) = &node.parent {
            let mut parent = parent.lock().unwrap();
            let index_in_parent = parent.index_of_child(key);
            if index_in_parent != parent.keys.len() && !node.keys.is_empty() {
                let index_in_parent = index_in_parent;
                parent.keys[index_in_parent] = node.keys[0];
            }
        }

        Ok(())
    }
    // TODO: remove_from_internal
    pub fn remove_from_internal(&self, key: i32, node: Arc<Mutex<Node>>) -> Result<(), String> {
        let mut node = node.lock().unwrap();

        let found_index = node.index_of_key(key);
        if found_index.is_some() {
            let index = found_index.unwrap();
            let mut left_most_leaf = node.children[index + 1].clone();

            while !left_most_leaf.lock().unwrap().is_leaf {
                let temp = left_most_leaf.lock().unwrap().children[0].clone();
                left_most_leaf = temp;
            }

            let left_most_key = Some(left_most_leaf.lock().unwrap().keys[0]);
            if let Some(key) = left_most_key {
                node.keys[index] = key;
            } else {
                return Err("Leftmost leaf has no keys!".to_string());
            }
        }

        Ok(())
    }
    // TODO: borrow_key_from_right_leaf
    pub fn borrow_key_from_right_leaf(&self, _node: Arc<Mutex<Node>>, _next: Arc<Mutex<Node>>) -> Result<(), String> {
        let mut node = _node.lock().unwrap();
        let mut next = _next.lock().unwrap();

        if next.keys.is_empty() || next.values.is_empty() {
            return Err("Next leaf has no keys or values to borrow.".to_string());
        }

        // Borrow key and value from the right leaf
        let borrowed_key = next.keys.remove(0);
        let borrowed_value = next.values.remove(0);

        node.keys.push(borrowed_key);
        node.values.push(borrowed_value);

        // Update the parent key
        if let Some(parent) = &node.parent {
            let mut parent_node = parent.lock().unwrap();
            for i in 0..parent_node.children.len() {
                if Arc::ptr_eq(&parent_node.children[i], &_next) {
                    if i > 0 {
                        parent_node.keys[i - 1] = node.keys.last().cloned().unwrap_or_default();
                    } else {
                        return Err("Invalid child index in parent.".to_string());
                    }
                    break;
                }
            }
        } else {
            return Err("Node has no parent.".to_string());
        }

        Ok(())
    }

    // TODO: borrow_key_from_left_leaf
    pub fn borrow_key_from_left_leaf(&self, _node: Arc<Mutex<Node>>, _prev: Arc<Mutex<Node>>) -> Result<(), String> {
        let mut node = _node.lock().unwrap();
        let mut prev = _prev.lock().unwrap();

        if prev.keys.is_empty() || prev.values.is_empty() {
            return Err("Previous node has no keys or values to borrow.".to_string());
        }

        // Borrow the last key and value from the previous node
        let borrowed_key = prev.keys.pop().unwrap();
        let borrowed_value = prev.values.pop().unwrap();

        // Insert the borrowed key and value at the beginning of the current node
        node.keys.insert(0, borrowed_key);
        node.values.insert(0, borrowed_value);

        // Update the parent node's key
        if let Some(parent) = &node.parent {
            let mut parent_node = parent.lock().unwrap();
            for i in 0..parent_node.children.len() {
                if Arc::ptr_eq(&parent_node.children[i], &_node) {
                    if i > 0 {
                        parent_node.keys[i - 1] = node.keys[0];
                    }
                    break;
                }
            }
        } else {
            return Err("Node does not have a parent.".to_string());
        }

        Ok(())
    }

    // TODO: merge_node_with_right_leaf
    pub fn merge_node_with_right_leaf(&self, _node: Arc<Mutex<Node>>, _next: Arc<Mutex<Node>>) -> Result<(), String> {
        let mut node = _node.lock().unwrap();
        let mut next = _next.lock().unwrap();

        // Merge keys and values from the right node into the current node
        node.keys.extend(next.keys.iter().cloned());
        node.values.extend(next.values.iter().cloned());

        // Update the next pointer of the current node
        node.next = next.next.clone();
        if let Some(ref mut next_node) = node.clone().next {
            let mut next_node_mut = next_node.lock().unwrap();
            next_node_mut.prev = Some(Arc::new(Mutex::new(node.clone())));
        }

        // Remove the right node from the parent's children and keys
        if let Some(parent) = &node.parent {
            let mut parent_node = parent.lock().unwrap();
            for i in 0..parent_node.children.len() {
                if Arc::ptr_eq(&parent_node.children[i], &_next) {
                    if i > 0 {
                        parent_node.keys.remove(i - 1);
                    }
                    parent_node.children.remove(i);
                    break;
                }
            }
        } else {
            return Err("Node does not have a parent.".to_string());
        }

        Ok(())
    }

    // TODO: merge_node_with_left_leaf
    pub fn merge_node_with_left_leaf(&self, _node: Arc<Mutex<Node>>, _prev: Arc<Mutex<Node>>) -> Result<(), String> {
        let mut node = _node.lock().unwrap();
        let mut prev = _prev.lock().unwrap();

        // Merge keys and values from the current node into the previous node
        prev.keys.extend(node.keys.iter().cloned());
        prev.values.extend(node.values.iter().cloned());

        // Update the next pointer of the previous node
        prev.next = node.next.clone();
        if let Some(ref mut prev_node) = prev.clone().next {
            let mut next_node_mut = prev_node.lock().unwrap();
            next_node_mut.prev = Some(Arc::new(Mutex::new(prev.clone())));
        }

        // Remove the current node from the parent's children and keys
        if let Some(parent) = &node.parent {
            let mut parent_node = parent.lock().unwrap();
            for i in 0..parent_node.children.len() {
                if Arc::ptr_eq(&parent_node.children[i], &_node) {
                    if i > 0 {
                        parent_node.keys.remove(i - 1);
                    }
                    parent_node.children.remove(i);
                    break;
                }
            }
        } else {
            return Err("Node does not have a parent.".to_string());
        }

        Ok(())
    }

    // TODO: borrow_key_from_right_interval
    pub fn borrow_key_from_right_internal(&self, my_position_in_parent: usize, node: Arc<Mutex<Node>>, next: Arc<Mutex<Node>>) -> Result<(), String> {
        let mut node = node.lock().unwrap();
        let mut next = next.lock().unwrap();

        if my_position_in_parent >= node.parent.as_ref().unwrap().lock().unwrap().keys.len() {
            return Err("Invalid position in parent.".to_string());
        }

        // Borrow the key from the parent and insert it at the end of the current node's keys
        let borrowed_key = node.parent.as_ref().unwrap().lock().unwrap().keys[my_position_in_parent];
        node.keys.push(borrowed_key);

        // Update the parent's key with the first key of the next node
        node.parent.as_ref().unwrap().lock().unwrap().keys[my_position_in_parent] = next.keys[0];

        // Remove the first key from the next node
        next.keys.remove(0);

        // Borrow the first child from the next node and insert it at the end of the current node's children
        let borrowed_child = Some(next.children.remove(0));
        node.children.push(borrowed_child.clone().unwrap());

        // Update the parent of the borrowed child to be the current node
        if let Some(child) = borrowed_child {
            child.lock().unwrap().parent = Some(Arc::new(Mutex::new(node.clone())));
        }

        Ok(())
    }

    // TODO: borrow_key_from_left_interval
    pub fn borrow_key_from_left_internal(&self, my_position_in_parent: usize, node: Arc<Mutex<Node>>, prev: Arc<Mutex<Node>>) -> Result<(), String> {
        let mut node = node.lock().unwrap();
        let mut prev = prev.lock().unwrap();

        if my_position_in_parent == 0 || my_position_in_parent > node.parent.as_ref().unwrap().lock().unwrap().keys.len() {
            return Err("Invalid position in parent.".to_string());
        }

        // Borrow the key from the parent and insert it at the beginning of the current node's keys
        let borrowed_key = node.parent.as_ref().unwrap().lock().unwrap().keys[my_position_in_parent - 1];
        node.keys.insert(0, borrowed_key);

        // Update the parent's key with the last key of the previous node
        node.parent.as_ref().unwrap().lock().unwrap().keys[my_position_in_parent - 1] = prev.keys.last().cloned().unwrap();

        // Remove the last key from the previous node
        prev.keys.pop();

        // Borrow the last child from the previous node and insert it at the beginning of the current node's children
        let borrowed_child = prev.children.pop();
        if let Some(child) = borrowed_child {
            node.children.insert(0, child.clone());

            // Update the parent of the borrowed child to be the current node
            child.lock().unwrap().parent = Some(Arc::new(Mutex::new(node.clone())));
        }

        Ok(())
    }

    // TODO: merge_node_with_right_interval
    pub fn merge_node_with_right_internal(&self, my_position_in_parent: usize, node: Arc<Mutex<Node>>, next: Arc<Mutex<Node>>) -> Result<(), String> {
        let mut node = node.lock().unwrap();
        let mut next = next.lock().unwrap();

        if my_position_in_parent >= node.parent.as_ref().unwrap().lock().unwrap().keys.len() {
            return Err("Invalid position in parent.".to_string());
        }

        // Borrow the key from the parent and insert it at the end of the current node's keys
        let borrowed_key = node.parent.as_ref().unwrap().lock().unwrap().keys[my_position_in_parent];
        node.keys.push(borrowed_key);

        // Remove the key from the parent
        node.parent.as_ref().unwrap().lock().unwrap().keys.remove(my_position_in_parent);

        // Remove the next node from the parent's children
        node.parent.as_ref().unwrap().lock().unwrap().children.remove(my_position_in_parent + 1);

        // Insert all keys from the next node into the current node
        node.keys.extend(next.keys.drain(..));

        // Insert all children from the next node into the current node
        node.children.extend(next.children.drain(..));

        // Update the parent of each child to be the current node
        for child in &node.children {
            child.lock().unwrap().parent = Some(Arc::new(Mutex::new(node.clone())));
        }

        Ok(())
    }

    // TODO: merge_node_with_left_interval
    pub fn merge_node_with_left_internal(&self, my_position_in_parent: usize, node: Arc<Mutex<Node>>, prev: Arc<Mutex<Node>>) -> Result<(), String> {
        let mut node = node.lock().unwrap();
        let mut prev = prev.lock().unwrap();

        if my_position_in_parent == 0 || my_position_in_parent > node.parent.as_ref().unwrap().lock().unwrap().keys.len() {
            return Err("Invalid position in parent.".to_string());
        }

        // Borrow the key from the parent and insert it at the end of the previous node's keys
        let borrowed_key = node.parent.as_ref().unwrap().lock().unwrap().keys[my_position_in_parent - 1];
        prev.keys.push(borrowed_key);

        // Remove the key from the parent
        node.parent.as_ref().unwrap().lock().unwrap().keys.remove(my_position_in_parent - 1);

        // Remove the current node from the parent's children
        node.parent.as_ref().unwrap().lock().unwrap().children.remove(my_position_in_parent);

        // Insert all keys from the current node into the previous node
        prev.keys.extend(node.keys.drain(..));

        // Insert all children from the current node into the previous node
        prev.children.extend(node.children.drain(..));

        // Update the parent of each child to be the previous node
        for child in &prev.children {
            child.lock().unwrap().parent = Some(Arc::new(Mutex::new(prev.clone())));
        }

        Ok(())
    }

    // TODO: remove
    pub fn remove(&mut self, key: i32, node: Option<Arc<Mutex<Node>>>) {
        let mut node = match node {
            Some(n) => n.clone(),
            None => self.find_leaf(key).clone(),
        };

        let mut node_ref = node.lock().unwrap();

        if node_ref.is_leaf {
            self.remove_from_leaf(key, node.clone());
        } else {
            self.remove_from_internal(key, node.clone());
        }

        if node_ref.keys.len() < self.min_capacity {
            if Arc::ptr_eq(&node, &self.root) {
                let mut root_ref = &self.root.lock().unwrap().clone();

                if root_ref.keys.is_empty() && !root_ref.children.is_empty() {
                    self.root = root_ref.children[0].clone();
                    let mut new_root = self.root.lock().unwrap();
                    new_root.parent = None;
                    self.depth -= 1;
                }
                return;
            }

            else if node_ref.is_leaf {
                let next = node_ref.next.clone();
                let prev = node_ref.prev.clone();

                if let Some(next_node) = &next {
                    let next_ref = next_node.lock().unwrap();

                    if Arc::ptr_eq(&next_ref.parent.clone().unwrap(), &node_ref.parent.clone().unwrap()) && next_ref.keys.len() > self.min_capacity {
                        self.borrow_key_from_right_leaf(
                            Arc::from(node.clone()),
                            Arc::from(next_node.clone())
                        ).expect("TODO: panic message");
                    }

                    else if let Some(prev_node) = &prev {
                        let prev_ref = prev_node.lock().unwrap();

                        if Arc::ptr_eq(&prev_ref.parent.clone().unwrap(), &node_ref.parent.clone().unwrap()) && prev_ref.keys.len() > self.min_capacity {
                            self.borrow_key_from_left_leaf(
                                Arc::from(node.clone()),
                                Arc::from(prev_node.clone())
                            ).expect("TODO: panic message");
                        }

                        else if next_ref.keys.len() <= self.min_capacity {
                            self.merge_node_with_right_leaf(
                                Arc::from(node.clone()),
                                Arc::from(next_node.clone())
                            ).expect("TODO: panic message");
                        }
                    } else if let Some(prev_node) = &prev {
                        let prev_ref = prev_node.lock().unwrap();

                        if Arc::ptr_eq(&prev_ref.parent.clone().unwrap(), &node_ref.parent.clone().unwrap()) && prev_ref.keys.len() <= self.min_capacity {
                            self.merge_node_with_right_leaf(
                                Arc::from(node.clone()),
                                Arc::from(prev_node.clone())
                            ).expect("TODO: panic message");
                        }
                    }
                }
            } else {
                let mut my_position_in_parent = -1;

                for (i, child) in node_ref.parent.as_ref().unwrap().lock().unwrap().children.iter().enumerate() {
                    if Arc::ptr_eq(child, &node) {
                        my_position_in_parent = i as i32;
                        break;
                    }
                }

                let next;
                let prev;

                if (my_position_in_parent + 1) < node_ref.parent.as_ref().unwrap().lock().unwrap().children.len() as i32 {
                    next = Some(node_ref.parent.as_ref().unwrap().lock().unwrap().children[(my_position_in_parent + 1) as usize].clone());
                } else {
                    next = None;
                }

                if my_position_in_parent > 0 {
                    prev = Some(node_ref.parent.as_ref().unwrap().lock().unwrap().children[(my_position_in_parent - 1) as usize].clone());
                } else {
                    prev = None;
                }

                if let Some(next_node) = &next {
                    let next_ref = next_node.lock().unwrap();

                    if Arc::ptr_eq(&next_ref.parent.clone().unwrap(), &node_ref.parent.clone().unwrap()) && next_ref.keys.len() > self.min_capacity {
                        self.borrow_key_from_right_internal(
                            my_position_in_parent as usize,
                            Arc::from(node.clone()),
                            Arc::from(next_node.clone())
                        ).expect("TODO: panic message");
                    }

                    else if let Some(prev_node) = &prev {
                        let prev_ref = prev_node.lock().unwrap();

                        if Arc::ptr_eq(&prev_ref.parent.clone().unwrap(), &node_ref.parent.clone().unwrap()) && prev_ref.keys.len() > self.min_capacity {
                            self.borrow_key_from_left_internal(
                                my_position_in_parent as usize,
                                Arc::from(node.clone()),
                                Arc::from(prev_node.clone())
                            ).expect("TODO: panic message");
                        }

                        else if next_ref.keys.len() <= self.min_capacity {
                            self.borrow_key_from_right_internal(
                                my_position_in_parent as usize,
                                Arc::from(node.clone()),
                                Arc::from(next_node.clone())
                            ).expect("TODO: panic message");
                        }
                    }

                    else if let Some(prev_node) = &prev {
                        let prev_ref = prev_node.lock().unwrap();
                        if Arc::ptr_eq(&prev_ref.parent.clone().unwrap(), &node_ref.parent.clone().unwrap()) && prev_ref.keys.len() > self.min_capacity {
                            self.borrow_key_from_left_internal(
                                my_position_in_parent as usize,
                                Arc::from(node.clone()),
                                Arc::from(prev_node.clone())
                            ).expect("TODO: panic message");
                        }
                    }
                }
            }
        }

        if let Some(parent) = &node_ref.parent {
            self.remove(key, Some(parent.clone()));
        }
    }
}