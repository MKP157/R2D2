use bson;

pub struct LeafNode {
    keys: Vec<u128>,
    values: Vec<bson::Document>,
    // next_leaf may be null, need option
    next_leaf: Option<Box<LeafNode>>
}

impl LeafNode {
    pub fn new(num_keys: usize) -> LeafNode {
        LeafNode {
            keys: vec![0; num_keys],
            values: vec![bson::doc!{}; num_keys],
            next_leaf: None
        }
    }

    pub fn print(self: &mut LeafNode) {
        print!("[");
        for i in 0..self.keys.len() {
            print!("{} ", self.keys[i]);
        }
        print!("]");
    }

    // TODO: find
    // TODO: insert
    // TODO: delete
    // TODO: split_and_push
    // TODO: combine_and_shrink
}



pub struct BPTree {
    root: LeafNode,
    num_keys: usize,
}

impl BPTree {
    // TODO: new
    // TODO: insert
    // TODO: delete
    // TODO: find
    // TODO: print
    

}