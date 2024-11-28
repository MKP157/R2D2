use bson::{Bson, Document};

mod bptree;

fn leaf_node_tests () {
    println!("LeafNode tests...");
    println!("=================");

    let max : usize = 3;

    let mut test_node = bptree::LeafNode::new(max);

    let mut test_docs : Vec<Document> = Vec::new();
    test_docs.push(bson::doc! {
        "name" : "Matthew",
        "age" : 21,
        "student" : true
    });

    test_docs.push(bson::doc! {
        "name" : "Dr. Kim",
        "age" : 40,
        "student" : false
    });

    test_docs.push(bson::doc! {
        "name" : "Aiden",
        "age" : 21,
        "student" : true
    });

    println!("\n\n======== INSERT TESTS ========");

    let mut status : i32;

    // Insert normal tests
    status = test_node.insert(3,  test_docs[0].clone());
    println!("Insert completed with status {}, result:", status);
    test_node.print();
    print!("\n\n");

    status = test_node.insert(1,  test_docs[1].clone());
    println!("Insert completed with status {}, result:", status);
    test_node.print();
    print!("\n\n");

    // Insert with collision
    status = test_node.insert(1,  test_docs[2].clone());
    println!("Insert completed with status {}, result:", status);
    test_node.print();
    print!("\n\n");

    status = test_node.insert(2,  test_docs[2].clone());
    println!("Insert completed with status {}, result:", status);
    test_node.print();
    print!("\n\n");

    // Insert on full:
    status = test_node.insert(4,  test_docs[2].clone());
    println!("Insert completed with status {}, result:", status);
    test_node.print();
    print!("\n\n");


    // Removal tests
    println!("\n\n======== REMOVE TESTS ========");
    let mut retrieved : Option<(u128, bson::Document)>;


    // Nonexistent key
    (status, retrieved) = test_node.remove(5);
    println!("Remove completed with status {}, result:", status);
    test_node.print();
    if retrieved.is_some() {
        let (k, d) = retrieved.unwrap();
        print!("\nReturned: {} : {}", k, d);
    }
    print!("\n\n");

    // Key exists
    (status, retrieved) = test_node.remove(2);
    test_node.print();
    if retrieved.is_some() {
        let (k, d) = retrieved.unwrap();
        print!("\nReturned: {} : {}", k, d);
    }
    print!("\n\n");

    // Less than half full
    (status, retrieved) = test_node.remove(1);
    test_node.print();
    if retrieved.is_some() {
        let (k, d) = retrieved.unwrap();
        print!("\nReturned: {} : {}", k, d);
    }
    print!("\n\n");

    (status, retrieved) = test_node.remove(3);
    test_node.print();
    if retrieved.is_some() {
        let (k, d) = retrieved.unwrap();
        print!("\nReturned: {} : {}", k, d);
    }
    print!("\n\n");
}


fn main() {
    leaf_node_tests();
}
