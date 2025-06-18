use r2d2p2::database::Database;
use bson::doc;

#[cfg(test)]
mod database_tests {
    use super::*;

    // Helper function to create a test database
    fn create_test_db() -> Database {
        // Database::new(
        //     vec![
        //         String::from("store"),
        //         String::from("product"),
        //         String::from("number_sold")
        //     ],
        //     vec![
        //         String::from("number"),
        //         String::from("number"),
        //         String::from("number")
        //     ]
        // )
        Database::from_schema("./test.schema.r2d2");
    }

    #[test]
    fn test_database_creation() {
        let db = create_test_db();
        // If the database was created without panicking, this test passes
        assert!(true);
    }

    #[test]
    fn test_insert_and_get_one() {
        let mut db = create_test_db();

        // Create a timestamp and document to insert
        let timestamp: u128 = 1733697225084;
        let test_doc = doc! {
            "store": 1,
            "product": 101,
            "number_sold": 5
        };

        // Insert into database
        db.insert_to_database(timestamp, test_doc.clone());

        // Retrieve the document
        let retrieved = db.get_one(timestamp);
        assert!(retrieved.is_some());

        let retrieved_doc = retrieved.unwrap();
        assert_eq!(retrieved_doc.get_i32("store").unwrap(), 1);
        assert_eq!(retrieved_doc.get_i32("product").unwrap(), 101);
        assert_eq!(retrieved_doc.get_i32("number_sold").unwrap(), 5);
    }

    #[test]
    fn test_get_range() {
        let mut db = create_test_db();

        // Insert multiple documents with sequential timestamps
        let base_time: u128 = 1733697225000;
        
        for i in 0..10 {
            let timestamp = base_time + i as u128;
            let doc = doc! {
                "store": i,
                "product": 100 + i,
                "number_sold": 5 * i
            };
            
            db.insert_to_database(timestamp, doc);
        }

        // Test retrieving a range of documents
        let result = db.get_range(base_time + 2, base_time + 5);

        // Check if we got results
        assert_eq!(result.iter().count(), 4); // Should include timestamps 2, 3, 4, and 5
    }

    #[test]
    fn test_aggregate_operations() {
        let mut db = create_test_db();

        // Insert test data
        let base_time: u128 = 1733697225000;

        for i in 1..6 {
            let timestamp = base_time + i as u128;
            let doc = doc! {
                "store": 1,
                "product": 100,
                "number_sold": i
            };
            db.insert_to_database(timestamp, doc);
        }

        // Test aggregation functions
        let sum = db.aggregate(String::from("SUM"), String::from("number_sold"));
        assert_eq!(sum, 15.0); // 1+2+3+4+5 = 15

        let avg = db.aggregate(String::from("AVG"), String::from("number_sold"));
        assert_eq!(avg, 3.0); // (1+2+3+4+5)/5 = 3

        let min = db.aggregate(String::from("MIN"), String::from("number_sold"));
        assert_eq!(min, 1.0);

        let max = db.aggregate(String::from("MAX"), String::from("number_sold"));
        assert_eq!(max, 5.0);
    }

    #[test]
    fn test_query_parsing() {
        let mut db = create_test_db();

        // Insert a document
        let timestamp: u128 = 1733697225084;
        let test_doc = doc! {
            "store": 1,
            "product": 101,
            "number_sold": 5
        };
        db.insert_to_database(timestamp, test_doc);

        // Test LIST::ALL query
        let result = db.query(String::from("LIST::ALL"));
        assert!(result.contains_key("rows"));

        // Test LIST::ONE query
        let result = db.query(format!("LIST::ONE::{}", timestamp));
        assert!(result.contains_key("rows"));

        // Test AGGREGATE query
        let result = db.query(String::from("AGGREGATE::number_sold::AVG"));
        assert!(result.contains_key("rows"));
    }
}
