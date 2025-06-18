use r2d2p2::database::{Database, DATA_PATH};
use bson::doc;
use std::fs;
use std::path::Path;

#[cfg(test)]
mod serialization_tests {
    use super::*;

    // Helper function to create a test database
    fn create_test_db() -> Database {
        Database::new(
            vec![
                String::from("store"),
                String::from("product"),
                String::from("number_sold")
            ],
            vec![
                String::from("number"),
                String::from("number"),
                String::from("number")
            ]
        )
    }

    // Helper to populate test database
    fn populate_test_db(db: &mut Database) {
        let base_time: u128 = 1733697225000;

        for i in 0..5 {
            let timestamp = base_time + i as u128;
            let doc = doc! {
                "store": i,
                "product": 100 + i,
                "number_sold": 5 * i
            };
            db.insert_to_database(timestamp, doc);
        }
    }

    #[test]
    fn test_save_and_load_json() {
        let mut original_db = create_test_db();
        populate_test_db(&mut original_db);

        // Generate a unique test filename
        let test_filename = format!("test_db_{}", chrono::Utc::now().timestamp());

        // Save the database
        original_db.save(test_filename.clone());

        // Check if file exists
        let file_path = format!("{}/{}.r2d2", DATA_PATH, test_filename);
        assert!(Path::new(&file_path).exists());

        // Create a new database and load the saved data
        let mut loaded_db = create_test_db();
        let load_result = loaded_db.load(format!("{}.r2d2", test_filename));
        assert!(load_result.is_ok());

        // Verify the loaded data with a query
        let result = loaded_db.query(String::from("LIST::ALL"));
        let rows = result.get_document("rows").unwrap();
        assert_eq!(rows.len(), 5);

        // Clean up test file
        if Path::new(&file_path).exists() {
            let _ = fs::remove_file(file_path);
        }
    }

    #[test]
    fn test_csv_export() {
        let mut db = create_test_db();
        populate_test_db(&mut db);

        // Export to CSV
        db.data_to_csv();

        // Check if CSV file exists
        let csv_path = format!("{}/dump.csv", DATA_PATH);
        assert!(Path::new(&csv_path).exists());

        // Read the CSV file to verify it has content
        let csv_content = fs::read_to_string(&csv_path).unwrap_or_default();
        assert!(!csv_content.is_empty());

        // Verify CSV has correct number of lines (header + 5 data rows)
        let line_count = csv_content.lines().count();
        assert_eq!(line_count, 6);

        // Clean up test file
        if Path::new(&csv_path).exists() {
            let _ = fs::remove_file(csv_path);
        }
    }
}
