use std::io::Read;
use std::thread;
use std::time::Duration;

#[cfg(test)]
mod api_tests {
    use std::error::Error;
    use std::fs::File;
    use std::io::{BufRead, BufReader, Write};
    use std::net::{SocketAddr, TcpStream};
    use super::*;

    // Helper function to make HTTP GET requests
    fn make_request(endpoint: &str) -> Result<String, Box<dyn Error>> {
        let url = format!("http://127.0.0.1:6969/{}", endpoint);
        let addr: SocketAddr = "127.0.0.1:6969".parse()?;
        let mut response = String::new();

        // Connect with a 5-second timeout
        let stream = match TcpStream::connect_timeout(&addr, Duration::from_secs(5)) {
            Ok(stream) => stream,
            Err(e) => return Err(Box::new(e)),
        };
        let mut stream = stream;

        // Set a read timeout (optional, but recommended for incomplete reads)
        stream.set_read_timeout(Some(Duration::from_secs(1))).ok();

        let request = format!("GET /{} HTTP/1.1\r\nHost: 127.0.0.1:6969\r\nConnection: close\r\n\r\n", endpoint);
        stream.write_all(request.as_bytes())?;

        match stream.read_to_string(&mut response) {
            Ok(_) => {
                if let Some(body_start) = response.find("\r\n\r\n") {
                    Ok(response[body_start + 4..].to_string())
                } else {
                    Ok(response)
                }
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::TimedOut {
                    Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Request timed out after 1 second",
                    )))
                } else {
                    Err(Box::new(e))
                }
            }
        }
    }

    // Helper function to wait for server startup
    fn clear_and_wait_for_server() {
        for _ in 0..5 {
            if make_request("TIME::2024-01-01%2000:00:00").is_ok() {
                let response = make_request("REMOVE::ALL");
                assert!(response.is_ok());

                return;
            }
            thread::sleep(Duration::from_millis(1000));
        }
        panic!("Server did not start within expected time");

    }


    fn test_api_server_connection() {
        clear_and_wait_for_server();

        // Test basic connection with LIST::ALL
        let response = make_request("LIST::ALL::HIDE");
        assert!(response.is_ok());

        let content = response.unwrap();
        assert!(content.contains("Success") || content.contains("rows") || content.len() > 0);
    }

    fn test_api_insert_and_get_one() {
        clear_and_wait_for_server();

        // Use a specific timestamp for testing
        let timestamp = 1733697225084u128;

        // Insert a document via API
        let insert_endpoint = format!("INSERT::store=1,product=101,number_sold=5::TIMESTAMP={}::HIDE", timestamp);
        let response = make_request(&insert_endpoint);
        assert!(response.is_ok());

        // Retrieve the document via API
        let get_endpoint = format!("LIST::ONE::{}", timestamp);
        let response = make_request(&get_endpoint);
        assert!(response.is_ok());

        let content = response.unwrap();
        // Check that the response contains our inserted data
        assert!(content.contains("1") && content.contains("101") && content.contains("5"));
    }

    fn test_api_get_range() {
        clear_and_wait_for_server();

        let base_time = 1733697226000u128;

        // Insert multiple documents with sequential timestamps
        for i in 0..10 {
            let timestamp = base_time + i as u128;
            let insert_endpoint = format!(
                "INSERT::store={},product={},number_sold={}::TIMESTAMP={}::HIDE",
                i, 100 + i, 5 * i, timestamp
            );
            let response = make_request(&insert_endpoint);
            assert!(response.is_ok());
        }

        // Test retrieving a range of documents
        let range_endpoint = format!("LIST::RANGE::{},{}", base_time + 2, base_time + 5);
        let response = make_request(&range_endpoint);
        assert!(response.is_ok());

        let content = response.unwrap();
        // The response should contain data from the range
        // Check for presence of some expected values
        assert!(content.len() > 0);
    }

    fn test_api_aggregate_operations() {
        clear_and_wait_for_server();

        let base_time = 0u128;

        // Insert test data
        for i in 1..6 {
            let timestamp = base_time + i as u128;
            let insert_endpoint = format!(
                "INSERT::store=1,product=100,number_sold={}::TIMESTAMP={}::HIDE",
                i, timestamp
            );
            let response = make_request(&insert_endpoint);
            assert!(response.is_ok());
        }

        // Test SUM aggregation
        let sum_response = make_request("AGGREGATE::number_sold::SUM");
        assert!(sum_response.is_ok());
        let sum_content = sum_response.unwrap();
        assert!(sum_content.contains("15") || sum_content.contains("15.0"));

        // Test AVG aggregation
        let avg_response = make_request("AGGREGATE::number_sold::AVG");
        assert!(avg_response.is_ok());
        let avg_content = avg_response.unwrap();
        assert!(avg_content.contains("3") || avg_content.contains("3.0"));

        // Test MIN aggregation
        let min_response = make_request("AGGREGATE::number_sold::MIN");
        assert!(min_response.is_ok());
        let min_content = min_response.unwrap();
        assert!(min_content.contains("1") || min_content.contains("1.0"));

        // Test MAX aggregation
        let max_response = make_request("AGGREGATE::number_sold::MAX");
        assert!(max_response.is_ok());
        let max_content = max_response.unwrap();
        assert!(max_content.contains("5") || max_content.contains("5.0"));
    }

    fn test_api_query_parsing() {
        clear_and_wait_for_server();

        // Insert a document
        let timestamp = 1733697228084u128;
        let insert_endpoint = format!(
            "INSERT::store=1,product=101,number_sold=5::TIMESTAMP={}::HIDE",
            timestamp
        );
        let response = make_request(&insert_endpoint);
        assert!(response.is_ok());

        // Test LIST::ALL query
        let all_response = make_request("LIST::ALL");
        assert!(all_response.is_ok());
        let all_content = all_response.unwrap();
        assert!(all_content.len() > 0);

        // Test LIST::ONE query
        let one_endpoint = format!("LIST::ONE::{}", timestamp);
        let one_response = make_request(&one_endpoint);
        assert!(one_response.is_ok());
        let one_content = one_response.unwrap();
        assert!(one_content.contains("1") && one_content.contains("101") && one_content.contains("5"));

        // Test AGGREGATE query
        let agg_response = make_request("AGGREGATE::number_sold::AVG");
        assert!(agg_response.is_ok());
        let agg_content = agg_response.unwrap();
        assert!(agg_content.len() > 0);
    }

    fn test_api_time_endpoint() {
        clear_and_wait_for_server();

        // Test TIME endpoint
        let time_response = make_request("TIME::2024-12-07%2011:15:10");
        assert!(time_response.is_ok());

        let time_content = time_response.unwrap();
        // Should return the timestamp as a number
        assert!(time_content.contains("1733570110000") || time_content.chars().any(|c| c.is_numeric()));
    }

    fn test_api_metadata() {
        clear_and_wait_for_server();

        // Test LIST::METADATA
        let metadata_response = make_request("LIST::METADATA");
        assert!(metadata_response.is_ok());

        let metadata_content = metadata_response.unwrap();
        assert!(metadata_content.len() > 0);
    }

    fn test_api_remove_operation() {
        clear_and_wait_for_server();

        // Insert a document to remove
        let timestamp = 1733697229000u128;
        let insert_endpoint = format!(
            "INSERT::store=999,product=999,number_sold=999::TIMESTAMP={}::HIDE",
            timestamp
        );
        let response = make_request(&insert_endpoint);
        assert!(response.is_ok());

        // Verify it was inserted
        let get_endpoint = format!("LIST::ONE::{}", timestamp);
        let response = make_request(&get_endpoint);
        assert!(response.is_ok());
        let content = response.unwrap();
        assert!(content.contains("999"));

        // Remove the document
        let remove_endpoint = format!("REMOVE::ONE::TIMESTAMP={}", timestamp);
        let response = make_request(&remove_endpoint);
        assert!(response.is_ok());

        // Verify it was removed (should return empty or error)
        let get_response = make_request(&get_endpoint);
        if get_response.is_ok() {
            let content = get_response.unwrap();
            // Should either be empty or not contain our data
            assert!(!content.contains("999") || content.trim().is_empty());
        }
    }

    fn test_api_load_schema() {
        clear_and_wait_for_server();

        let mut response = make_request("LOAD::SCHEMA::test.schema.r2d2");
        assert!(response.is_ok());

        let mut content = response.unwrap();
        assert!(content.to_ascii_lowercase().contains("successfully loaded schema"));

        response = make_request("LIST::ALL");
        assert!(response.is_ok());

        content = response.unwrap();

        let schema_file = File::open("data/test.schema.r2d2").unwrap();
        let schema_reader = BufReader::new(schema_file);
        for line in schema_reader.lines() {
            let line = line.unwrap();
            let column_name = line.split(',').next().unwrap();
            assert!(content.contains(&column_name));
        }
    }

    fn test_api_load_schema_edge_cases() {
        clear_and_wait_for_server();


        // Incorrect formatting
        let mut response = make_request("LOAD::SCHEMA::test_bad.schema.r2d2");
        assert!(response.is_ok());

        let mut content = response.unwrap();
        assert!(content.to_ascii_lowercase().contains("error"));


        // Empty schema & extra period in file extension
        response = make_request("LOAD::SCHEMA::test.empty.schema.r2d2");
        assert!(response.is_ok());

        content = response.unwrap();
        assert!(content.to_ascii_lowercase().contains("error"));


        // Column name and type collisions
        response = make_request("LOAD::SCHEMA::test_collisions.schema.r2d2");
        assert!(response.is_ok());

        content = response.unwrap();
        assert!(content.to_ascii_lowercase().contains("error"));


        // Massive label lengths
        response = make_request("LOAD::SCHEMA::test_label_length.schema.r2d2");
        assert!(response.is_ok());

        content = response.unwrap();
        assert!(content.to_ascii_lowercase().contains("successfully loaded schema"));

        response = make_request("LIST::ALL");
        assert!(response.is_ok());

        content = response.unwrap();

        let label_length_schema_file = File::open("data/test_label_length.schema.r2d2").unwrap();
        let label_length_schema_reader = BufReader::new(label_length_schema_file);
        for line in label_length_schema_reader.lines() {
            let line = line.unwrap();
            let column_name = line.split(',').next().unwrap();
            assert!(!content.contains(&column_name));
        }


        // Large number of columns
        response = make_request("LOAD::SCHEMA::test_size.schema.r2d2");
        assert!(response.is_ok());

        content = response.unwrap();
        assert!(content.to_ascii_lowercase().contains("error"));
    }

    // API tests have to be run sequentially because they expect a certain ordering
    // of execution. That is why they are all called one-by-one within the following test
    // (Rust tests are run in parallel by default). This is kind of unavoidable, unfortunately,
    // due to the nature of how my database is designed.
    #[test]
    fn api_tests_main() {

        // P2P unit tests
        test_api_server_connection();
        println!("✓ Passed connection testing (\"test_api_server_connection\")");

        test_api_insert_and_get_one();
        println!("✓ Passed single insert with retrieval (\"test_api_insert_and_get_one\")");

        test_api_get_range();
        println!("✓ Passed retrieval over range (\"test_api_get_range\")");

        test_api_aggregate_operations();
        println!("✓ Passed aggregate operations testing (\"test_api_aggregate_operations\")");

        test_api_query_parsing();
        println!("✓ Passed complex query parsing testing (\"test_api_query_parsing\")");

        test_api_time_endpoint();
        println!("✓ Passed timestamp endpoint testing (\"test_api_time_endpoint\")");

        test_api_metadata();
        println!("✓ Passed metadata endpoint testing (\"test_api_get_range\")");

        test_api_remove_operation();
        println!("✓ Passed data removal testing (\"test_api_remove_operation\")");


        // New unit tests
        test_api_load_schema();
        println!("✓ Passed schema loading testing (\"test_api_server_connection\")");

        test_api_load_schema_edge_cases();
        println!("✓ Passed schema loading edge-case testing (\"test_api_server_connection\")");
    }
}
