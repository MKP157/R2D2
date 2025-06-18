# Testing R2D2 Time-Series Database

This document outlines the testing structure and approach for the R2D2 time-series database.

## Test Organization

The tests are organized into several files:

1. **database_tests.rs** - Core unit tests for the Database struct functionality
2. **serialization_tests.rs** - Tests for saving and loading database data

## Running Tests

To run all tests, navigate to the project root directory and use:

```bash
cargo test
```

To run a specific test file:

```bash
cargo test --test database_tests
```

```bash
cargo test --test serialization_tests
```

** Note: API tests MUST be run sequentially due to how they're structured!**

```bash
cargo test --test api_tests -- --test-threads=1
```

To run a specific test function:

```bash
cargo test test_insert_and_get_one
```

## Test Coverage

The test suite covers:

- Database creation and basic operations
- Data insertion and retrieval
- Range queries
- Aggregation operations (SUM, AVG, MIN, MAX)
- Query parsing and execution
- Data serialization (JSON and CSV)
- Complex workflows integrating multiple operations

## Test Data

Test data consists of synthetic time-series entries representing store sales data with fields such as:
- store ID
- product ID
- number sold
- price
- stock status

## Adding New Tests

When adding new functionality to the database, follow these guidelines for testing:

1. Add unit tests for individual components
2. Add integration tests for complex workflows
3. Ensure tests clean up after themselves (delete test files)
4. Use the helper functions in the test modules to create and populate test databases

## Mock Data Generation

If you need to generate larger test datasets, consider using the included Python scripts or creating a test helper that can generate large volumes of test data.
