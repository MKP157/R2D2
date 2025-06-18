<img src="https://github.com/MKP157/R2D2/blob/main/R2D2_logo.png" height="150" alt="">

# Rust-based Relational Database Design (R2D2): A Bare-bones Time-Series Database written in Rust
For my Advanced Databases (CS 4525) class, we were tasked with implementing a time-series database 
with a B+ tree at the heart of it. Building off of my previous work building [Proxide](https://github.com/MKP157/Proxide)
for my Networking (CS 3893) class, I decided to implement it in Rust using some boilerplate
from Proxide.

In the end, it only really used the original TCP listener and HTML response scheme from Proxide.
It became a much bigger project, while still relying on the core principles of Proxide's design.

---

## How does it work?
For more information on the inner workings of R2D2, please
[read my final report as assembled for the class.](https://github.com/MKP157/R2D2/blob/main/r2d2/CS_4525_Final_Report-1.pdf) **The API reference may also be found below.**

---

## Running R2D2
The only prerequisite to using R2D2 is having Rust installed. 
If you don't have it yet, the Rust installer can be [found here](https://www.rust-lang.org/tools/install).

To run R2D2, navigate to the folder containing the project's `Cargo.toml` file 
with your system's terminal, and type `cargo run`. This will pull all the necessary crates,
compile the project, and run it directly afterward. If no changes to the code are made, 
then Cargo won't  recompile and simply run instead.

To run a build with compilation optimizations, run `cargo run --release`.

Once R2D2 is running, nothing will actually happen until you try and connect to it. 
From your web browser of choice, navigate to http://127.0.0.1:6969/LIST::ALL. You should see
an empty table - that's good! That means it's running, and responding to requests.
Using the API reference in the final report, try and explore how to use R2D2 from here.

Further, to run benchmarking tests, run the Python scripts included in the `src` folder.

---

## API Overview
- Access: HTTP GET requests to `<ip address>:6969/<operation>[::<category>]::<options>[::HIDE]`
- Response: HTML table (or status message if `::HIDE` is appended)
- Note: All endpoints are case-, whitespace-, and character-sensitive.

### 1. Data Retrieval: `LIST`
- Usage: `LIST(::<category>)(::<options>)[::HIDE]`
- Examples:
    - View all rows: `LIST::ALL`
    - Benchmark range query (no HTML): `LIST::RANGE::100,1000::HIDE`
- Categories:
    - `ALL` — List all rows.
    - `ONE::<timestamp>` — List row matching timestamp.
    - `RANGE::<A>,<B>` — List rows with keys in `[A, B]`.
    - `METADATA` — Database metadata.
    - `SAVED` — Lists all saved databases in the data folder.

### 2. Data Aggregation: `AGGREGATE`
- Usage: `AGGREGATE::<column name>(::<category>)`
- Examples:
    - Average: `AGGREGATE::number_sold::AVG`
    - Sum: `AGGREGATE::number_sold::SUM`
- Categories:
    - `SUM`
    - `AVG`
    - `MIN`
    - `MAX`

### 3. Data Insertion: `INSERT`
- Usage: `INSERT::<column>=<value>{,<column>=<value>}[::TIMESTAMP=<timestamp>][::HIDE]`
- Example: `INSERT::store="Walmart",product_id=101,available=false::TIMESTAMP=1733697225084::HIDE`
- Notes:
    - Any column can be omitted.
    - If timestamp not provided, current time is used.
    - On collision, timestamp is incremented until unique.

### 4. Data Deletion: `REMOVE`
- Usage: `REMOVE::ONE::TIMESTAMP=<timestamp>`
- Note: Only single-row deletion by timestamp is currently supported.

### 5. Data Serialization: `SAVE`
- Usage: `SAVE::<NAME=<filename>>[::CSV]`
- Notes:
    - JSON: filename required, .r2d2 extension auto-applied.
    - CSV: always named dump.csv, overwrites previous.
    - On success, lists all saved databases (LIST::SAVED).

### 6. Data De-serialization: `LOAD`
- Usage: `LOAD::<filename>`
- Note: Loads .r2d2 file, overwrites current database. This cannot be undone.

### 7. Current Timestamp: `TIME`
- Usage: `TIME::YYYY-MM-DD HH:MM:SS`
- Example: `TIME::2024-12-07 11:15:10`
    - Returns: `1733570110000` (milliseconds since epoch, UTC)

### General Notes
- All operations are accessed via HTTP GET requests on TCP port 6969.
- Appending ::HIDE to any operation returns only a status message (no HTML table), useful for benchmarking.
- The database schema and main setup are not exposed via API; changes require editing the main method in code.

---

## Copyright Disclaimer
The character R2D2 and their likeness do not belong to me. "R2-D2" is a registered trademark owned by Lucasfilm Ltd. The [art I have used](https://pixabay.com/vectors/ai-generated-robot-r2d2-character-8898448/)
is available to anyone for free use and permitted modification under the [Pixabay Content License.](https://pixabay.com/service/license-summary/)