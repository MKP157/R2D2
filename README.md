<img src="https://github.com/MKP157/R2D2/blob/main/R2D2_logo.png" height="150" alt="">

# R2D2: A Bare-bones Time-Series Database written in Rust
For my Advanced Databases (CS 4525) class, we were tasked with implementing a time-series database 
with a B+ tree at the heart of it. Building off of my previous work building [Proxide](https://github.com/MKP157/Proxide)
for my Networking (CS 3893) class, I decided to implement it in Rust using some boilerplate
from Proxide.

In the end, it only really used the original TCP listener and HTML response scheme from Proxide.
It became a much bigger project, while still relying on the core principles of Proxide's design.

## How does it work?
For more information on the inner workings of R2D2, **including the API reference**, please
[read my final report as assembled for the class.](https://github.com/MKP157/R2D2/blob/main/r2d2/CS_4525_Final_Report-1.pdf)

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