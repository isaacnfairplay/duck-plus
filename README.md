DuckDB Plus
DuckDB Plus is an upcoming Python wrapper designed to enhance the DuckDB in-memory analytical database. DuckDB is powerful but has limitations due to its C-based bindings, inconsistent typing, and verbose file-based data interactions. DuckDB Plus aims to provide a Pythonic, user-friendly API with improved typing, streamlined file handling, and enhanced methods—without relying on inheritance.
Note: This project is under active development and not yet available on PyPI. Stay tuned for updates, or contribute to shape its future!
Planned Features

Pythonic Wrapper: A clean interface that sidesteps DuckDB’s C-binding limitations, offering intuitive methods for querying and data management.
Robust Typing: Strong type hints for queries, results, and configurations to improve IDE support and code safety.
Enhanced File Handling: Simplified methods for working with file-based data sources (e.g., CSV, Parquet, JSON) with better error handling and format options.
Performance Focus: Lightweight design that preserves DuckDB’s speed while adding usability.
Extensible API: Easy-to-use methods for common tasks like bulk inserts, query optimization, and result formatting.

Installation
DuckDB Plus is not yet released but will be installable via pip once ready:
pip install duck-plus

It will support Python 3.8+ and DuckDB 0.8.0 or higher. For now, clone the repository to explore the code or contribute:
git clone https://github.com/yourusername/duck-plus
cd duck-plus
pip install -e .

Example Usage (Planned)
Here’s how DuckDB Plus will work once implemented (subject to change):

from duckdb_plus import DuckDBPlus

# Initialize an in-memory database
db = DuckDBPlus()

# Run a type-safe, formatted query
results = db.smart_query("SELECT * FROM range(10)")
print(results)  # Expected: [{"range": 0}, {"range": 1}, ...]

# Load a CSV file with intuitive options
db.load_file("data.csv", table_name="my_table", format="csv", options={"delimiter": ",", "header": True})

# Perform a bulk insert
data = [{"id": i, "name": f"Item {i}"} for i in range(5)]
db.bulk_insert("my_table", data)

Check the examples/ directory for more sample scripts as they’re added.
Why DuckDB Plus?
DuckDB is an amazing analytical database, but its C-based bindings prevent class inheritance, its typing can be spotty, and file-based operations (e.g., loading CSV or Parquet files) often feel clunky. DuckDB Plus is being built to address these by:

Providing a standalone wrapper class with enhanced methods, avoiding inheritance entirely.
Adding comprehensive type hints for better developer experience.
Streamlining file-based data interactions with user-friendly APIs and robust error handling.

Project Status
DuckDB Plus is in the early stages of development. The folder structure is set up, and the core wrapper design is being planned. Key tasks include:

Implementing the DuckDBPlus class with enhanced query and file-handling methods.
Adding type hints and documentation.
Writing unit tests with pytest.
Preparing for PyPI release.

See the issues page for current tasks and progress.
Contributing
We’re looking for contributors to help build DuckDB Plus! To get involved:

Fork the repository.
Install development dependencies: pip install -e .[test].
Run tests (once implemented): pytest tests/.
Submit a pull request with your changes.

Please follow the CONTRIBUTING.md guidelines (coming soon).
License
This project will be released under the MIT License. See LICENSE for details.
Contact
Have ideas or want to collaborate? Open an issue on GitHub or email isaacmooreuky@protonmail.com.
