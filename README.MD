***

# Mirieri DB (MDB) – Custom Relational Database Management System

**Mirieri DB (MDB)** is a lightweight, custom-built **Relational Database Management System** developed in **Python**. It was created as part of the **Pesapal Junior Developer Challenge 2026**, with a focus on database architecture, query parsing, and full-stack integration.

MDB features a custom storage engine, an interactive SQL-like command shell, and a web demo built with **Flask** to demonstrate real-world database integration.

***

### 🚀 Key Features

- **Custom Storage Engine** – Implements persistent storage using JSON-based table structures for lightweight durability.  
- **O(1) Indexing** – Uses hash maps for instantaneous primary key lookups, regardless of table size.  
- **SQL-like Query Parser** – Supports core operations such as `CREATE`, `INSERT`, `SELECT`, and `DELETE`.  
- **Interactive REPL** – A command-line interface for direct interaction with the database through SQL-like commands.  
- **Flask Web Interface** – A modern, glassmorphic web interface showcasing MDB’s real-time integration capabilities.  

***

### 🧩 Project Structure

```text
├── data/               # Persistent storage (JSON files)
├── src/
│   ├── db.py           # Core database engine (CRUD + indexing)
│   └── repl.py         # Command-line shell (query parsing logic)
├── static/             # CSS and frontend assets
├── templates/          # HTML templates for the web interface
├── app.py              # Web application entry point
└── requirements.txt    # Project dependencies
```

***

### ⚙️ Getting Started

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/mdb.git
   cd mdb
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the web demo**
   ```bash
   python app.py
   ```

4. **Or launch the interactive shell**
   ```bash
   python src/repl.py
   ```

***

### 💡 Technical Highlights

- Built with a **modular architecture** for extensibility and clarity.  
- Components like **storage**, **indexing**, and **querying** are abstracted to reflect true RDBMS principles.  
- Demonstrates core database internals — from parsing to storage — at a manageable, educational scale.  

***

### ⚙️ Architectural Decisions

**Storage & Indexing**  
JSON was chosen for storage to keep data human-readable during debugging, though a binary format would be more efficient at scale.  
Indexing is handled with a **Hash Map** (Python dictionary), mapping primary keys directly to memory addresses for O(1) lookups.  
This design sacrifices range query performance (e.g., `> 100`), which could be improved with **B-Tree indexing** in the future.

**Separation of Concerns**  
The architecture follows clear boundaries:
- `db.py`: Core logic (CRUD, indexing). Independent from any I/O interface.  
- `repl.py` & `app.py`: User interfaces (CLI and web), both interact with the database only through defined methods, mimicking a driver connection.

***

### 🧭 Future Improvements

In future iterations, I plan to implement:

- **B-Tree Indexing** – To efficiently handle range queries and sorting.  
- **Foreign Key Support** – To enforce referential integrity between tables.  
- **ACID Compliance** – Through write-ahead logging (WAL) for safer transactions and crash recovery.  

***

### 👨‍💻 Author

**Michael Mirieri**  
Final Year Computer Science Student (awaiting graduation)  
**Strathmore University**  
Submission for **Pesapal Junior Developer Challenge 2026**

***

