import json
import os

class Table:
    """
    Represents a single table in our database.
    Data is stored in memory as a list of dictionaries and saved to a JSON file.
    """
    def __init__(self, name, schema, pk_col):
        self.name = name
        self.schema = schema        # e.g., {"id": "int", "name": "str"}
        self.pk_col = pk_col        # The column used as the Primary Key
        self.rows = []              # The actual data: [{'id': 1, ...}, ...]
        self.index = {}             # HashMap for O(1) lookups: { '1': row_index }
        self.filepath = f"data/{name}.json"
        
        # Load data if the file already exists
        self.load()

    def load(self):
        """Loads the table data from the JSON file if it exists."""
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'r') as f:
                    data = json.load(f)
                    self.rows = data.get("rows", [])
                    # We trust the file, but we must rebuild the memory index
                    self.rebuild_index()
            except json.JSONDecodeError:
                print(f"Error: Could not decode {self.filepath}. Starting empty.")

    def save(self):
        """Persists the current state to disk."""
        # Ensure the data directory exists
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        
        with open(self.filepath, 'w') as f:
            json.dump({
                "schema": self.schema,
                "pk_col": self.pk_col, # Save metadata too!
                "rows": self.rows
            }, f, indent=4)

    def rebuild_index(self):
        """Rebuilds the O(1) index mapping PK -> Row Index."""
        self.index = {}
        for idx, row in enumerate(self.rows):
            key = str(row.get(self.pk_col)) # Always stringify keys for consistency
            self.index[key] = idx

    def validate_schema(self, row_dict):
        """Ensures the inserted row matches the table schema."""
        # 1. Check for extra fields
        for col in row_dict:
            if col not in self.schema:
                raise ValueError(f"Column '{col}' not in schema {list(self.schema.keys())}")
        
        # 2. Check for missing fields
        for col in self.schema:
            if col not in row_dict:
                raise ValueError(f"Missing column '{col}'")

    def insert(self, row_dict):
        """Inserts a new row with O(1) duplicate checking."""
        self.validate_schema(row_dict)

        # Check Primary Key Constraint
        pk_val = str(row_dict[self.pk_col])
        if pk_val in self.index:
            raise ValueError(f"Duplicate entry for Primary Key '{pk_val}'")

        # Insert and Save
        self.rows.append(row_dict)
        self.index[pk_val] = len(self.rows) - 1
        self.save()
        return f"Inserted 1 row into {self.name}."

    def select(self, where_col=None, where_val=None):
        """
        Retrieves rows. 
        OPTIMIZATION: Uses Index if searching by Primary Key.
        """
        # Case 1: Select All
        if not where_col:
            return self.rows

        # Case 2: Index Lookup (O(1))
        if where_col == self.pk_col:
            idx = self.index.get(str(where_val))
            if idx is not None:
                return [self.rows[idx]]
            return []

        # Case 3: Full Table Scan (O(n))
        results = []
        for row in self.rows:
            # simple string comparison for now
            if str(row.get(where_col)) == str(where_val):
                results.append(row)
        return results

    def delete(self, pk_val):
        """Deletes a row by Primary Key."""
        pk_str = str(pk_val)
        if pk_str not in self.index:
            raise ValueError(f"Key {pk_str} not found.")

        # Filter out the row (Soft delete is harder, so we rewrite the list)
        self.rows = [row for row in self.rows if str(row[self.pk_col]) != pk_str]
        
        # Rebuild index and save
        self.rebuild_index()
        self.save()
        return "Row deleted."

class Database:
    """
    The main interface that manages multiple tables.
    """
    def __init__(self):
        self.tables = {}
        # Auto-load existing tables from data/ folder could go here
        # For this MVP, we will rely on 'create_table' or explicit loading

    def create_table(self, name, schema, pk_col):
        if name in self.tables:
            raise ValueError(f"Table {name} already loaded.")
        
        self.tables[name] = Table(name, schema, pk_col)
        return f"Table '{name}' created/loaded."

    def get_table(self, name):
        if name not in self.tables:
            # Try to lazy-load if file exists
            if os.path.exists(f"data/{name}.json"):
                 # HACK: To load existing, we need to read the file to find the schema/PK
                 # This is a bit circular, but works for the challenge
                 with open(f"data/{name}.json", 'r') as f:
                     meta = json.load(f)
                     return self.create_table(name, meta['schema'], meta['pk_col'])
            
            raise ValueError(f"Table '{name}' does not exist.")
        return self.tables[name]