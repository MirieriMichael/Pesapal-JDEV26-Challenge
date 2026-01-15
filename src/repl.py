import shlex  # Helps parse quoted strings like "John Doe"
import sys
import os

# Add the current directory to path so we can import db
sys.path.append(os.getcwd())

from src.db import Database

def main():
    db = Database()
    print("="*60)
    print("   WELCOME TO MIRIERI DB (MDB)   ")
    print("   Type 'help' for commands or 'exit' to quit.")
    print("="*60)

    while True:
        try:
            # 1. Get User Input
            query = input("MDB> ").strip()
            if not query:
                continue
                
            if query.lower() in ["exit", "quit"]:
                print("Goodbye!")
                break
                
            if query.lower() == "help":
                print_help()
                continue

            # 2. Parse the Command
            # shlex.split handles quotes properly: 'INSERT "Startups Law"' -> ["INSERT", "Startups Law"]
            tokens = shlex.split(query)
            command = tokens[0].upper()

            # 3. Execute Logic
            if command == "CREATE":
                # Syntax: CREATE TABLE <name> <pk_col> <col1> <col2> ...
                if len(tokens) < 4:
                    print("Error: Usage -> CREATE TABLE <name> <pk_col> <col1> <col2>...")
                    continue
                    
                table_name = tokens[2]
                pk_col = tokens[3]
                columns = tokens[4:]
                
                # Add PK to schema + others (defaulting all to "str" for this challenge)
                schema = {pk_col: "str"}
                for col in columns:
                    schema[col] = "str"
                
                print(db.create_table(table_name, schema, pk_col))

            elif command == "INSERT":
                # Syntax: INSERT INTO <name> <val1> <val2> ...
                if len(tokens) < 4:
                    print("Error: Usage -> INSERT INTO <name> <val1> <val2>...")
                    continue
                
                table_name = tokens[2]
                values = tokens[3:]
                table = db.get_table(table_name)
                
                # Check if values match schema count
                if len(values) != len(table.schema):
                    print(f"Error: Table has {len(table.schema)} columns but you provided {len(values)} values.")
                    continue

                # Zip schema keys with values to create the dictionary
                row_data = dict(zip(table.schema.keys(), values))
                print(table.insert(row_data))

            elif command == "SELECT":
                # Syntax 1: SELECT * FROM <table>
                # Syntax 2: SELECT * FROM <table> WHERE <col> = <val>
                
                if "FROM" not in [t.upper() for t in tokens]:
                    print("Error: Syntax -> SELECT * FROM <name> [WHERE <col> = <val>]")
                    continue
                
                from_index = [t.upper() for t in tokens].index("FROM")
                table_name = tokens[from_index + 1]
                table = db.get_table(table_name)

                # Check for WHERE clause
                results = []
                if "WHERE" in [t.upper() for t in tokens]:
                    where_index = [t.upper() for t in tokens].index("WHERE")
                    col = tokens[where_index + 1]
                    # skip the "=" sign if user typed it
                    val_index = where_index + 2
                    if tokens[val_index] == "=":
                        val_index += 1
                    val = tokens[val_index]
                    
                    results = table.select(where_col=col, where_val=val)
                else:
                    results = table.select()

                # Pretty Print Results
                print(f"Found {len(results)} rows:")
                for row in results:
                    print(row)

            elif command == "DELETE":
                # Syntax: DELETE FROM <table> WHERE <pk> = <val>
                # Simplified: DELETE FROM users <pk_val>
                if len(tokens) < 4:
                    print("Error: Usage -> DELETE FROM <table> <pk_value>")
                    continue
                
                table_name = tokens[2]
                pk_val = tokens[3]
                table = db.get_table(table_name)
                print(table.delete(pk_val))

            else:
                print(f"Unknown command: {command}")

        except Exception as e:
            print(f"System Error: {e}")

def print_help():
    print("""
    COMMANDS:
    ---------
    CREATE TABLE <name> <pk_col> <col1> <col2>
       Ex: CREATE TABLE students id name course
       
    INSERT INTO <name> <val1> <val2> ...
       Ex: INSERT INTO students 151731 "Michael" "CompSci"
       
    SELECT * FROM <name> [WHERE <col> = <val>]
       Ex: SELECT * FROM students WHERE id = 151731
       
    DELETE FROM <name> <pk_val>
       Ex: DELETE FROM students 151731
    """)

if __name__ == "__main__":
    main()