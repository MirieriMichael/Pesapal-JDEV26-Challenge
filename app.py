from flask import Flask, request, render_template, redirect, url_for
import sys
import os

# Add src to path so we can import the DB engine
sys.path.append(os.getcwd())
from src.db import Database

app = Flask(__name__)
db = Database()

# Ensure the table exists for the web app
try:
    # Schema: PK=id, columns=name, role
    db.create_table("employees", {"id": "str", "name": "str", "role": "str"}, "id")
except ValueError:
    pass # Table already exists

@app.route('/')
def index():
    # 1. Fetch data from our custom DB
    table = db.get_table("employees")
    rows = table.select()
    # 2. Render the HTML template (Flask looks in the 'templates' folder automatically)
    return render_template('index.html', rows=rows)

@app.route('/add', methods=['POST'])
def add_entry():
    # 1. Get form data
    id_val = request.form.get('id')
    name_val = request.form.get('name')
    role_val = request.form.get('role')
    
    # 2. Insert into DB
    table = db.get_table("employees")
    try:
        table.insert({"id": id_val, "name": name_val, "role": role_val})
    except Exception as e:
        return f"Error: {e} <a href='/'>Go Back</a>"
    
    return redirect(url_for('index'))

@app.route('/delete/<id_val>', methods=['POST'])
def delete_entry(id_val):
    table = db.get_table("employees")
    try:
        table.delete(id_val)
    except Exception as e:
        return f"Error: {e} <a href='/'>Go Back</a>"
    return redirect(url_for('index'))

if __name__ == '__main__':
    print("Starting Web App on http://127.0.0.1:5000")
    app.run(debug=False, port=5000)