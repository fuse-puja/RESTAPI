import sqlite3 
from models import Employee 

# Connect to the SQLite database named "data.db"
conn = sqlite3.connect("data.db") 
cursor = conn.cursor() 

# Function to create the "employees" table if it doesn't exist
def create_table():
    conn = sqlite3.connect("data.db") 
    cursor = conn.cursor() 
    try:
        # SQL query to create the table with columns: id, name, and department
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                id INTEGER PRIMARY KEY,
                name TEXT,
                department TEXT
            )
        """)
        conn.commit()  # Commit changes to the database
    finally:
        conn.close()

# Function to retrieve a list of employees with pagination
def get_employees(skip: int, limit: int):
    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()
    try:
         # SQL query to select rows from the "employees" table with specified limit and offset
        cursor.execute("SELECT * FROM employees LIMIT ? OFFSET ?", (limit, skip))
        # Fetch all rows returned by the query
        rows = cursor.fetchall()
        # Create Employee objects from the rows and return a list
        employees = [Employee(id=row[0], name=row[1], department=row[2]) for row in rows]
        return employees
    finally:
        conn.close()

# Function to retrieve an employee by their ID
def get_employee(employee_id: int):
    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()
    try:
        # SQL query to select a row from the "employees" table based on ID
        cursor.execute("SELECT * FROM employees WHERE id = ?", (employee_id,))
        row = cursor.fetchone()
        if row:
             # Fetch one row from the query result
            return Employee(id=row[0], name=row[1], department=row[2])
        else:
            return None
    finally:
        conn.close()


# Function to insert a new employee into the "employees" table
def insert_employee(employee: Employee):
    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()
    try:
        # SQL query to insert values into the "employees" table
        cursor.execute("INSERT INTO employees (name, department) VALUES (?, ?)", (employee.name, employee.department))
        conn.commit()
        return employee
    finally:
        conn.close()

# Function to delete an employee by their ID
def delete_employee(employee_id: int):
    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()
    try:
        # SQL query to delete a row from the "employees" table based on ID
        cursor.execute("DELETE FROM employees WHERE id = ?", (employee_id,))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()

# Function to update an employee's information
def update_employee(employee_id: int, column: str, new_value: str):
    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()
    try:
        # SQL query to update a specific column for an employee based on ID
        cursor.execute(f"UPDATE employees SET {column} = ? WHERE id = ?", (new_value, employee_id))
        conn.commit()
        return cursor.rowcount > 0 # Return True if rows were affected
    finally:
        conn.close()