!pip install pandas cx_Oracle
!pip install oracledb pandas

import pandas as pd
import oracledb

# --- Step 1: Create a sample DataFrame ---
data = {
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35]
}
df = pd.DataFrame(data)

# --- Step 2: Connect to Oracle Database ---
# Replace with your own Oracle DB details
conn = oracledb.connect(
    user="bits",
    password="bits",
    host="localhost",
    port=1521,
    service_name="PDB_SANTHU"
)
cursor = conn.cursor()

# --- Step 3: Create Table (if not exists) ---
create_table_sql = """
CREATE TABLE employees (
    id NUMBER PRIMARY KEY,
    name VARCHAR2(50),
    age NUMBER
)
"""
try:
    cursor.execute(create_table_sql)
except oracledb.DatabaseError:
    print("⚠️ Table already exists, skipping creation.")

# --- Step 4: Insert DataFrame into Oracle ---
insert_sql = "INSERT INTO employees (id, name, age) VALUES (:1, :2, :3)"
rows = [tuple(x) for x in df.to_numpy()]  # convert DataFrame rows to tuples
cursor.executemany(insert_sql, rows)

# --- Step 5: Commit & Close ---
conn.commit()
cursor.close()
conn.close()

print("✅ DataFrame loaded into Oracle table successfully!")
print("✅ End!")

