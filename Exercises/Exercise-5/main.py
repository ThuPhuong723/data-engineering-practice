import psycopg2
import csv
from pathlib import Path

def execute_sql_script(cursor, filepath):
    with open(filepath, 'r') as f:
        cursor.execute(f.read())

def import_csv_to_table(cursor, csv_file, table_name):
    with open(csv_file, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for row in reader:
            placeholders = ','.join(['%s'] * len(row))
            cursor.execute(
                f"INSERT INTO {table_name} VALUES ({placeholders})", row
            )

def main():
    conn = psycopg2.connect(
        host="localhost", database="postgres", user="postgres", password="postgres"
    )
    cur = conn.cursor()

    # Step 1: Run schema.sql
    execute_sql_script(cur, "schema.sql")
    conn.commit()
    print("✅ Tables created.")

    # Step 2: Load CSV data
    data_dir = Path("data")
    csv_table_map = {
        "customers.csv": "customers",
        "products.csv": "products",
        "orders.csv": "orders"
    }

    for csv_file, table in csv_table_map.items():
        import_csv_to_table(cur, data_dir / csv_file, table)
        print(f"✅ Imported {csv_file} into {table}")

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
