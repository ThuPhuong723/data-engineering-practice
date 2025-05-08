import psycopg2
import csv
from pathlib import Path

DATA_DIR = Path("./data")

def setup_database(cursor):
    try:
        with open("schema.sql", "r", encoding='utf-8') as file:
            schema_sql = file.read()
            cursor.execute(schema_sql)
            print("✅ Bảng đã được tạo thành công.")
    except Exception as e:
        print(f"❌ Lỗi khi tạo bảng: {e}")

def load_csv_to_table(cursor, table_name, file_path):
    try:
        with open(file_path, newline='', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader)
            records = [tuple(row) for row in reader]

            cols = ', '.join(headers)
            vals = ', '.join(['%s'] * len(headers))
            query = f"INSERT INTO {table_name} ({cols}) VALUES ({vals})"

            for record in records:
                cursor.execute(query, record)
            print(f"✅ Đã chèn dữ liệu vào bảng '{table_name}' từ '{file_path.name}'")
    except Exception as e:
        print(f"❌ Lỗi khi chèn dữ liệu vào bảng '{table_name}': {e}")

def main():
    config = {
        "host": "postgres",
        "database": "postgres",
        "user": "postgres",
        "password": "postgres"
    }

    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cursor:
                setup_database(cursor)

                load_csv_to_table(cursor, "accounts", DATA_DIR / "accounts.csv")
                load_csv_to_table(cursor, "products", DATA_DIR / "products.csv")
                load_csv_to_table(cursor, "transactions", DATA_DIR / "transactions.csv")

                conn.commit()
                print("🎉 Tất cả dữ liệu đã được xử lý thành công.")
    except Exception as err:
        print(f"❌ Kết nối thất bại hoặc lỗi trong quá trình xử lý: {err}")

if __name__ == "__main__":
    main()
