import csv
import psycopg2
import psycopg2.extras
import os

CSV_FILE_PATH = "data/2019-Oct.csv"
BATCH_SIZE = 10000

DB_CONFIG = {
    "host": "localhost",
    "database": "ecommerce",
    "user": "ecommerce_user",
    "password": "ecommerce_password",
    "port": "5432"
}


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def create_table(conn):
    create_query = """
    CREATE TABLE IF NOT EXISTS ecommerce_behavior (
        id SERIAL PRIMARY KEY,
        event_time TIMESTAMP NOT NULL,
        event_type VARCHAR(50) NOT NULL,
        product_id BIGINT,
        category_id BIGINT,
        category_code VARCHAR(255),
        brand VARCHAR(255),
        price DECIMAL(10, 2),
        user_id BIGINT,
        user_session VARCHAR(255)
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_query)
    conn.commit()
    print("✅ Table schema checked/created.")


def process_csv_file():
    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ Error: File not found at {CSV_FILE_PATH}")
        return

    conn = get_db_connection()
    create_table(conn)

    insert_query = """
    INSERT INTO ecommerce_behavior 
    (event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session)
    VALUES %s
    """

    print(f"🚀 Starting import from {CSV_FILE_PATH}...")

    with open(CSV_FILE_PATH, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)

        batch_buffer = []
        total_rows = 0

        cur = conn.cursor()

        try:
            for row in reader:
                cleaned_row = [val if val != '' else None for val in row]

                batch_buffer.append(cleaned_row)

                if len(batch_buffer) >= BATCH_SIZE:
                    psycopg2.extras.execute_values(cur, insert_query, batch_buffer)
                    conn.commit()
                    total_rows += len(batch_buffer)
                    print(f"Inserted {total_rows} rows...", end='\r')
                    batch_buffer = []

            if batch_buffer:
                psycopg2.extras.execute_values(cur, insert_query, batch_buffer)
                conn.commit()
                total_rows += len(batch_buffer)

            print(f"\n✅ Import Complete! Total rows inserted: {total_rows}")

        except Exception as e:
            print(f"\n❌ Error during import: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()


if __name__ == "__main__":
    process_csv_file()