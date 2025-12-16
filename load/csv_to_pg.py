import csv
import time

import psycopg2
import psycopg2.extras
import os

CSV_FILE_PATH = os.getenv("CSV_FILE_PATH_PG", "./2019-Oct.csv")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "ecommerce"),
    "user": os.getenv("DB_USER", "ecommerce_user"),
    "password": os.getenv("DB_PASSWORD", "ecommerce_password"),
    "port": os.getenv("DB_PORT", "5432"),
}


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def create_table(conn):
    create_query = """
                   CREATE TABLE IF NOT EXISTS ecommerce_events
                   (
                       id
                       SERIAL
                       PRIMARY
                       KEY,
                       event_time
                       TIMESTAMP
                       NOT
                       NULL,
                       event_type
                       VARCHAR
                   (
                       50
                   ) NOT NULL,
                       product_id BIGINT,
                       category_id BIGINT,
                       category_code VARCHAR
                   (
                       255
                   ),
                       brand VARCHAR
                   (
                       255
                   ),
                       price DECIMAL
                   (
                       10,
                       2
                   ),
                       user_id BIGINT,
                       user_session VARCHAR
                   (
                       255
                   )
                       ); \
                   """
    with conn.cursor() as cur:
        cur.execute(create_query)
    conn.commit()
    print("Table schema checked/created.")


def process_file():
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Error: File not found at {CSV_FILE_PATH}")
        return

    conn = None
    start_time = time.perf_counter()

    try:
        print(f"Starting import from {CSV_FILE_PATH}")
        conn = get_db_connection()
        create_table(conn)
        with conn.cursor() as cur:
            cur.execute("SET synchronous_commit = OFF;")
            with open(CSV_FILE_PATH, "r", encoding="utf-8") as f:
                next(f)
                cur.copy_expert(
                    """
                    COPY ecommerce_events (
                        event_time,
                        event_type,
                        product_id,
                        category_id,
                        category_code,
                        brand,
                        price,
                        user_id,
                        user_session
                    )
                    FROM STDIN WITH CSV
                    """,
                    f
                )
        conn.commit()
        elapsed = time.perf_counter() - start_time
        print(f"Import completed successfully in {elapsed:.2f} seconds")
    except psycopg2.Error as db_err:
        print("Database error occurred:")
        print(db_err)
        if conn:
            conn.rollback()
    except Exception as e:
        print("Unexpected error occurred:")
        print(e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    process_file()
