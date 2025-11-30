import os
import psycopg
from dotenv import load_dotenv

def test_db_connection():
    """
    Tests the connection to the Neon.tech Postgres database.
    """
    load_dotenv()

    print("Attempting to connect to the database...")
    try:
        conn_string = (
            f"host={os.environ.get('PGHOST')} "
            f"user={os.environ.get('PGUSER')} "
            f"password={os.environ.get('PGPASSWORD')} "
            f"dbname={os.environ.get('PGDATABASE')} "
            f"sslmode={os.environ.get('PGSSLMODE')} "
            f"channel_binding={os.environ.get('PGCHANNELBINDING')}"
        )
        with psycopg.connect(conn_string) as conn:
            print("Database connection successful!")
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                result = cur.fetchone()
                print(f"Test query result: {result}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    test_db_connection()
