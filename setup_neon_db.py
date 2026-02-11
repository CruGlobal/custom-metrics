import os
from database import init_db
from dotenv import load_dotenv

def main():
    """
    Connects to the Neon.tech Postgres database and initializes the tables.
    """
    load_dotenv()

    print("Initializing Neon.tech database...")
    try:
        init_db()
        print("Database initialized successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
