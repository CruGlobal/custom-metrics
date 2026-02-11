import os
import database

def migrate():
    """Add ip_address column to ping_metrics and speed_metrics tables."""
    print("Starting database migration...")
    try:
        conn = database.get_db_connection()
        cursor = conn.cursor()

        # Add ip_address to ping_metrics
        try:
            cursor.execute("ALTER TABLE ping_metrics ADD COLUMN ip_address TEXT")
            print("Added ip_address column to ping_metrics table.")
        except Exception as e:
            if "duplicate column name" in str(e):
                print("ip_address column already exists in ping_metrics table.")
            else:
                raise e

        # Add ip_address to speed_metrics
        try:
            cursor.execute("ALTER TABLE speed_metrics ADD COLUMN ip_address TEXT")
            print("Added ip_address column to speed_metrics table.")
        except Exception as e:
            if "duplicate column name" in str(e):
                print("ip_address column already exists in speed_metrics table.")
            else:
                raise e

        conn.commit()
        conn.close()
        print("Database migration completed successfully.")
    except Exception as e:
        print(f"An error occurred during migration: {e}")

if __name__ == "__main__":
    migrate()
