import psycopg2
import os
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

load_dotenv()


ADMIN_DB_URL = os.getenv("ADMIN_DB_URL")
DB_URL = os.getenv("DB_URL")

def create_database():
    
    if not ADMIN_DB_URL:
        print(" Brak ADMIN_DB_URL w .env")
        return

    try:
        conn = psycopg2.connect(ADMIN_DB_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        #Get the new database name from DB_URL
        new_db_name = DB_URL.split('/')[-1]

        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{new_db_name}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {new_db_name}")
            print(f"Baza danych '{new_db_name}' została utworzona")
        else:
            print(f"Baza '{new_db_name}' już istnieje")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Błąd podczas tworzenia bazy {e}")

def create_audit_table():
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS error_logs (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file_name VARCHAR(255),
                error_type VARCHAR(100),
                error_details TEXT,
                severity VARCHAR(50)
            )
        ''')
        
        conn.commit()
        print("Tabela 'error_logs' gotowa")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Błąd podczas tworzenia tabeli {e}")

if __name__ == "__main__":
    create_database()
    create_audit_table()