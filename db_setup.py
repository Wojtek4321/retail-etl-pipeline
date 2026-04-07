import os
import logging
#database setup connect
from psycopg2 import connect, sql
from dotenv import load_dotenv

# basic logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


load_dotenv()

# Load database URLs from file env 
ADMIN_DB_URL = os.getenv("ADMIN_DB_URL")
DB_URL = os.getenv("DB_URL")

# create database and tables if not exist
def create_database():
    if not ADMIN_DB_URL:
        logger.critical("Missing ADMIN_DB_URL ")
        return

    # get database name from DB_URL
    db_name = DB_URL.split('/')[-1].split('?')[0]
    
    # error handling for database connection and creation
    try:
        
        conn = connect(ADMIN_DB_URL)
        conn.autocommit = True   # trannscation off for database creation
        cursor = conn.cursor() 
        
        # check catalog if database exists, if not create it POSTGRESQL
        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (db_name,))
        exists = cursor.fetchone()

        if not exists:
            # create database with proper escaping to avoid SQL injection
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
            logger.info(f"Database '{db_name}' created successfully")
        else:
            logger.info(f"Database '{db_name}' already exists")
            
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")

def create_tables():
    schemas = {
        "error_logs": """
            CREATE TABLE IF NOT EXISTS error_logs (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file_name VARCHAR(255),
                error_type VARCHAR(100),
                error_details TEXT,
                severity VARCHAR(50)
            )
        """,
        "cleaned_orders": """
            CREATE TABLE IF NOT EXISTS cleaned_orders (
                order_id INTEGER PRIMARY KEY,
                order_date TIMESTAMP,
                customer_email VARCHAR(255),
                country VARCHAR(100),
                category VARCHAR(100),
                amount DECIMAL(10, 2),
                currency VARCHAR(10),
                payment_method VARCHAR(50),
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
    }

    # error handling for table creation and connection
    try:
        with connect(DB_URL) as conn:
            with conn.cursor() as cursor:
                for table_name, ddl in schemas.items():
                    cursor.execute(ddl)
                    logger.info(f"Verified schema for: {table_name}")
            conn.commit()
    except Exception as e:
        logger.error(f"Error while creating tables: {e}")

if __name__ == "__main__":
    create_database()
    create_tables()