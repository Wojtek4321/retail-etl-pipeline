import os
import logging
from psycopg2 import connect, sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

# Configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

#from .env
ADMIN_DB_URL = os.getenv("ADMIN_DB_URL")
DB_URL = os.getenv("DB_URL")



def create_database():
    if not ADMIN_DB_URL:
        logger.error(" ADMIN_DB_URL non existent")
        return

    # to get database name from DB_URL
    db_name = DB_URL.split('/')[-1].split('?')[0]
    
    try:
        
        with connect(ADMIN_DB_URL) as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cursor:
                # check if database exists
                cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (db_name,))
                exists = cursor.fetchone()

                if not exists:
                    # create database with sql.SQL to avoid SQL injection
                    cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
                    logger.info(f"Data base '{db_name}' was created")
                else:
                    logger.info(f"Data base '{db_name}' already exists")
    except Exception as e:
        logger.error(f"Critical error during database initialization: {e}")

def create_tables():
    # definitinion of tables and their structure
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

    try:
        with connect(DB_URL) as conn:
            with conn.cursor() as cursor:
                for table_name, ddl in schemas.items():
                    cursor.execute(ddl)
                    logger.info(f"Table structure for '{table_name}' verified.")
            conn.commit()
    except Exception as e:
        logger.error(f"Error occurred while creating schemas: {e}")

if __name__ == "__main__":
    create_database()
    create_tables()