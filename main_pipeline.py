import json
import os
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# load env variables
load_dotenv()
DB_URL = os.getenv("DB_URL")

# directories and configuration
INPUT_DIR = 'data/input'
CLEAN_DIR = 'data/processed'
QUARANTINE_DIR = 'data/quarantine'
RULES_FILE = 'validation_rules.json'

# ensure necessary directories exist
for folder in [CLEAN_DIR, QUARANTINE_DIR]:
    os.makedirs(folder, exist_ok=True)

# function to log errors to PostgreSQL
def log_error_to_postgres(file_name, error_type, details):
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO error_logs (file_name, error_type, error_details, severity)
            VALUES (%s, %s, %s, %s)
            ''', (file_name, error_type, details, 'HIGH'))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error writing to database: {e}")

# function to validate a single record against the rules
def validate_record(record, rules):
    errors = []
    
    # dynamic validation based on rules configuration
    for field, field_rules in rules.items():
        value = record.get(field)
        
        # search for required fields and check if they are empty
        is_empty = (value is None or value == "")
        if field_rules.get("required") and is_empty:
            # error if required field is missing or empty
            if not field_rules.get("allow_empty", True) or value is None:
                errors.append(f"Missing expected value for field '{field}'")
                continue 
                
        # if the field is not required and empty values are allowed, skip further checks
        if not is_empty:
            
            # minimum value check for numeric fields
            if "min_value" in field_rules:
                if float(value) < field_rules["min_value"]:
                    errors.append(f"Value for field '{field}' ({value}) is too small (min: {field_rules['min_value']})")
                    
            # allowed values check for categorical fields
            if "allowed_values" in field_rules:
                if value not in field_rules["allowed_values"]:
                    errors.append(f"Invalid value for field '{field}': {value}")
                    
            # date format check for date fields
            if "date_format" in field_rules:
                try:
                    datetime.strptime(str(value), field_rules["date_format"])
                except ValueError:
                    errors.append(f"Invalid date format for field '{field}': {value}")
                    
    return errors

def insert_clean_data(records):
    if not records:
        return
    
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        
        
        # ON CONFLICT example to avoid duplicate entries based on order_id (assuming it's unique)
        insert_query = '''
            INSERT INTO cleaned_orders 
            (order_id, order_date, customer_email, country, category, amount, currency, payment_method)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING;
        '''
        
        # prepare data for insertion
        data_to_insert = [
            (
                r['order_id'], r['order_date'], r['customer_email'], 
                r['country'], r['category'], r['amount'], 
                r['currency'], r['payment_method']
            ) for r in records
        ]
        
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        print(f" Success: {len(records)} records inserted into 'cleaned_orders'.")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error while writing clean data to Postgres: {e}")

def run_pipeline():
    # load validation rules from JSON configuration
    with open(RULES_FILE, 'r', encoding='utf-8') as f:
        config = json.load(f)
        validation_rules = config.get("rules", {})

    files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.json')]
    if not files:
        print("Missing input files in the directory. Please add JSON files to 'data/input' and rerun the pipeline.")
        return

    for file_name in files:
        path = os.path.join(INPUT_DIR, file_name)
        
        with open(path, 'r', encoding='utf-8') as f:
            batch_data = json.load(f)
            
        valid_records, invalid_records = [], []
        print(f"Processing: {file_name} according to rules for dataset '{config.get('dataset_name')}'")

        for index, record in enumerate(batch_data):
            # validate each record and collect errors
            errors = validate_record(record, validation_rules)

            if errors:
                error_msg = " | ".join(errors)
                record_id = record.get('order_id', 'Missing ID')
                print(f" Rejected record ID {record_id}: {error_msg}")
                log_error_to_postgres(file_name, "Rule Engine Alert", f"ID: {record_id} | Errors: {error_msg}")
                invalid_records.append(record)
            else:
                valid_records.append(record)

        # insert valid records into PostgreSQL and optionally save to clean directory
        if valid_records:
            insert_clean_data(valid_records) # <--- DODAJ TO
            
            # optionally save clean records to a separate directory for auditing
            clean_path = os.path.join(CLEAN_DIR, f"clean_{file_name}")
            with open(clean_path, 'w', encoding='utf-8') as f:
                json.dump(valid_records, f, indent=4)
                
        if invalid_records:
            quarantine_path = os.path.join(QUARANTINE_DIR, f"bad_{file_name}")
            with open(quarantine_path, 'w', encoding='utf-8') as f:
                json.dump(invalid_records, f, indent=4)
                
        os.remove(path)
        print(f"Summary: {len(valid_records)} valid, {len(invalid_records)} invalid.\n")

if __name__ == "__main__":
    run_pipeline()