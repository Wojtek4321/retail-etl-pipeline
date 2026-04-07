import os
import json
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# config logging
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# sqlalchemy engine setup
DB_URL = os.getenv("DB_URL").replace("postgres://", "postgresql+psycopg2://").replace("postgresql://", "postgresql+psycopg2://")
engine = create_engine(DB_URL)

INPUT_DIR = 'data/input'
CLEAN_DIR = 'data/processed'
QUARANTINE_DIR = 'data/quarantine'
RULES_FILE = 'validation_rules.json'


for folder in [CLEAN_DIR, QUARANTINE_DIR]:
    os.makedirs(folder, exist_ok=True)

# validation function based on rules from json file
def validate_dataframe(df: pd.DataFrame, rules: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    
    df['error_details'] = ""

    for field, field_rules in rules.items():
        if field not in df.columns:
            continue

        # value not empty
        if field_rules.get("required"):
            mask_empty = df[field].isna() | (df[field].astype(str).str.strip() == "")
            df.loc[mask_empty, 'error_details'] += f"Brak wartości w '{field}' | "

        # value minimum for numeric fields
        if "min_value" in field_rules:
            numeric_col = pd.to_numeric(df[field], errors='coerce') # convert column to numeric invalid values become NaN
            mask_min = numeric_col < field_rules["min_value"]
            df.loc[mask_min & numeric_col.notna(), 'error_details'] += f"too small value '{field}' | "

    # separate valid and invalid records based on error_details column
    mask_has_errors = df['error_details'] != ""
    invalid_df = df[mask_has_errors].copy()
    valid_df = df[~mask_has_errors].drop(columns=['error_details']).copy()
    
    return valid_df, invalid_df


def log_errors_bulk(invalid_df: pd.DataFrame, file_name: str):
    if invalid_df.empty:
        return

    # prepare DataFrame for bulk insert into error_logs table
    error_logs_df = pd.DataFrame({
        'file_name': file_name,
        'error_type': 'Rule Engine Alert',
        'error_details': "ID: " + invalid_df['order_id'].fillna('Brak').astype(str) + " | " + invalid_df['error_details'],
        'severity': 'HIGH'
    })
    
    # Bulk insert za pomocą pandas
    error_logs_df.to_sql('error_logs', engine, if_exists='append', index=False)

def run_pipeline():
    with open(RULES_FILE, 'r', encoding='utf-8') as f:
        validation_rules = json.load(f).get("rules", {})

    files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.json')]
    if not files:
        logger.info("No new files found in data/input folder")
        return

    for file_name in files:
        path = os.path.join(INPUT_DIR, file_name)
        
        # load data and log file processing start
        df = pd.read_json(path)
        logger.info(f"Processing file: {file_name} ({len(df)} records)")

        # use validation function to separate valid and invalid records
        valid_df, invalid_df = validate_dataframe(df, validation_rules)

        # save valid records to database and log the result
        if not valid_df.empty:
            valid_df = valid_df.drop_duplicates(subset=['order_id']) # remove duplicates based on order_id to avoid primary key conflicts
            try:
                valid_df.to_sql('cleaned_orders', engine, if_exists='append', index=False)
                logger.info(f"Success: Saved {len(valid_df)} valid records.")
            except Exception as e:
                logger.error(f"Error saving to database: {e}")

        # save invalid records to quarantine folder and log the result
        if not invalid_df.empty:
            logger.warning(f"Rejected: {len(invalid_df)} records. Saving to quarantine.")
            invalid_df.drop(columns=['error_details']).to_json(os.path.join(QUARANTINE_DIR, f"bad_{file_name}"), orient='records', indent=4)
            log_errors_bulk(invalid_df, file_name)

        # remove processed file and log the completion
        os.remove(path)
        logger.info(f"Completed processing file: {file_name}")

if __name__ == "__main__":
    run_pipeline()