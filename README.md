# retail-etl-pipeline

ETL implementation for processing retail transaction data. The project uses Python (Pandas) for data transformation and PostgreSQL for storage. Everything is wrapped in Docker for consistent environment setup.

## Logic overview
1. Extraction: Reads raw data from JSON files in `data/input`.
2. Transformation: 
    - Validates data against rules in `validation_rules.json`.
    - Handles missing IDs and negative amounts.
    - Moves invalid records to a quarantine folder.
3. Loading: Upserts cleaned data into a PostgreSQL table using SQLAlchemy.
4. Logging: Tracks execution and DB errors in an `error_logs` table.

## Prerequisites
- Docker 
- Environment variables defined in a `.env` file (see `.env.example` for reference)

## How to run
Build and start the services:
```bash
docker-compose up -d --build
```
Initialize the database
```bash
docker-compose run --rm etl_app python db_setup.py
```
Process data:
```bash
Generate test files: docker-compose run --rm etl_app python generate_data.py
Run the pipeline: docker-compose run --rm etl_app python main_pipeline.py