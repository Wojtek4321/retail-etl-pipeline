FROM python:3.10-slim

# install system dependencies for psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# work directory inside the container
WORKDIR /app

# install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy the rest of the application
COPY . .

# ensure data directories exist
RUN mkdir -p data/input data/processed data/quarantine

CMD ["python", "main_pipeline.py"]