# API to PostgreSQL ETL with Airflow

This project retrieves sample customer data from the Mockaroo API and loads it into a PostgreSQL database. Apache Airflow orchestrates the extraction and loading tasks, while Docker Compose runs the project services and environment.

## How It Works

1. **Extract:** Airflow requests sample data from the Mockaroo API.
2. **Load:** The data is converted into records and inserted into the `customers` table in PostgreSQL.
3. **Orchestrate:** Airflow runs the extraction task first, followed by the loading task, according to the schedule defined in the DAG.

## Technologies

- Python
- Apache Airflow
- Mockaroo API
- PostgreSQL
- Docker Compose

## Project Structure

.
├── dags/
│   └── API_to_postgres_pipeline.py
├── logs/
├── plugins/
│
├── .env                  # Local secrets; not committed to Git
├── .gitignore
├── README.md
└── docker-compose.yaml

## Configuration

Create a .env file in the same directory as docker-compose.yaml and add the required values:

 - MOCKAROO_API_KEY=your_mockaroo_api_key
 - POSTGRES_DB=your_database
 - POSTGRES_USER=your_user
 - POSTGRES_PASSWORD=your_password

## Run the Project
From the project directory, start the services with Docker Compose:
 - docker compose up -d

Open the Airflow web interface at:
http://localhost:8080

To stop the services:
 - docker compose down

## Notes
- Docker Compose starts and manages the project services; this project does not use a separate Dockerfile.
- The DAG schedule is configured in the Airflow DAG file.