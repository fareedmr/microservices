# ETL Pipeline with Python, MongoDB and Docker

## Requirements

- Python 3.8
- Docker
- `pymongo` library

## Setup

1. Clone the repository and navigate to the project directory:

    > git clone git@github.com:fareedmr/microservices.git

    > cd MongoDB_ETL_Pipeline

2. Place the `member-data.txt` file in the `data/` directory.

3. Build and run the services using Docker Compose:

    > docker-compose up --build

4. This will start `db_service` and the `etl_service`.

The ETL pipeline will automatically read the file, transform the data, and load it into MongoDB.

## Testing

1. Run the test suite using Docker Compose:

    > docker-compose run etl_service python test_etl_pipeline.py
   
2. This will run the unit tests for the ETL pipeline.

## Cleanup

To stop the services, run:

> docker-compose down

To remove volumes and clean up, use:

> docker-compose down --volumes