# Data_Collection_Final
# Real-Time Cryptocurrency Data Pipeline

## Project Overview
This project was developed as part of the final group assignment for the Data Collection and Processing course. The main goal of the project is to design and implement a complete data pipeline that works with frequently updating real-world data.

The system collects live cryptocurrency market data from the CoinGecko API, streams it through Kafka, processes and cleans it using scheduled Airflow jobs, and stores both cleaned data and aggregated analytics in an SQLite database. The project demonstrates how streaming and batch processing can be combined in a single end-to-end data pipeline.

## Data Source
The project uses the CoinGecko API as the main data source. CoinGecko provides real-time cryptocurrency market information such as prices, market capitalization, and trading volume.

This API was chosen because it updates frequently, returns structured JSON data, is stable and well documented, and provides real market values rather than generated or static data. These characteristics make it suitable for a streaming-based data collection pipeline.

## System Architecture
The pipeline is implemented using three separate Airflow DAGs, each handling a different stage of data processing.

### DAG 1 – Continuous Data Ingestion
This job runs continuously and fetches cryptocurrency data from the CoinGecko API at regular intervals. Each API response is sent as a raw JSON message to a Kafka topic.

Flow: CoinGecko API → Kafka (raw events topic)

### DAG 2 – Hourly Cleaning and Storage
This job runs every hour. It reads all new messages from the Kafka topic, cleans and normalizes the data using Pandas, handles missing or invalid values, and converts fields to appropriate data types. The cleaned data is then stored in an SQLite database.

Flow: Kafka → Data Cleaning → SQLite (events table)

### DAG 3 – Daily Analytics
This job runs once per day. It reads the cleaned data from SQLite and computes aggregated metrics such as average, minimum, and maximum prices, as well as total trading volume. The results are stored in a separate summary table.

Flow: SQLite (events) → Analytics → SQLite (daily_summary table)

## Database Schema
The SQLite database contains two tables:
- events: cleaned cryptocurrency market data (coin ID, symbol, price, market cap, volume, timestamp)
- daily_summary: aggregated daily statistics (date, coin ID, average price, minimum price, maximum price, total volume)

## Project Structure
Data_Collection_Final/
├── README.md
├── requirements.txt
├── src/
│   ├── job1_producer.py
│   ├── job2_cleaner.py
│   ├── job3_analytics.py
│   └── db_utils.py
├── airflow/
│   └── dags/
│       ├── job1_ingestion_dag.py
│       ├── job2_clean_store_dag.py
│       └── job3_daily_summary_dag.py
└── data/
    └── crypto.db

## Technologies Used
- Python
- Apache Airflow
- Apache Kafka
- SQLite
- Pandas
- Docker and Docker Compose
- CoinGecko API

## How to Run the Project
1. Make sure Docker and Docker Compose are installed
2. Clone the repository
3. Run docker-compose up
4. Open Airflow at http://localhost:8080 and enable all DAGs

## Notes
This project was created for educational purposes and demonstrates core data engineering concepts. SQLite was used for simplicity, and API rate limits should be considered when running the ingestion job continuously.

## Team Contribution
All team members contributed equally to the design, implementation, testing, and documentation of this project.

## Conclusion
This project demonstrates how real-time data ingestion, batch processing, and analytics can be integrated into a single pipeline using modern data engineering tools.
