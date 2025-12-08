# Real-Time Telecom Data Pipeline 🚀

A comprehensive real-time streaming project that generates synthetic telecom data, streams it through **Apache Kafka**, processes it with **Apache Spark (Structured Streaming)**, and stores it in a **Postgres** data warehouse (simulating Amazon Redshift).

## 🏗 Architecture
1.  **Data Source**: Python Producer generating fake Call Data Records (CDRs).
2.  **Message Broker**: Apache Kafka (running in Docker).
3.  **Stream Processing**: Apache Spark (reading from Kafka, transformating, writing to DB).
4.  **Storage**: Postgres Database (Local Docker container).

## 🛠 Prerequisites
*   **Docker Desktop** (Required for Kafka & Postgres)
*   **Python 3.8+**
*   **Java 8 or 11** (Required for Spark)

## 📦 Installation

1.  **Clone the repository**
    ```bash
    git clone https://github.com/YOUR_USERNAME/kafka-spark-streaming.git
    cd kafka-spark-streaming
    ```

2.  **Install Python Dependencies**
    ```bash
    pip install kafka-python-ng pyspark faker psycopg2-binary
    ```

3.  **Start Infrastructure**
    Start Kafka, Zookeeper, and Postgres using Docker Compose:
    ```bash
    docker-compose up -d
    ```

4.  **Initialize Database**
    Create the `telecom_data` table in Postgres:
    ```bash
    # Command to run SQL file inside the postgres container
    docker exec -i postgres psql -U admin -d telecom_db < postgres_create_table.sql
    ```

## ▶️ How to Run

### 1. Start the Data Producer
This script generates random call events and sends them to the `telecom-data` Kafka topic.
```bash
python kafka_producer.py
```

### 2. Start the Spark Streaming Job
This script consumes data from Kafka, validates it, and writes it to Postgres.
```bash
python spark_redshift_stream.py
```

### 3. Verify Data
Check if data is landing in the database:
```bash
python postgres_connect.py
```

## 📂 Project Structure
*   `docker-compose.yml`: Infrastructure as Code (Kafka, Zookeeper, Postgres).
*   `kafka_producer.py`: Python script to generate and push mock data.
*   `spark_redshift_stream.py`: Main PySpark streaming logic.
*   `postgres_create_table.sql`: SQL DDL for the destination table.
*   `postgres_connect.py`: Utility to test database connection.
*   `redshift-jdbc42-2.1.0.12.jar`: JDBC Driver (Required for Spark SQL connectivity).

## 📝 Notes
*   This project uses a local Postgres instance to mimic a cloud data warehouse like Amazon Redshift for cost-free development and testing.
*   To switch to AWS Redshift, update the JDBC URL in `spark_redshift_stream.py` and use the Redshift driver.
