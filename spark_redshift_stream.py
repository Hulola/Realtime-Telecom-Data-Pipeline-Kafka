from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StringType, IntegerType

# Package dependencies
kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1"  # Adjust the version as per your Spark version
postgres_package = "org.postgresql:postgresql:42.6.0"

# Initialize Spark Session with Kafka and Postgres packages
spark = SparkSession.builder \
    .appName("PySpark Kafka to Postgres Stream") \
    .config("spark.jars.packages", f"{kafka_package},{postgres_package}") \
    .getOrCreate()

# Kafka Configuration
kafka_bootstrap_servers = 'localhost:9092'  # Replace with your Kafka server address
kafka_topic = 'telecom-data'

# Schema of Incoming Data
schema = StructType() \
    .add("caller_name", StringType()) \
    .add("receiver_name", StringType()) \
    .add("caller_id", StringType()) \
    .add("receiver_id", StringType()) \
    .add("start_datetime", StringType()) \
    .add("end_datetime", StringType()) \
    .add("call_duration", IntegerType()) \
    .add("network_provider", StringType()) \
    .add("total_amount", StringType())

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option('startingOffsets', 'latest') \
    .load()

df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Data Quality Check (Example: Ensuring call_duration is positive)
df = df.filter(df.call_duration > 0)

# Redshift Configuration
# Postgres Configuration (Local Replacement for Redshift)
postgres_jdbc_url = "jdbc:postgresql://localhost:5432/telecom_db"
postgres_table = "telecom_data"
postgres_user = "admin"
postgres_password = "password"
postgres_driver = "org.postgresql.Driver"

# Writing Data to Postgres
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", postgres_jdbc_url) \
        .option("user", postgres_user) \
        .option("password", postgres_password) \
        .option("dbtable", postgres_table) \
        .option("driver", postgres_driver) \
        .mode("append") \
        .save()

print("Streaming started !")
print("********************************")
# Execute Streaming Query
query = df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .start()

query.awaitTermination()
